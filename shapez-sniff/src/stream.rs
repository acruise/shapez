//! Streaming front end: sniff a bitstream of unbounded length.
//!
//! [`crate::sniff`] takes a slice, which means the caller already holds
//! every byte. That is the easy case and not the general one — the
//! inputs this crate exists for arrive as files too large to map, as
//! socket reads, as Kafka partitions, as the decompressed side of a
//! codec. [`SniffStream`] takes them a chunk at a time in bounded
//! memory, and the chunk boundaries are invisible to the answer.
//!
//! ## Bounded memory
//!
//! Every accumulator in the pipeline is fixed-size or capped: the
//! unigram table is 256 counters, the bigram and trigram tables are
//! Space-Saving sketches with a cap, the cadence histograms are capped,
//! line geometry is running sums, and the only per-line buffer is
//! bounded by `LINE_BUF_CAP`. The head buffer used for the encoding
//! decision is bounded by [`HEAD_TARGET`]. Memory is O(1) in the length
//! of the stream — a single-document JSON stream whose one "line" is
//! forty gigabytes costs the same as a short one.
//!
//! ## Sampling without seeking
//!
//! The slice sampler in [`crate::ngram::segments`] spreads its window
//! budget evenly across a *known* length, which needs random access and
//! a length. A stream has neither. Instead the admission policy profiles
//! a contiguous prefix, then admits a window, skips, admits a window,
//! skips — **doubling the skip each time**, for at most
//! `budget.windows` windows. Coverage is dense early and sparse late,
//! the reach grows exponentially in the number of windows, and no byte
//! offset has to be known in advance.
//!
//! The window count has to be bounded, and the reason is worth stating:
//! with unbounded doubling the budget is never spent. Filling a 1 MiB
//! profile 32 KiB at a time with a skip that doubles takes on the order
//! of *thirty terabytes* of stream to get through, so `is_satisfied`
//! would never fire and a `Read`-driven sniff would never stop early.
//! With the default eight windows the sampler reaches roughly 8 MiB
//! into the stream and profiles about 320 KiB of it. Raising
//! `budget.windows` extends the reach exponentially, at proportional
//! read cost.
//!
//! What this cannot do is sample the end of a stream it refuses to
//! read. Sequential access with a bounded read budget only ever sees a
//! prefix, however cleverly subsampled. A caller who is streaming the
//! whole input anyway — where the cost is CPU, not I/O — should raise
//! `windows` and `max_bytes` to spread coverage as deep as it likes.
//!
//! The consequence worth stating plainly: for an input larger than
//! `budget.prefix`, the slice path and the stream path sample
//! *different bytes* and can therefore report slightly different
//! confidences. Below `budget.prefix` they are identical by
//! construction. This is not a defect to be papered over — random
//! access is genuinely more information than sequential access, and the
//! slice path should use it.
//!
//! One distortion the sampler introduces on either path: each seam
//! manufactures a line boundary, because the partial line at the end of
//! a window has to be flushed before the next window starts. That adds
//! at most one spurious line per window to the line-anchored
//! statistics — negligible against a window's worth of real lines, but
//! it does mean a sampled stream never reports exactly one line, even
//! when the input genuinely is one enormous line.

use std::borrow::Cow;
use std::io::{self, Read};

use crate::alphabet::{decide, Folder, RawStats};
use crate::fingerprint;
use crate::ngram::{Budget, ProfileBuilder};
use crate::{SniffPolicy, SniffReport};

/// Bytes buffered before the encoding decision is made. The decision
/// needs a prefix — a BOM, a NUL-parity sample, enough high bytes to
/// test UTF-8 sequence validity — and nothing can be normalized until
/// it is made.
pub const HEAD_TARGET: usize = 8 << 10;

// ---------------------------------------------------------------------------
// Admission
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    /// Contiguous profiling from byte zero.
    Prefix,
    /// Discarding bytes between windows.
    Skip,
    /// Discarding up to the next newline so the window opens on a line
    /// boundary and the line-anchored features aren't garbage.
    Align,
    /// Profiling a sampled window.
    Window,
    /// Budget exhausted; nothing further is profiled.
    Done,
}

#[derive(Clone, Debug)]
struct Admission {
    budget: Budget,
    phase: Phase,
    left: u64,
    skip: u64,
    windows_taken: u64,
    profiled: u64,
    /// A discontinuity is pending: the next admitted byte does not
    /// follow the previous admitted byte.
    seam: bool,
}

impl Admission {
    fn new(budget: Budget) -> Self {
        let skip = budget.window.max(1) as u64;
        Self {
            phase: if budget.prefix == 0 { Phase::Skip } else { Phase::Prefix },
            left: if budget.prefix == 0 { skip } else { budget.prefix as u64 },
            skip,
            budget,
            windows_taken: 0,
            profiled: 0,
            seam: false,
        }
    }

    fn advance(&mut self) {
        self.phase = match self.phase {
            Phase::Prefix if self.budget.windows == 0 => {
                self.left = 0;
                Phase::Done
            }
            Phase::Prefix => {
                self.left = self.skip;
                Phase::Skip
            }
            Phase::Skip => {
                // Bound the search for a line boundary so a stream with
                // no newlines at all doesn't skip forever.
                self.left = self.budget.window as u64;
                Phase::Align
            }
            Phase::Align if self.windows_taken >= self.budget.windows as u64 => {
                self.left = 0;
                Phase::Done
            }
            Phase::Align => {
                self.left = self.budget.window as u64;
                self.windows_taken += 1;
                self.seam = true;
                Phase::Window
            }
            Phase::Window => {
                self.skip = self.skip.saturating_mul(2);
                self.left = self.skip;
                Phase::Skip
            }
            Phase::Done => Phase::Done,
        };
    }

    fn done(&self) -> bool {
        self.phase == Phase::Done
    }

    /// Walk a chunk, calling `emit` with each admitted sub-slice and
    /// whether a discontinuity precedes it.
    fn feed<'a>(&mut self, chunk: &'a [u8], mut emit: impl FnMut(&'a [u8], bool)) {
        let max = self.budget.max_bytes as u64;
        let mut i = 0usize;
        loop {
            if self.phase == Phase::Done {
                return;
            }
            if self.profiled >= max {
                self.phase = Phase::Done;
                return;
            }
            if self.left == 0 {
                self.advance();
                continue;
            }
            if i >= chunk.len() {
                return;
            }
            match self.phase {
                Phase::Prefix | Phase::Window => {
                    let room = (max - self.profiled) as usize;
                    let n = (chunk.len() - i).min(self.left as usize).min(room);
                    if n == 0 {
                        self.phase = Phase::Done;
                        return;
                    }
                    let seam = std::mem::take(&mut self.seam);
                    emit(&chunk[i..i + n], seam);
                    self.profiled += n as u64;
                    self.left -= n as u64;
                    i += n;
                }
                Phase::Skip => {
                    let n = (chunk.len() - i).min(self.left as usize);
                    self.left -= n as u64;
                    i += n;
                }
                Phase::Align => {
                    let take = (chunk.len() - i).min(self.left as usize);
                    match chunk[i..i + take].iter().position(|&c| c == b'\n') {
                        Some(pos) => {
                            i += pos + 1;
                            self.left = 0; // aligned; next loop advances to Window
                        }
                        None => {
                            self.left -= take as u64;
                            i += take;
                        }
                    }
                }
                Phase::Done => return,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// SniffStream
// ---------------------------------------------------------------------------

/// Incremental sniffer. Push chunks; ask for a report whenever.
///
/// ```
/// # use shapez_sniff::{SniffStream, Syntax};
/// let mut s = SniffStream::new();
/// for line in 0..500 {
///     s.push(format!("{{\"id\":{line},\"ok\":true}}\n").as_bytes());
/// }
/// assert_eq!(s.finish().best().unwrap().syntax, Syntax::JsonLines);
/// ```
#[derive(Clone, Debug)]
pub struct SniffStream {
    policy: SniffPolicy,
    stats: RawStats,
    /// Raw prefix, retained for the BOM and UTF-8 sequence checks.
    /// Bounded by `HEAD_TARGET`.
    head: Vec<u8>,
    /// `Some` once the encoding has been decided.
    folder: Option<Folder>,
    builder: ProfileBuilder,
    admit: Admission,
    consumed: u64,
}

impl Default for SniffStream {
    fn default() -> Self {
        Self::new()
    }
}

impl SniffStream {
    pub fn new() -> Self {
        Self::with_policy(SniffPolicy::default())
    }

    pub fn with_policy(policy: SniffPolicy) -> Self {
        let builder = ProfileBuilder::new(policy.bigram_cap, policy.trigram_cap);
        let admit = Admission::new(policy.budget);
        Self {
            policy,
            stats: RawStats::default(),
            head: Vec::with_capacity(HEAD_TARGET.min(4096)),
            folder: None,
            builder,
            admit,
            consumed: 0,
        }
    }

    /// Feed the next chunk. Any chunking gives the same answer.
    pub fn push(&mut self, chunk: &[u8]) {
        if chunk.is_empty() {
            return;
        }
        self.consumed += chunk.len() as u64;
        self.stats.push(chunk);

        if self.folder.is_some() {
            self.ingest(chunk);
            return;
        }
        let take = HEAD_TARGET.saturating_sub(self.head.len()).min(chunk.len());
        self.head.extend_from_slice(&chunk[..take]);
        if self.head.len() < HEAD_TARGET {
            return; // still buffering toward the encoding decision
        }
        self.start();
        if take < chunk.len() {
            self.ingest(&chunk[take..]);
        }
    }

    /// Total raw bytes pushed.
    pub fn bytes_consumed(&self) -> u64 {
        debug_assert_eq!(self.consumed, self.stats.bytes());
        self.consumed
    }

    /// Whether the sampling budget is spent. Further pushes cost only
    /// the raw-byte statistics; a caller driving the stream purely to
    /// sniff it can stop here.
    pub fn is_satisfied(&self) -> bool {
        self.admit.done()
    }

    /// Score what has been seen so far without ending the stream.
    pub fn report(&self) -> SniffReport {
        self.snapshot()
    }

    /// End the stream and score it.
    pub fn finish(self) -> SniffReport {
        self.snapshot()
    }

    /// Decide the encoding from the buffered head, then push the head
    /// itself through the newly built folder.
    fn start(&mut self) {
        let alpha = decide(&self.stats, &self.head);
        self.folder = Some(Folder::new(alpha.encoding, alpha.bom_len));
        let head = std::mem::take(&mut self.head);
        self.ingest(&head);
        self.head = head;
    }

    fn ingest(&mut self, raw: &[u8]) {
        let Self { folder, builder, admit, .. } = self;
        let folder = folder.as_mut().expect("ingest only runs after start()");
        let normalized = folder.push(raw);
        admit.feed(&normalized, |slice, seam| {
            if seam {
                builder.boundary();
            }
            builder.push(slice);
        });
    }

    fn snapshot(&self) -> SniffReport {
        let alphabet = decide(&self.stats, &self.head);
        let profile = if self.folder.is_some() {
            self.builder.snapshot()
        } else {
            // The stream ended before the head target was reached, so
            // the encoding decision is being made now. Run the buffered
            // head through a throwaway folder and profile it in one go.
            let mut builder = self.builder.clone();
            let mut admit = self.admit.clone();
            let mut folder = Folder::new(alphabet.encoding, alphabet.bom_len);
            let normalized: Cow<'_, [u8]> = folder.push(&self.head);
            admit.feed(&normalized, |slice, seam| {
                if seam {
                    builder.boundary();
                }
                builder.push(slice);
            });
            builder.snapshot()
        };

        let scored = fingerprint::score(&profile, &alphabet);
        let (candidates, rejected): (Vec<_>, Vec<_>) =
            scored.into_iter().partition(|c| c.confidence >= self.policy.min_confidence);

        SniffReport {
            bytes_profiled: profile.units,
            bytes_total: self.consumed,
            alphabet,
            profile,
            candidates,
            rejected,
        }
    }
}

// ---------------------------------------------------------------------------
// Read adapter
// ---------------------------------------------------------------------------

/// Sniff a reader with the default policy.
///
/// Reads until the sampling budget is spent or the reader is exhausted,
/// whichever comes first. A `Read` cannot seek, so the skipped bytes
/// between sampling windows still have to be read and discarded — the
/// cost is bytes-until-satisfied, not bytes-profiled. A caller who is
/// already streaming the data for another reason should drive
/// [`SniffStream`] directly and pay nothing extra.
pub fn sniff_reader<R: Read>(r: R) -> io::Result<SniffReport> {
    sniff_reader_with(r, &SniffPolicy::default())
}

/// Sniff a reader with an explicit policy.
pub fn sniff_reader_with<R: Read>(mut r: R, policy: &SniffPolicy) -> io::Result<SniffReport> {
    let mut s = SniffStream::with_policy(policy.clone());
    let mut buf = vec![0u8; 64 << 10];
    loop {
        match r.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                s.push(&buf[..n]);
                if s.is_satisfied() {
                    break;
                }
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(s.finish())
}
