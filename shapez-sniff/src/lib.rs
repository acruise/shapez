//! Bitstream syntax discovery: what is this file, and why do we think so.
//!
//! shapez normally starts from decoded values — `JsonEventSink` events
//! driven by an adapter that already knows what the bytes are. This
//! crate is the layer below: given a byte range and no hypothesis at
//! all, work out the code unit, profile the ngram distribution, and
//! score a ranked list of candidate syntaxes with the evidence attached.
//!
//! ```
//! # use shapez_sniff::{sniff, Syntax};
//! let report = sniff(b"{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n{\"a\":4}\n");
//! assert_eq!(report.best().unwrap().syntax, Syntax::JsonLines);
//! ```
//!
//! ## Slices and streams
//!
//! [`sniff`] takes a slice, which assumes the caller already holds every
//! byte. That is the easy case and not the general one. [`SniffStream`]
//! takes the input a chunk at a time in bounded memory, with chunk
//! boundaries invisible to the answer, and [`sniff_reader`] drives it
//! from any [`std::io::Read`]. Use the slice API when the bytes are
//! already in hand — it can seek, so it samples better — and the stream
//! API for anything else.
//!
//! Scope: stages 0-2 of the pipeline in
//! `shapez/SYNTAX_DISCOVERY.md` — code-unit detection, ngram sketching,
//! and fingerprint scoring. Framing detection (stage 3), speculative
//! parsing with the bankruptcy protocol (stage 4), event emission
//! (stage 5), and chaos-driven descent at leaves (stage 6) are
//! specified in that document and not built here.
//!
//! The crate has no dependencies, including on `shapez` itself. That is
//! deliberate: stages 0-2 are pure sketching over bytes and shouldn't
//! drag parser implementations into anybody's dependency graph. The
//! coupling arrives at stage 4, and the doc argues it should arrive
//! behind feature flags in separate crates when it does.
//!
//! ## Reading the answer
//!
//! [`SniffReport::best`] returns the top candidate above the confidence
//! floor, or `None`. `None` is a real answer — "opaque bytes" is a
//! perfectly good shape, and this classifier is deliberately biased
//! toward it. A false negative costs one opaque column; a false
//! positive costs a promotion plan built on a fictional schema.
//!
//! Every candidate carries the [`Evidence`] that produced it, and
//! candidates below the floor are kept in [`SniffReport::rejected`] so
//! "why didn't you say CSV?" is answerable.

mod alphabet;
mod evidence;
mod fingerprint;
mod ngram;
mod stream;

pub use alphabet::{Alphabet, Encoding};
pub use evidence::{atleast, atmost, balance, band, confidence_from_bits, frac, yes_no, Evidence};
pub use fingerprint::{Candidate, Syntax};
pub use ngram::{BraceScan, Budget, Cadence, NgramProfile, ProfileBuilder, SpaceSaving, DELIMITERS};
pub use stream::{sniff_reader, sniff_reader_with, SniffStream, HEAD_TARGET};

/// Knobs for a sniff run.
#[derive(Clone, Debug)]
pub struct SniffPolicy {
    /// Space-Saving cap for the bigram sketch.
    pub bigram_cap: usize,
    /// Space-Saving cap for the trigram sketch; `None` disables
    /// trigrams. Off is the right default under stage-6 recursion,
    /// where the bigram evidence is usually already decisive and the
    /// per-leaf budget is small.
    pub trigram_cap: Option<usize>,
    /// Candidates scoring below this are moved to `rejected`.
    pub min_confidence: f64,
    pub budget: Budget,
}

impl Default for SniffPolicy {
    fn default() -> Self {
        Self {
            bigram_cap: 256,
            trigram_cap: Some(256),
            min_confidence: 0.60,
            budget: Budget::default(),
        }
    }
}

/// The result of a sniff, with every intermediate kept for diagnostics.
#[derive(Clone, Debug)]
pub struct SniffReport {
    pub alphabet: Alphabet,
    pub profile: NgramProfile,
    /// Candidates above the confidence floor, best first. Empty means
    /// "unknown", which is a first-class answer.
    pub candidates: Vec<Candidate>,
    /// Everything that was scored and didn't clear the floor, best
    /// first. Kept so a miss is explainable.
    pub rejected: Vec<Candidate>,
    /// Bytes actually profiled, after the sampling budget.
    pub bytes_profiled: u64,
    /// Raw bytes handed to the sniffer.
    pub bytes_total: u64,
}

impl SniffReport {
    pub fn best(&self) -> Option<&Candidate> {
        self.candidates.first()
    }

    /// Whether the input identified as anything at all.
    pub fn is_unknown(&self) -> bool {
        self.candidates.is_empty()
    }

    /// Look up a candidate's score whether or not it cleared the floor.
    pub fn candidate(&self, syntax: Syntax) -> Option<&Candidate> {
        self.candidates
            .iter()
            .chain(self.rejected.iter())
            .find(|c| c.syntax == syntax)
    }
}

/// Sniff with the default policy.
pub fn sniff(bytes: &[u8]) -> SniffReport {
    sniff_with(bytes, &SniffPolicy::default())
}

/// Sniff with an explicit policy.
pub fn sniff_with(bytes: &[u8], policy: &SniffPolicy) -> SniffReport {
    let alphabet = alphabet::detect(bytes);
    let normalized = alphabet.normalize(bytes);
    let segs = ngram::segments(&normalized, &policy.budget);
    let profile = NgramProfile::build(&segs, policy.bigram_cap, policy.trigram_cap);

    let scored = fingerprint::score(&profile, &alphabet);
    let (candidates, rejected): (Vec<_>, Vec<_>) =
        scored.into_iter().partition(|c| c.confidence >= policy.min_confidence);

    SniffReport {
        bytes_profiled: profile.units,
        bytes_total: bytes.len() as u64,
        alphabet,
        profile,
        candidates,
        rejected,
    }
}

impl std::fmt::Display for SniffReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "{} bytes ({} profiled), {} @ {:.2} bits/byte, text confidence {:.2}",
            self.bytes_total,
            self.bytes_profiled,
            self.alphabet.encoding.name(),
            self.alphabet.entropy_bits,
            self.alphabet.text_confidence
        )?;
        if self.candidates.is_empty() {
            writeln!(f, "  unknown — nothing cleared the confidence floor")?;
        }
        for c in &self.candidates {
            writeln!(f, "  [ {:<11} {:.2} ]  {:+.2} bits", c.syntax.name(), c.confidence, c.bits)?;
        }
        for c in self.rejected.iter().take(3) {
            writeln!(f, "  ( {:<11} {:.2} )  {:+.2} bits", c.syntax.name(), c.confidence, c.bits)?;
        }
        Ok(())
    }
}
