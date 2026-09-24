//! Stage 1: ngram sketches as the universal front end.
//!
//! Unigrams are exact (256 counters, two kilobytes — there is no reason
//! to sketch them). Bigrams and trigrams go through Space-Saving with a
//! small cap, so the same profile is affordable at the root *and* under
//! the per-leaf recursion of stage 6. The sketch's eviction rate carries
//! the same meaning here that it does in the analyzer's cluster
//! sketches: a head that won't stay put means the input has no
//! characteristic distribution.
//!
//! Two features here are not really ngrams but live in the same pass
//! because they need the same bytes: **line-anchored** counts (a
//! line-leading `<` is much stronger XML evidence than a `<` anywhere)
//! and **per-line delimiter cadence** (every line having exactly seven
//! commas says CSV, and nothing else says it).
//!
//! See `shapez/SYNTAX_DISCOVERY.md` § *Stage 1*.

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

/// Space-Saving (Metwally-Agrawal-Abbadi), same shape as the analyzer's.
/// Bounded counters; on overflow evict the lowest and let the newcomer
/// inherit its count.
#[derive(Clone, Debug)]
pub struct SpaceSaving<K: Eq + Hash + Clone> {
    counters: HashMap<K, u64>,
    cap: usize,
    evictions: u64,
    observations: u64,
}

impl<K: Eq + Hash + Clone> SpaceSaving<K> {
    pub fn new(cap: usize) -> Self {
        Self { counters: HashMap::new(), cap: cap.max(1), evictions: 0, observations: 0 }
    }

    pub fn observe(&mut self, key: K) {
        self.observations += 1;
        if let Some(c) = self.counters.get_mut(&key) {
            *c += 1;
            return;
        }
        if self.counters.len() < self.cap {
            self.counters.insert(key, 1);
            return;
        }
        let (min_k, min_c) = self
            .counters
            .iter()
            .min_by_key(|(_, v)| *v)
            .map(|(k, v)| (k.clone(), *v))
            .expect("cap >= 1 so the table is non-empty");
        self.counters.remove(&min_k);
        self.counters.insert(key, min_c + 1);
        self.evictions += 1;
    }

    /// Estimated count. Zero for a key that was never seen *or* was
    /// evicted — Space-Saving over-estimates survivors and forgets the
    /// tail, which is the trade we want for fingerprinting.
    pub fn count(&self, key: &K) -> u64 {
        self.counters.get(key).copied().unwrap_or(0)
    }

    pub fn entries(&self) -> Vec<(K, u64)> {
        let mut v: Vec<_> = self.counters.iter().map(|(k, c)| (k.clone(), *c)).collect();
        v.sort_by_key(|(_, c)| std::cmp::Reverse(*c));
        v
    }

    /// Fraction of observations that displaced an entry. A high rate is
    /// the same "no head to the distribution" signal the analyzer reads
    /// off its cluster sketches.
    pub fn eviction_rate(&self) -> f64 {
        if self.observations == 0 {
            0.0
        } else {
            self.evictions as f64 / self.observations as f64
        }
    }

    pub fn observations(&self) -> u64 {
        self.observations
    }
}

/// Per-line count distribution for one candidate delimiter.
///
/// The discriminating signal is *agreement*, not frequency: prose is
/// full of commas, but prose lines do not all contain exactly the same
/// number of them.
#[derive(Clone, Debug)]
pub struct Cadence {
    pub delim: u8,
    pub lines: u64,
    hist: BTreeMap<u32, u64>,
}

const CADENCE_HIST_CAP: usize = 128;

impl Cadence {
    fn new(delim: u8) -> Self {
        Self { delim, lines: 0, hist: BTreeMap::new() }
    }

    fn observe(&mut self, count: u32) {
        self.lines += 1;
        if let Some(c) = self.hist.get_mut(&count) {
            *c += 1;
        } else if self.hist.len() < CADENCE_HIST_CAP {
            self.hist.insert(count, 1);
        }
        // Past the cap we drop the observation rather than grow. Only
        // reachable when the per-line counts are already so scattered
        // that agreement is near zero anyway.
    }

    /// The most common per-line count and how many lines carried it.
    pub fn modal(&self) -> Option<(u32, u64)> {
        self.hist.iter().max_by_key(|(k, v)| (**v, std::cmp::Reverse(**k))).map(|(k, v)| (*k, *v))
    }

    /// Fraction of lines agreeing with the modal count. Defined as zero
    /// when the modal count is zero — "every line has no commas" is not
    /// evidence of a comma-delimited format.
    pub fn agreement(&self) -> f64 {
        match self.modal() {
            Some((0, _)) | None => 0.0,
            Some((_, lines)) => {
                if self.lines == 0 {
                    0.0
                } else {
                    lines as f64 / self.lines as f64
                }
            }
        }
    }
}

/// JSON-aware bracket accounting: braces inside string literals do not
/// count, and backslash escapes are honored. Harmless (just counting) on
/// input that turns out not to be JSON.
#[derive(Clone, Debug, Default)]
pub struct BraceScan {
    pub obj_open: u64,
    pub obj_close: u64,
    pub arr_open: u64,
    pub arr_close: u64,
    /// Number of string literals opened.
    pub strings: u64,
    pub max_depth: u32,
    /// Non-empty lines at whose end the bracket depth was zero. The
    /// JSON-Lines signal.
    pub depth_zero_lines: u64,
    pub scanned_lines: u64,
    /// A string literal was still open at end of segment.
    pub unterminated_string: bool,
    /// Depth went below zero: more closers than openers at some point.
    pub went_negative: bool,
    pub final_depth: i64,
}

impl BraceScan {
    pub fn balanced(&self) -> bool {
        self.obj_open == self.obj_close && self.arr_open == self.arr_close && !self.went_negative
    }

    /// Fraction of non-empty lines that closed at depth zero.
    pub fn line_framed_frac(&self) -> f64 {
        if self.scanned_lines == 0 {
            0.0
        } else {
            self.depth_zero_lines as f64 / self.scanned_lines as f64
        }
    }
}

/// Everything stage 2 is allowed to look at.
#[derive(Clone, Debug)]
pub struct NgramProfile {
    /// Normalized bytes actually profiled (after sampling).
    pub units: u64,
    pub unigram: [u64; 256],
    pub bigram: SpaceSaving<[u8; 2]>,
    pub trigram: Option<SpaceSaving<[u8; 3]>>,
    pub lines: u64,
    pub blank_lines: u64,
    /// Non-blank lines that are, in their entirety, a single JSON
    /// scalar literal. A stream of bare numbers is legitimately JSON
    /// Lines and has none of the object markers the other JSON
    /// features look for; without this it reads as unstructured text.
    pub scalar_lines: u64,
    pub line_lead: [u64; 256],
    pub line_last: [u64; 256],
    pub line_len_mean: f64,
    pub line_len_stddev: f64,
    /// Leading-whitespace width histogram over non-blank lines, capped.
    pub indents: BTreeMap<u32, u64>,
    pub cadence: Vec<Cadence>,
    pub brace: BraceScan,
    pub first_nonspace: Option<u8>,
    /// How many sampled windows fed this profile. More than one means
    /// bracket balance is approximate at the seams.
    pub segments: usize,
}

/// Candidate field delimiters, in the order `cadence` reports them.
pub const DELIMITERS: [u8; 4] = [b',', b'\t', b';', b'|'];

/// Maximum bytes of a single line buffered for the bare-scalar check.
/// Everything else about a line is tracked with counters, so a
/// single-document JSON stream with one 40 GB "line" costs this much
/// and no more.
const LINE_BUF_CAP: usize = 4096;

/// Incremental, chunk-at-a-time profiler.
///
/// Every per-byte statistic that spans more than one byte — bigrams,
/// trigrams, line geometry, delimiter cadence, bracket depth, string
/// state — is carried across chunk boundaries in explicit state. The
/// profile of a stream fed in 1-byte chunks is bit-identical to the
/// profile of the same bytes fed all at once; `chunking_does_not_change_the_answer`
/// in the test module pins that down.
///
/// The only thing that resets is at an explicit [`boundary`](Self::boundary),
/// which marks a genuine discontinuity in the underlying data — a
/// sampling seam, where the next byte does not follow the previous one.
/// Chunk edges are not boundaries.
#[derive(Clone, Debug)]
pub struct ProfileBuilder {
    p: NgramProfile,
    /// Shift register for gram extraction; survives chunk edges.
    reg: [u8; 3],
    reg_len: usize,
    line: LineState,
    len_sum: f64,
    len_sq: f64,
    // Bracket / string scanner state.
    depth: i64,
    in_str: bool,
    escaped: bool,
    line_had_content: bool,
}

#[derive(Clone, Debug, Default)]
struct LineState {
    len: u32,
    indent: u32,
    in_indent: bool,
    lead: Option<u8>,
    last: Option<u8>,
    last_before: Option<u8>,
    saw_nonspace: bool,
    delims: [u32; DELIMITERS.len()],
    in_quote: bool,
    /// A quote was seen while inside a quoted run; whether it closed the
    /// run or was the first half of a doubled `""` escape depends on the
    /// next byte, which may live in the next chunk.
    quote_pending: bool,
    buf: Vec<u8>,
    buf_overflow: bool,
}

impl LineState {
    fn fresh() -> Self {
        Self { in_indent: true, ..Default::default() }
    }
}

impl ProfileBuilder {
    pub fn new(bigram_cap: usize, trigram_cap: Option<usize>) -> Self {
        Self {
            p: NgramProfile {
                units: 0,
                unigram: [0; 256],
                bigram: SpaceSaving::new(bigram_cap),
                trigram: trigram_cap.map(SpaceSaving::new),
                lines: 0,
                blank_lines: 0,
                scalar_lines: 0,
                line_lead: [0; 256],
                line_last: [0; 256],
                line_len_mean: 0.0,
                line_len_stddev: 0.0,
                indents: BTreeMap::new(),
                cadence: DELIMITERS.iter().map(|&d| Cadence::new(d)).collect(),
                brace: BraceScan::default(),
                first_nonspace: None,
                segments: 0,
            },
            reg: [0; 3],
            reg_len: 0,
            line: LineState::fresh(),
            len_sum: 0.0,
            len_sq: 0.0,
            depth: 0,
            in_str: false,
            escaped: false,
            line_had_content: false,
        }
    }

    /// Feed a chunk. Chunk size is irrelevant to the result.
    pub fn push(&mut self, chunk: &[u8]) {
        for &b in chunk {
            self.p.units += 1;
            self.p.unigram[b as usize] += 1;
            if self.p.first_nonspace.is_none() && !b.is_ascii_whitespace() {
                self.p.first_nonspace = Some(b);
            }

            self.reg = [self.reg[1], self.reg[2], b];
            if self.reg_len < 3 {
                self.reg_len += 1;
            }
            if self.reg_len >= 2 {
                self.p.bigram.observe([self.reg[1], self.reg[2]]);
            }
            if self.reg_len >= 3 {
                if let Some(t) = self.p.trigram.as_mut() {
                    t.observe(self.reg);
                }
            }

            if b == b'\n' {
                self.end_line();
            } else {
                self.line_byte(b);
            }
            self.brace_byte(b);
        }
    }

    /// Mark a discontinuity: the next byte pushed does not follow the
    /// previous one. Flushes the partial line, closes out the bracket
    /// scan, and stops grams from spanning the seam.
    pub fn boundary(&mut self) {
        self.end_line();
        if self.line_had_content {
            self.p.brace.scanned_lines += 1;
            if self.depth == 0 {
                self.p.brace.depth_zero_lines += 1;
            }
        }
        if self.in_str {
            self.p.brace.unterminated_string = true;
        }
        self.p.brace.final_depth += self.depth;
        self.depth = 0;
        self.in_str = false;
        self.escaped = false;
        self.line_had_content = false;
        self.reg_len = 0;
        self.p.segments += 1;
    }

    /// Finalize. Closes the trailing segment and computes the derived
    /// line-length statistics.
    pub fn finish(mut self) -> NgramProfile {
        self.boundary();
        let n = self.p.lines as f64;
        if n > 0.0 {
            self.p.line_len_mean = self.len_sum / n;
            let var = (self.len_sq / n) - self.p.line_len_mean * self.p.line_len_mean;
            self.p.line_len_stddev = var.max(0.0).sqrt();
        }
        self.p
    }

    /// A finalized copy without consuming the builder, so a long-running
    /// stream can be interrogated mid-flight.
    pub fn snapshot(&self) -> NgramProfile {
        self.clone().finish()
    }

    fn line_byte(&mut self, b: u8) {
        let ls = &mut self.line;
        ls.len += 1;
        ls.last_before = ls.last;
        ls.last = Some(b);
        if !b.is_ascii_whitespace() {
            ls.saw_nonspace = true;
        }
        if ls.in_indent {
            if b == b' ' || b == b'\t' {
                ls.indent += 1;
            } else {
                ls.in_indent = false;
                ls.lead = Some(b);
            }
        }
        if !ls.buf_overflow {
            if ls.buf.len() < LINE_BUF_CAP {
                ls.buf.push(b);
            } else {
                // Too long to be a scalar literal by any reasonable
                // reading; stop paying for it.
                ls.buf_overflow = true;
                ls.buf = Vec::new();
            }
        }

        // Delimiters are counted outside quoted runs only. The doubled
        // `""` escape needs one byte of lookahead, which may not have
        // arrived yet — hence the deferred resolution rather than the
        // index arithmetic a slice-at-a-time scanner would use.
        if ls.quote_pending {
            ls.quote_pending = false;
            if b == b'"' {
                return; // doubled quote: an escaped literal, consumed whole
            }
            ls.in_quote = false;
        }
        if b == b'"' {
            if ls.in_quote {
                ls.quote_pending = true;
            } else {
                ls.in_quote = true;
            }
            return;
        }
        if !ls.in_quote {
            for (k, &d) in DELIMITERS.iter().enumerate() {
                if b == d {
                    ls.delims[k] += 1;
                }
            }
        }
    }

    fn end_line(&mut self) {
        let ls = std::mem::replace(&mut self.line, LineState::fresh());
        if !ls.saw_nonspace {
            self.p.blank_lines += 1;
            return;
        }
        self.p.lines += 1;

        // A trailing CR belongs to the line terminator, not the line.
        let (len, last) = match ls.last {
            Some(b'\r') => (ls.len.saturating_sub(1), ls.last_before),
            other => (ls.len, other),
        };
        let l = len as f64;
        self.len_sum += l;
        self.len_sq += l * l;

        if self.p.indents.len() < 64 || self.p.indents.contains_key(&ls.indent) {
            *self.p.indents.entry(ls.indent).or_insert(0) += 1;
        }
        self.p.line_lead[ls.lead.unwrap_or(b' ') as usize] += 1;
        if let Some(b) = last {
            self.p.line_last[b as usize] += 1;
        }
        for (k, c) in ls.delims.iter().enumerate() {
            self.p.cadence[k].observe(*c);
        }
        if !ls.buf_overflow && is_json_scalar(trim_ascii(&ls.buf)) {
            self.p.scalar_lines += 1;
        }
    }

    fn brace_byte(&mut self, b: u8) {
        if self.in_str {
            if self.escaped {
                self.escaped = false;
            } else if b == b'\\' {
                self.escaped = true;
            } else if b == b'"' {
                self.in_str = false;
            }
            if b != b'\n' {
                self.line_had_content = true;
            }
            return;
        }
        match b {
            b'"' => {
                self.in_str = true;
                self.p.brace.strings += 1;
            }
            b'{' => {
                self.p.brace.obj_open += 1;
                self.depth += 1;
            }
            b'}' => {
                self.p.brace.obj_close += 1;
                self.depth -= 1;
            }
            b'[' => {
                self.p.brace.arr_open += 1;
                self.depth += 1;
            }
            b']' => {
                self.p.brace.arr_close += 1;
                self.depth -= 1;
            }
            b'\n' => {
                if self.line_had_content {
                    self.p.brace.scanned_lines += 1;
                    if self.depth == 0 {
                        self.p.brace.depth_zero_lines += 1;
                    }
                }
                self.line_had_content = false;
                return;
            }
            _ => {}
        }
        if !b.is_ascii_whitespace() {
            self.line_had_content = true;
        }
        if self.depth < 0 {
            self.p.brace.went_negative = true;
        }
        self.p.brace.max_depth = self.p.brace.max_depth.max(self.depth.max(0) as u32);
    }
}

impl NgramProfile {
    /// Profile a fixed set of segments. Convenience wrapper over
    /// [`ProfileBuilder`] for callers that already hold every byte.
    pub fn build(segments: &[&[u8]], bigram_cap: usize, trigram_cap: Option<usize>) -> Self {
        let mut b = ProfileBuilder::new(bigram_cap, trigram_cap);
        for (i, seg) in segments.iter().enumerate() {
            if i > 0 {
                b.boundary();
            }
            b.push(seg);
        }
        b.finish()
    }

    // --- accessors used by the fingerprints --------------------------------

    pub fn density(&self, b: u8) -> f64 {
        if self.units == 0 {
            0.0
        } else {
            self.unigram[b as usize] as f64 / self.units as f64
        }
    }

    pub fn count(&self, b: u8) -> u64 {
        self.unigram[b as usize]
    }

    pub fn bigram_density(&self, g: [u8; 2]) -> f64 {
        if self.units == 0 {
            0.0
        } else {
            self.bigram.count(&g) as f64 / self.units as f64
        }
    }

    pub fn trigram_density(&self, g: [u8; 3]) -> f64 {
        match (&self.trigram, self.units) {
            (Some(t), u) if u > 0 => t.count(&g) as f64 / u as f64,
            _ => 0.0,
        }
    }

    /// Fraction of non-blank lines whose first non-indent byte is `b`.
    pub fn lead_frac(&self, b: u8) -> f64 {
        if self.lines == 0 {
            0.0
        } else {
            self.line_lead[b as usize] as f64 / self.lines as f64
        }
    }

    /// Fraction of non-blank lines whose last byte is `b`.
    pub fn last_frac(&self, b: u8) -> f64 {
        if self.lines == 0 {
            0.0
        } else {
            self.line_last[b as usize] as f64 / self.lines as f64
        }
    }

    /// Combined density of every byte in `set`.
    pub fn set_density(&self, set: &[u8]) -> f64 {
        set.iter().map(|&b| self.density(b)).sum()
    }

    /// Fraction of profiled bytes belonging to `pred`.
    pub fn ratio_where(&self, pred: impl Fn(u8) -> bool) -> f64 {
        if self.units == 0 {
            return 0.0;
        }
        let hit: u64 = (0..=255u8).filter(|&b| pred(b)).map(|b| self.unigram[b as usize]).sum();
        hit as f64 / self.units as f64
    }

    /// Fraction of non-blank lines that are a bare JSON scalar.
    pub fn scalar_lines_frac(&self) -> f64 {
        if self.lines == 0 {
            0.0
        } else {
            self.scalar_lines as f64 / self.lines as f64
        }
    }

    pub fn cadence_for(&self, delim: u8) -> Option<&Cadence> {
        self.cadence.iter().find(|c| c.delim == delim)
    }
}

// ---------------------------------------------------------------------------
// Bare-scalar recognition
//
// Strictly lexical — enough to tell `-1.5e3` from `hello`, and nothing
// more. Real parsing is stage 4's job.
// ---------------------------------------------------------------------------

fn trim_ascii(s: &[u8]) -> &[u8] {
    let a = s.iter().position(|b| !b.is_ascii_whitespace()).unwrap_or(s.len());
    let b = s.iter().rposition(|b| !b.is_ascii_whitespace()).map(|i| i + 1).unwrap_or(a);
    &s[a..b]
}

fn is_json_scalar(s: &[u8]) -> bool {
    match s {
        b"true" | b"false" | b"null" => true,
        [b'"', .., b'"'] if s.len() >= 2 => {
            // A closing quote that is itself escaped doesn't close.
            let backslashes = s[..s.len() - 1].iter().rev().take_while(|b| **b == b'\\').count();
            backslashes % 2 == 0
        }
        _ => is_json_number(s),
    }
}

fn is_json_number(s: &[u8]) -> bool {
    let mut i = 0usize;
    let digits = |i: &mut usize| {
        let start = *i;
        while *i < s.len() && s[*i].is_ascii_digit() {
            *i += 1;
        }
        *i > start
    };
    if s.first() == Some(&b'-') {
        i += 1;
    }
    if !digits(&mut i) {
        return false;
    }
    if s.get(i) == Some(&b'.') {
        i += 1;
        if !digits(&mut i) {
            return false;
        }
    }
    if matches!(s.get(i), Some(b'e') | Some(b'E')) {
        i += 1;
        if matches!(s.get(i), Some(b'+') | Some(b'-')) {
            i += 1;
        }
        if !digits(&mut i) {
            return false;
        }
    }
    i == s.len()
}

// ---------------------------------------------------------------------------
// Sampling
// ---------------------------------------------------------------------------

/// Byte budget for a sniff. A naive prefix is biased — files start with
/// headers, BOMs, comment banners and one unrepresentative record — so
/// the sampler takes a prefix *plus* newline-aligned windows from
/// further in.
#[derive(Clone, Copy, Debug)]
pub struct Budget {
    pub prefix: usize,
    pub window: usize,
    pub windows: usize,
    pub max_bytes: usize,
}

impl Default for Budget {
    fn default() -> Self {
        Self { prefix: 64 << 10, window: 32 << 10, windows: 8, max_bytes: 1 << 20 }
    }
}

/// Split the input into the segments to profile. Returns the whole
/// slice when it fits the budget.
pub fn segments<'a>(bytes: &'a [u8], b: &Budget) -> Vec<&'a [u8]> {
    if bytes.len() <= b.max_bytes || bytes.len() <= b.prefix {
        return vec![bytes];
    }
    let mut out = vec![&bytes[..b.prefix]];
    if b.windows == 0 || b.window == 0 {
        return out;
    }
    let rest_start = b.prefix;
    let rest_len = bytes.len() - rest_start;
    let stride = rest_len / b.windows;
    if stride == 0 {
        return out;
    }
    for i in 0..b.windows {
        let nominal = rest_start + i * stride;
        // Align to the byte after the next newline so line-anchored
        // features aren't garbage. Give up on alignment if the window
        // has no newline in reach.
        let aligned = bytes[nominal..(nominal + stride).min(bytes.len())]
            .iter()
            .position(|&c| c == b'\n')
            .map(|off| nominal + off + 1)
            .unwrap_or(nominal);
        let end = (aligned + b.window).min(bytes.len());
        if aligned < end {
            out.push(&bytes[aligned..end]);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile(s: &[u8]) -> NgramProfile {
        NgramProfile::build(&[s], 256, Some(256))
    }

    #[test]
    fn space_saving_forgets_the_tail_and_reports_it() {
        let mut ss: SpaceSaving<u32> = SpaceSaving::new(4);
        for _ in 0..100 {
            ss.observe(1);
        }
        for k in 10..60 {
            ss.observe(k);
        }
        assert!(ss.count(&1) >= 100, "the heavy hitter survives");
        assert!(ss.eviction_rate() > 0.2, "and the churn is visible");
    }

    #[test]
    fn cadence_separates_agreement_from_frequency() {
        // Prose: plenty of commas, no agreement.
        let prose = profile(b"one, two and three\nfour, five, six, seven\neight\nnine, ten\n");
        assert!(prose.cadence_for(b',').unwrap().agreement() < 0.6);

        // CSV: same commas per line, every line.
        let csv = profile(b"a,b,c\n1,2,3\n4,5,6\n7,8,9\n");
        assert_eq!(csv.cadence_for(b',').unwrap().modal(), Some((2, 4)));
        assert_eq!(csv.cadence_for(b',').unwrap().agreement(), 1.0);
    }

    #[test]
    fn cadence_ignores_delimiters_inside_quotes() {
        let csv = profile(b"a,b\n\"x,y\",z\n\"p,q,r\",s\n");
        let c = csv.cadence_for(b',').unwrap();
        assert_eq!(c.modal(), Some((1, 3)));
        assert_eq!(c.agreement(), 1.0);
    }

    #[test]
    fn brace_scan_ignores_brackets_inside_strings() {
        let p = profile(br#"{"a":"}}}}","b":[1,2]}"#);
        assert_eq!(p.brace.obj_open, 1);
        assert_eq!(p.brace.obj_close, 1);
        assert_eq!(p.brace.arr_open, 1);
        assert!(p.brace.balanced());
    }

    #[test]
    fn brace_scan_honors_backslash_escapes() {
        let p = profile(br#"{"a":"he said \"}\" once"}"#);
        assert_eq!(p.brace.obj_open, 1);
        assert_eq!(p.brace.obj_close, 1);
        assert!(p.brace.balanced());
    }

    #[test]
    fn line_framing_is_visible_for_jsonl_and_absent_for_pretty_json() {
        let jsonl = profile(b"{\"a\":1}\n{\"a\":2}\n{\"a\":3}\n");
        assert_eq!(jsonl.brace.line_framed_frac(), 1.0);

        let pretty = profile(b"{\n  \"a\": 1,\n  \"b\": [\n    2\n  ]\n}\n");
        assert!(pretty.brace.line_framed_frac() < 0.3, "{}", pretty.brace.line_framed_frac());
    }

    #[test]
    fn unbalanced_closers_are_flagged() {
        let p = profile(b"} } ] }");
        assert!(p.brace.went_negative);
        assert!(!p.brace.balanced());
    }

    #[test]
    fn line_anchors_are_recorded() {
        let p = profile(b"<a>\n<b>\n<c>\n");
        assert_eq!(p.lead_frac(b'<'), 1.0);
        assert_eq!(p.last_frac(b'>'), 1.0);
    }

    #[test]
    fn indent_widths_are_histogrammed() {
        let p = profile(b"a:\n  b: 1\n  c: 2\n    d: 3\n");
        assert_eq!(p.indents.get(&0), Some(&1));
        assert_eq!(p.indents.get(&2), Some(&2));
        assert_eq!(p.indents.get(&4), Some(&1));
    }

    #[test]
    fn bare_scalar_lines_are_recognized() {
        let p = profile(b"5440\n-5626\n1.5e-3\ntrue\nnull\n\"quoted\"\n");
        assert_eq!(p.lines, 6);
        assert_eq!(p.scalar_lines, 6);
        assert_eq!(p.scalar_lines_frac(), 1.0);
    }

    #[test]
    fn near_miss_scalars_are_rejected() {
        let p = profile(b"hello\n01x\n1.\n-\n1e\nnot a scalar\n");
        assert_eq!(p.lines, 6);
        assert_eq!(p.scalar_lines, 0, "none of these are JSON scalars");
    }

    #[test]
    fn sampling_returns_the_whole_slice_under_budget() {
        let b = Budget::default();
        let data = vec![b'x'; 1000];
        assert_eq!(segments(&data, &b).len(), 1);
    }

    #[test]
    fn sampling_aligns_windows_to_line_boundaries() {
        let b = Budget { prefix: 64, window: 32, windows: 4, max_bytes: 256 };
        let line = b"0123456789abcdef\n";
        let data: Vec<u8> = line.iter().cycle().take(4096).copied().collect();
        let segs = segments(&data, &b);
        assert_eq!(segs.len(), 5, "prefix plus four windows");
        assert_eq!(segs[0].len(), 64);
        for w in &segs[1..] {
            // Aligned windows start just after a newline, so they open
            // at the start of a line.
            assert_eq!(w[0], b'0', "window should open on a line boundary");
        }
    }

    #[test]
    fn empty_input_profiles_without_panicking() {
        let p = profile(b"");
        assert_eq!(p.units, 0);
        assert_eq!(p.density(b'x'), 0.0);
        assert_eq!(p.lead_frac(b'x'), 0.0);
    }
}
