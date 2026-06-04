//! Streaming JSON shape analyzer.
//!
//! Walks `JsonEventSink` events into a flat arena of per-path
//! accumulator nodes; produces a `ShapeNode` tree at `finish()` with
//! record-vs-map and tuple-vs-bag decisions applied.
//!
//! Phase 1 scope: dual-view accumulators (record_view + map_value,
//! positional_view + bag_value), Option A lazy variant emergence at
//! leaves, simple cardinality-based record-vs-map and arity-based
//! tuple-vs-bag heuristics, plus per-array Space-Saving subtree
//! clustering so polymorphic arrays surface as `Array{element:
//! Variant{...}}`. No HLLs yet; cluster sketches are the primary
//! variant signal. Assertions / exception sessions land in later passes.

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

use meta_types::value::{StructField, ValueType};

use crate::ingest::{Analyzer, JsonEventSink};
use crate::node::{ShapeField, ShapeKind, ShapeNode};
use crate::stats::Stats;

type NodeId = usize;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Ord, PartialOrd)]
enum ScalarKind {
    Null,
    Bool,
    I64,
    U64,
    F64,
    String,
}

impl ScalarKind {
    fn to_value_type(self) -> ValueType {
        match self {
            ScalarKind::Null => ValueType::Null,
            ScalarKind::Bool => ValueType::Bool,
            ScalarKind::I64 => ValueType::I64,
            ScalarKind::U64 => ValueType::U64,
            ScalarKind::F64 => ValueType::F64,
            ScalarKind::String => ValueType::String,
        }
    }
}

/// Canonicalized structural signature of a single observed value. Used
/// as the key for per-array subtree clustering. Order-canonical: Record
/// fields are sorted by name; Variant arms are sorted by their own
/// derived order.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd)]
enum Sig {
    Scalar(ScalarKind),
    Array(Box<Sig>),
    Record(Vec<(String, Sig)>),
    /// Heterogeneous element/value within a single container. Arms are
    /// deduped and sorted.
    Variant(Vec<Sig>),
    /// Container with no children observed (empty array/object).
    Empty,
}

fn homogenize(sigs: Vec<Sig>) -> Sig {
    if sigs.is_empty() {
        return Sig::Empty;
    }
    let mut uniq: Vec<Sig> = Vec::new();
    for s in sigs {
        if !uniq.contains(&s) {
            uniq.push(s);
        }
    }
    uniq.sort();
    if uniq.len() == 1 {
        uniq.into_iter().next().unwrap()
    } else {
        Sig::Variant(uniq)
    }
}

// ---------------------------------------------------------------------------
// Space-Saving sketch (Metwally-Agrawal-Abbadi). Bounded counters; on
// overflow, evict the lowest counter and replace its key, inheriting the
// evicted counter as the new entry's starting count. Good enough for
// top-K identification when the head of the distribution is heavy.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct SpaceSaving<K: Eq + Hash + Clone> {
    counters: HashMap<K, u64>,
    cap: usize,
    evictions: u64,
}

impl<K: Eq + Hash + Clone> SpaceSaving<K> {
    fn new(cap: usize) -> Self {
        Self { counters: HashMap::new(), cap, evictions: 0 }
    }

    /// Returns true when the observation caused an eviction (the cap was
    /// full and the lowest-count entry had to be displaced).
    fn observe(&mut self, key: K) -> bool {
        if let Some(c) = self.counters.get_mut(&key) {
            *c += 1;
            return false;
        }
        if self.counters.len() < self.cap {
            self.counters.insert(key, 1);
            return false;
        }
        let (min_k, min_c) = self
            .counters
            .iter()
            .min_by_key(|(_, v)| *v)
            .map(|(k, v)| (k.clone(), *v))
            .unwrap();
        self.counters.remove(&min_k);
        self.counters.insert(key, min_c + 1);
        self.evictions += 1;
        true
    }

    fn entries(&self) -> Vec<(K, u64)> {
        let mut v: Vec<_> = self.counters.iter().map(|(k, c)| (k.clone(), *c)).collect();
        v.sort_by_key(|(_, c)| std::cmp::Reverse(*c));
        v
    }
}

// ---------------------------------------------------------------------------
// DistSketch — DDSketch-style relative-error log-bucket sketch.
//
// Records non-negative, zero, and negative values into separate log-scale
// bucket maps. Bucket index for |v| is ceil(ln|v| / ln γ), where γ is
// derived from the desired relative error ε via γ = (1+ε)/(1-ε). The
// representative value for bucket i is γ^(i - 0.5) (the geometric
// midpoint of the bucket's bounds).
//
// Memory is bounded by `cap` total buckets across the positive and
// negative halves; on overflow we evict the bucket with the lowest
// count. Eviction trades tail fidelity for bounded space — fine for our
// use case where the head of the distribution carries the signal.
// ---------------------------------------------------------------------------

const DIST_EPSILON: f64 = 0.02;
const DIST_CAP: usize = 256;

#[derive(Clone, Debug)]
struct DistSketch {
    gamma: f64,
    log_gamma: f64,
    cap: usize,
    pos: BTreeMap<i32, u64>,
    neg: BTreeMap<i32, u64>,
    zero_count: u64,
    count: u64,
    min: f64,
    max: f64,
    sum: f64,
}

impl DistSketch {
    fn new(epsilon: f64, cap: usize) -> Self {
        let gamma = (1.0 + epsilon) / (1.0 - epsilon);
        Self {
            gamma,
            log_gamma: gamma.ln(),
            cap,
            pos: BTreeMap::new(),
            neg: BTreeMap::new(),
            zero_count: 0,
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            sum: 0.0,
        }
    }

    fn observe(&mut self, v: f64) {
        if !v.is_finite() {
            return;
        }
        self.count += 1;
        if v < self.min {
            self.min = v;
        }
        if v > self.max {
            self.max = v;
        }
        self.sum += v;
        if v == 0.0 {
            self.zero_count += 1;
            return;
        }
        let abs = v.abs();
        let idx = (abs.ln() / self.log_gamma).ceil() as i32;
        let bucket = if v > 0.0 { &mut self.pos } else { &mut self.neg };
        *bucket.entry(idx).or_insert(0) += 1;
        let total = self.pos.len() + self.neg.len();
        if total > self.cap {
            self.evict_smallest();
        }
    }

    fn evict_smallest(&mut self) {
        let pos_min = self.pos.iter().min_by_key(|(_, c)| **c).map(|(k, c)| (*k, *c));
        let neg_min = self.neg.iter().min_by_key(|(_, c)| **c).map(|(k, c)| (*k, *c));
        match (pos_min, neg_min) {
            (Some((p_k, p_c)), Some((n_k, n_c))) => {
                if p_c <= n_c {
                    self.pos.remove(&p_k);
                } else {
                    self.neg.remove(&n_k);
                }
            }
            (Some((p_k, _)), None) => {
                self.pos.remove(&p_k);
            }
            (None, Some((n_k, _))) => {
                self.neg.remove(&n_k);
            }
            (None, None) => {}
        }
    }

    /// q in [0.0, 1.0]. Returns None when no values have been recorded.
    fn quantile(&self, q: f64) -> Option<f64> {
        if self.count == 0 {
            return None;
        }
        let q = q.clamp(0.0, 1.0);
        let target = ((q * self.count as f64).ceil() as u64).max(1);
        let mut cum: u64 = 0;
        for (idx, c) in self.neg.iter().rev() {
            cum += c;
            if cum >= target {
                return Some(-self.bucket_value(*idx));
            }
        }
        cum += self.zero_count;
        if cum >= target {
            return Some(0.0);
        }
        for (idx, c) in self.pos.iter() {
            cum += c;
            if cum >= target {
                return Some(self.bucket_value(*idx));
            }
        }
        Some(self.max)
    }

    fn bucket_value(&self, idx: i32) -> f64 {
        self.gamma.powf(idx as f64 - 0.5)
    }
}

impl Default for DistSketch {
    fn default() -> Self {
        Self::new(DIST_EPSILON, DIST_CAP)
    }
}

#[derive(Debug, Default)]
struct Node {
    obs: u64,
    first_doc: u64,
    last_doc: u64,
    scalar_arms: BTreeMap<ScalarKind, u64>,
    /// Syntactic distribution stats for observed string values at this
    /// path. None until at least one String observation lands here.
    string_stats: Option<StringStats>,
    /// Range/sign/integer-valued stats for observed numbers (i64/u64/f64)
    /// at this path. None until at least one numeric observation lands here.
    numeric_stats: Option<NumericStats>,
    object: Option<ObjectAcc>,
    array: Option<ArrayAcc>,
}

#[derive(Debug)]
struct ObjectAcc {
    obs: u64,
    fields: BTreeMap<String, NodeId>,
    field_order: Vec<String>,
    record_alive: bool,
    map_value: NodeId,
    key_count_sum: u64,
    /// Syntactic stats over every key string observed at this object
    /// position, regardless of whether the record_view retained the key
    /// or routed it through map_value. This is the load-bearing signal
    /// for "all map keys are UUIDs" at high-cardinality positions where
    /// we deliberately don't record individual values.
    key_stats: StringStats,
    /// Per-document key-count distribution. Fed at object_end.
    key_count_sketch: DistSketch,
}

// ---------------------------------------------------------------------------
// Scalar-syntax distribution stats (strings)
// ---------------------------------------------------------------------------

/// Best-match canonical format for an observed string. Detection is
/// cheap (no regex), prefers more-specific formats over less-specific
/// ones, and produces exactly one classification per string.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Ord, PartialOrd)]
enum StringFormat {
    Uuid,
    IsoTimestamp,
    IsoDate,
    Ipv4,
    Ipv6,
    Email,
    UrlHttp,
    AllDigits,
    AllAlpha,
    AlphaNum,
    Other,
}

/// Aggregate syntax stats over a stream of observed strings. Bounded
/// state: counters per format, per log2-length bucket, and a
/// Space-Saving top-K of "shape skeletons" — alphabetic/digit/
/// whitespace runs collapsed to `A`/`9`/space, punctuation preserved.
/// The skeleton is a Splunk-`_punct`-inspired fingerprint that
/// characterizes the syntax of strings that don't match any built-in
/// format (e.g. `ORD-9999-999999` for order IDs).
#[derive(Clone, Debug)]
struct StringStats {
    count: u64,
    min_len: u32,
    max_len: u32,
    sum_len: u64,
    formats: BTreeMap<StringFormat, u64>,
    length_buckets: [u64; 32],
    /// Length distribution sketch for percentile estimation.
    length_sketch: DistSketch,
    /// Top-K shape skeletons. Only fed for strings whose format
    /// resolved to `Other` — known-format strings already have a name
    /// (UUID, ISO-timestamp, …) and don't need a fingerprint.
    skeletons: SpaceSaving<String>,
    other_count: u64,
}

impl Default for StringStats {
    fn default() -> Self {
        Self {
            count: 0,
            min_len: 0,
            max_len: 0,
            sum_len: 0,
            formats: BTreeMap::new(),
            length_buckets: [0; 32],
            length_sketch: DistSketch::default(),
            skeletons: SpaceSaving::new(16),
            other_count: 0,
        }
    }
}

impl StringStats {
    fn observe(&mut self, s: &str) {
        let len = s.len() as u32;
        if self.count == 0 {
            self.min_len = len;
            self.max_len = len;
        } else {
            if len < self.min_len {
                self.min_len = len;
            }
            if len > self.max_len {
                self.max_len = len;
            }
        }
        self.count += 1;
        self.sum_len += len as u64;
        self.length_buckets[len_bucket(s.len())] += 1;
        self.length_sketch.observe(len as f64);
        let format = detect_format(s);
        *self.formats.entry(format).or_insert(0) += 1;
        if format == StringFormat::Other {
            self.other_count += 1;
            self.skeletons.observe(skeleton(s));
        }
    }

    fn mean_len(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum_len as f64 / self.count as f64
        }
    }

    fn dominant_format(&self) -> Option<(StringFormat, u64, f64)> {
        let total: u64 = self.formats.values().sum();
        if total == 0 {
            return None;
        }
        self.formats
            .iter()
            .max_by_key(|(_, c)| **c)
            .map(|(k, c)| (*k, *c, 100.0 * (*c as f64) / (total as f64)))
    }

    /// Returns the dominant skeleton among Other-classified strings
    /// (skeleton, count, percent-of-other) when one exists.
    fn dominant_skeleton(&self) -> Option<(String, u64, f64)> {
        if self.other_count == 0 {
            return None;
        }
        let entries = self.skeletons.entries();
        entries.into_iter().next().map(|(sk, c)| {
            let pct = 100.0 * (c as f64) / (self.other_count as f64);
            (sk, c, pct)
        })
    }
}

// ---------------------------------------------------------------------------
// Scalar-syntax distribution stats (numbers)
// ---------------------------------------------------------------------------

/// Aggregate range/sign/integer-valued stats over observed numbers.
/// All values are projected to f64 for the running stats (precision loss
/// past 2^53 is acceptable for the column-promotion hints this drives);
/// per-kind counts already live in `Node.scalar_arms` and are not
/// duplicated here.
#[derive(Clone, Debug)]
struct NumericStats {
    count: u64,
    min: f64,
    max: f64,
    sum: f64,
    negative: u64,
    zero: u64,
    positive: u64,
    /// Number of observations that were whole-number-valued. Includes
    /// every i64/u64 plus any f64 whose fractional part was zero.
    integer_valued: u64,
    /// Value distribution sketch for percentile estimation.
    sketch: DistSketch,
}

impl Default for NumericStats {
    fn default() -> Self {
        Self {
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            sum: 0.0,
            negative: 0,
            zero: 0,
            positive: 0,
            integer_valued: 0,
            sketch: DistSketch::default(),
        }
    }
}

impl NumericStats {
    fn observe(&mut self, v: f64, kind: ScalarKind) {
        self.count += 1;
        if v < self.min {
            self.min = v;
        }
        if v > self.max {
            self.max = v;
        }
        self.sum += v;
        if v < 0.0 {
            self.negative += 1;
        } else if v == 0.0 {
            self.zero += 1;
        } else {
            self.positive += 1;
        }
        let int_valued = match kind {
            ScalarKind::I64 | ScalarKind::U64 => true,
            ScalarKind::F64 => v.is_finite() && v.fract() == 0.0,
            _ => false,
        };
        if int_valued {
            self.integer_valued += 1;
        }
        self.sketch.observe(v);
    }

    fn mean(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }

    fn all_integer_valued(&self) -> bool {
        self.count > 0 && self.integer_valued == self.count
    }

    fn all_positive(&self) -> bool {
        self.count > 0 && self.positive == self.count
    }

    fn all_non_negative(&self) -> bool {
        self.count > 0 && self.negative == 0
    }

    /// Guess whether the observed values look like an epoch timestamp.
    /// Returns the inferred precision (seconds / millis / micros / nanos)
    /// when every observation is a non-negative integer falling in the
    /// canonical range for that precision (~2001 through ~2065). Returns
    /// None on negatives, on f64 with non-zero fractional part, on
    /// values spanning multiple precision buckets, or when the count
    /// is too small to be confident.
    fn epoch_guess(&self) -> Option<&'static str> {
        const MIN_OBS: u64 = 8;
        if self.count < MIN_OBS {
            return None;
        }
        if !self.all_integer_valued() || !self.all_non_negative() {
            return None;
        }
        // Canonical windows (lower bounds correspond to early 2001;
        // upper bounds correspond to ~2065).
        const SEC_LO: f64 = 9.0e8;
        const SEC_HI: f64 = 3.0e9;
        const MS_LO: f64 = 9.0e11;
        const MS_HI: f64 = 3.0e12;
        const US_LO: f64 = 9.0e14;
        const US_HI: f64 = 3.0e15;
        const NS_LO: f64 = 9.0e17;
        const NS_HI: f64 = 3.0e18;
        let (min, max) = (self.min, self.max);
        if min >= SEC_LO && max <= SEC_HI {
            Some("epoch seconds")
        } else if min >= MS_LO && max <= MS_HI {
            Some("epoch millis")
        } else if min >= US_LO && max <= US_HI {
            Some("epoch micros")
        } else if min >= NS_LO && max <= NS_HI {
            Some("epoch nanos")
        } else {
            None
        }
    }

    /// Compact range description used in the plain-language summary.
    /// Returns Some when the observed values fit a tight, useful
    /// characterization; None when the distribution is too varied for a
    /// one-liner.
    fn range_hint(&self) -> Option<String> {
        if self.count == 0 {
            return None;
        }
        let int = self.all_integer_valued();
        let signs = if self.all_positive() {
            "positive"
        } else if self.all_non_negative() {
            "non-negative"
        } else if self.negative == self.count {
            "negative"
        } else {
            "mixed-sign"
        };
        let kind = if int { "integers" } else { "numbers" };
        if int {
            Some(format!("{signs} {kind} in [{}, {}]", self.min as i64, self.max as i64))
        } else {
            Some(format!("{signs} {kind} in [{:.3}, {:.3}]", self.min, self.max))
        }
    }
}

/// Splunk-`_punct`-inspired skeleton: alphabetic runs collapse to `A`,
/// digit runs to `9`, whitespace runs to a single space, every other
/// character is preserved. Capped at 16 output chars (longer skeletons
/// truncate with `…`). Finite cardinality and stable across strings
/// from the same syntactic family.
fn skeleton(s: &str) -> String {
    const CAP: usize = 16;
    let mut out = String::with_capacity(CAP);
    let mut prev_class: Option<char> = None;
    for c in s.chars() {
        let class = if c.is_alphabetic() {
            'A'
        } else if c.is_ascii_digit() {
            '9'
        } else if c.is_whitespace() {
            ' '
        } else {
            c
        };
        let collapsible = matches!(class, 'A' | '9' | ' ');
        if collapsible && prev_class == Some(class) {
            continue;
        }
        prev_class = Some(class);
        if out.chars().count() >= CAP {
            out.push('…');
            break;
        }
        out.push(class);
    }
    out
}

fn len_bucket(len: usize) -> usize {
    if len < 2 {
        0
    } else {
        (len.ilog2() as usize).min(31)
    }
}

fn detect_format(s: &str) -> StringFormat {
    if is_uuid(s) {
        StringFormat::Uuid
    } else if is_iso_timestamp(s) {
        StringFormat::IsoTimestamp
    } else if is_iso_date(s) {
        StringFormat::IsoDate
    } else if is_ipv4(s) {
        StringFormat::Ipv4
    } else if is_ipv6(s) {
        StringFormat::Ipv6
    } else if is_email(s) {
        StringFormat::Email
    } else if is_url_http(s) {
        StringFormat::UrlHttp
    } else if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) {
        StringFormat::AllDigits
    } else if !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphabetic()) {
        StringFormat::AllAlpha
    } else if !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric()) {
        StringFormat::AlphaNum
    } else {
        StringFormat::Other
    }
}

fn is_uuid(s: &str) -> bool {
    if s.len() != 36 {
        return false;
    }
    let bytes = s.as_bytes();
    for (i, b) in bytes.iter().enumerate() {
        let hyphen = matches!(i, 8 | 13 | 18 | 23);
        if hyphen {
            if *b != b'-' {
                return false;
            }
        } else if !b.is_ascii_hexdigit() {
            return false;
        }
    }
    true
}

fn is_iso_date(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 10 && is_iso_date_prefix(b)
}

fn is_iso_date_prefix(b: &[u8]) -> bool {
    b.len() >= 10
        && b[..4].iter().all(|c| c.is_ascii_digit())
        && b[4] == b'-'
        && b[5].is_ascii_digit()
        && b[6].is_ascii_digit()
        && b[7] == b'-'
        && b[8].is_ascii_digit()
        && b[9].is_ascii_digit()
}

fn is_iso_timestamp(s: &str) -> bool {
    // Permissive: accept any of
    //   2024-01-15T10:30:00
    //   2024-01-15 10:30:00
    //   2024-01-15T10:30:00Z
    //   2024-01-15T10:30:00.123Z
    //   2024-01-15T10:30:00+05:30
    //   2024-01-15T10:30:00.123456789-08:00
    // Validates the YYYY-MM-DD<sep>HH:MM:SS prefix and (when present) a
    // suffix that is one of: Z, +HH:MM, -HH:MM, .<digits>, or .<digits>
    // followed by a Z / offset.
    let b = s.as_bytes();
    if b.len() < 19 {
        return false;
    }
    if !is_iso_date_prefix(b) {
        return false;
    }
    let sep = b[10];
    if sep != b'T' && sep != b' ' {
        return false;
    }
    let prefix_ok = b[11].is_ascii_digit()
        && b[12].is_ascii_digit()
        && b[13] == b':'
        && b[14].is_ascii_digit()
        && b[15].is_ascii_digit()
        && b[16] == b':'
        && b[17].is_ascii_digit()
        && b[18].is_ascii_digit();
    if !prefix_ok {
        return false;
    }
    if b.len() == 19 {
        return true;
    }
    // Validate the suffix.
    let mut i = 19;
    if b[i] == b'.' {
        i += 1;
        let start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        if i == start {
            return false; // dot with no digits
        }
    }
    if i == b.len() {
        return true; // ended after fractional seconds
    }
    match b[i] {
        b'Z' => i + 1 == b.len(),
        b'+' | b'-' => {
            // ±HH:MM or ±HHMM
            let rest = &b[i + 1..];
            match rest.len() {
                4 => rest.iter().all(|c| c.is_ascii_digit()),
                5 => {
                    rest[0].is_ascii_digit()
                        && rest[1].is_ascii_digit()
                        && rest[2] == b':'
                        && rest[3].is_ascii_digit()
                        && rest[4].is_ascii_digit()
                }
                _ => false,
            }
        }
        _ => false,
    }
}

fn is_ipv4(s: &str) -> bool {
    let parts: Vec<&str> = s.split('.').collect();
    if parts.len() != 4 {
        return false;
    }
    for p in parts {
        if p.is_empty() || p.len() > 3 || !p.bytes().all(|b| b.is_ascii_digit()) {
            return false;
        }
        match p.parse::<u16>() {
            Ok(n) if n <= 255 => (),
            _ => return false,
        }
    }
    true
}

fn is_ipv6(s: &str) -> bool {
    // Cheap heuristic: contains ':', no spaces, all chars are hex or ':',
    // length 2..=39, and at least two colons or a "::" group.
    let len = s.len();
    if !(2..=39).contains(&len) {
        return false;
    }
    let mut colons = 0;
    for b in s.bytes() {
        if b == b':' {
            colons += 1;
        } else if !b.is_ascii_hexdigit() {
            return false;
        }
    }
    colons >= 2
}

fn is_email(s: &str) -> bool {
    match s.find('@') {
        Some(i) => i > 0 && i < s.len() - 1 && !s.contains(' '),
        None => false,
    }
}

fn is_url_http(s: &str) -> bool {
    s.starts_with("http://") || s.starts_with("https://")
}

#[derive(Debug)]
struct ArrayAcc {
    obs: u64,
    positional: Vec<NodeId>,
    positional_alive: bool,
    bag_value: NodeId,
    length_sum: u64,
    min_length: u32,
    max_length: u32,
    length_histogram: BTreeMap<u32, u64>,
    /// Per-document array-length distribution. Fed at array_end.
    length_sketch: DistSketch,
    /// Per-element subtree-signature clustering. Cross-document.
    element_cluster: SpaceSaving<Sig>,
}

enum Frame {
    /// Object container. As keys arrive and values complete, child sigs
    /// accumulate here; on object_end we compute this object's overall
    /// Sig::Record.
    Object {
        node: NodeId,
        pending_key: Option<String>,
        children: Vec<(String, Sig)>,
    },
    /// Array container. As elements complete, their sigs feed the array
    /// node's cluster sketch AND accumulate locally so this array's
    /// overall Sig::Array(homogenized) can be reported to its parent.
    Array {
        node: NodeId,
        position: u32,
        children: Vec<Sig>,
    },
}

/// Operator-tunable policy for an analyzer run. Carries the cost knobs
/// from the *Cost, briefly* section of DESIGN.md plus an audit-reason
/// string and chaos-bailout thresholds. The defaults are the Phase 1
/// baseline used by streaming ingest; batch runs override them.
#[derive(Clone, Debug)]
pub struct AnalyzerPolicy {
    pub record_view_cap: usize,
    pub positional_view_cap: usize,
    pub cluster_cap: usize,
    /// Optional audit reason carried through to outputs. Empty for
    /// routine streaming ingest; populated when an operator wants to
    /// label a batch run (an investigation, a follow-up re-analysis,
    /// a scheduled re-pass with a richer policy).
    pub reason: String,
    /// Bail out if total cluster evictions per processed document
    /// exceeds this rate (e.g. 0.5 = half the docs are evicting). None
    /// disables the check. Sources are expected to poll `should_bail()`
    /// periodically.
    pub max_eviction_rate: Option<f64>,
    /// Bail out after this many documents have been processed. Useful
    /// for "show me what you've got after 10k docs" investigations.
    pub max_docs: Option<u64>,
    /// Don't trigger eviction-rate bailout before this many documents
    /// have been seen — sketches need warmup before their rate is
    /// meaningful.
    pub min_docs_before_bail: u64,
}

impl Default for AnalyzerPolicy {
    fn default() -> Self {
        Self {
            record_view_cap: 64,
            positional_view_cap: 32,
            cluster_cap: 16,
            reason: String::new(),
            max_eviction_rate: None,
            max_docs: None,
            min_docs_before_bail: 100,
        }
    }
}

pub struct StreamingAnalyzer {
    arena: Vec<Node>,
    root: NodeId,
    doc_count: u64,
    current_doc: u64,
    pending: Option<NodeId>,
    stack: Vec<Frame>,
    policy: AnalyzerPolicy,
    /// Sum of `SpaceSaving::evictions` across every cluster sketch in
    /// the arena. Maintained incrementally; consulted by `should_bail`.
    total_cluster_evictions: u64,
}

impl StreamingAnalyzer {
    pub fn new() -> Self {
        Self::with_policy(AnalyzerPolicy::default())
    }

    pub fn with_policy(policy: AnalyzerPolicy) -> Self {
        let mut arena = Vec::new();
        let root = alloc_node(&mut arena);
        Self {
            arena,
            root,
            doc_count: 0,
            current_doc: 0,
            pending: None,
            stack: Vec::new(),
            policy,
            total_cluster_evictions: 0,
        }
    }

    pub fn with_caps(record_view_cap: usize, positional_view_cap: usize) -> Self {
        Self::with_policy(AnalyzerPolicy {
            record_view_cap,
            positional_view_cap,
            ..AnalyzerPolicy::default()
        })
    }

    pub fn policy(&self) -> &AnalyzerPolicy {
        &self.policy
    }

    /// Total cluster evictions across the entire arena, useful for chaos
    /// detection. Maintained incrementally — O(1) to query.
    pub fn total_cluster_evictions(&self) -> u64 {
        self.total_cluster_evictions
    }

    /// Returns Some(reason) when the policy says we should stop. Batch
    /// `DocumentSource::drive` impls poll this after every document
    /// and exit cleanly when it returns Some. Bailout is suppressed
    /// for the first `policy.min_docs_before_bail` documents because
    /// the sketches need warmup to make the rate meaningful.
    pub fn should_bail(&self) -> Option<&'static str> {
        if let Some(cap) = self.policy.max_docs {
            if self.doc_count >= cap {
                return Some("doc count cap reached");
            }
        }
        if self.doc_count < self.policy.min_docs_before_bail {
            return None;
        }
        if let Some(max_rate) = self.policy.max_eviction_rate {
            let rate = self.total_cluster_evictions as f64 / self.doc_count as f64;
            if rate > max_rate {
                return Some("cluster eviction rate exceeded threshold");
            }
        }
        None
    }

    pub fn doc_count(&self) -> u64 {
        self.doc_count
    }

    /// Render a human-readable description of the analyzer's current
    /// state: per-path observation counts, the dual-view accumulators,
    /// the decision that would be made at finalization (record vs map,
    /// tuple vs bag), and the top-K element clusters at each array.
    /// Walks `&self` so callers can produce a report without consuming
    /// the analyzer.
    pub fn report(&self) -> String {
        let mut out = String::new();
        use std::fmt::Write as _;
        let _ = writeln!(
            &mut out,
            "shapez analyzer report — {} documents",
            self.doc_count
        );
        let _ = writeln!(&mut out);
        let _ = writeln!(&mut out, "{}", self.summary());
        self.write_node(&mut out, self.root, ".", 0);
        out
    }

    /// Plain-language paragraph describing the root shape and the
    /// decisions that resolved it. Deterministic, ~2-5 lines. Callable
    /// independently of `report()` for callers that only want the
    /// headline.
    pub fn summary(&self) -> String {
        let mut out = String::new();
        use std::fmt::Write as _;

        if self.doc_count == 0 {
            let _ = writeln!(&mut out, "Summary: no documents observed.");
            return out;
        }

        let n = &self.arena[self.root];
        let mut kinds: Vec<&str> = Vec::new();
        if !n.scalar_arms.is_empty() {
            kinds.push("scalar");
        }
        if n.object.is_some() {
            kinds.push("object");
        }
        if n.array.is_some() {
            kinds.push("array");
        }

        let _ = writeln!(
            &mut out,
            "Summary: {} document{} observed.",
            self.doc_count,
            if self.doc_count == 1 { "" } else { "s" },
        );

        match kinds.len() {
            0 => {
                let _ = writeln!(&mut out, "No root values were observed.");
            }
            1 => {
                let kind = kinds[0];
                if kind == "scalar" {
                    let _ = writeln!(&mut out, "Each document is a single scalar ({}).", scalar_arms_summary(n));
                    if let Some(ss) = &n.string_stats {
                        if let Some((fmt, _, pct)) = ss.dominant_format() {
                            if pct >= 90.0 {
                                if fmt == StringFormat::Other {
                                    if let Some((sk, _, sk_pct)) = ss.dominant_skeleton() {
                                        if sk_pct >= 90.0 {
                                            let _ = writeln!(
                                                &mut out,
                                                "  Strings follow pattern `{sk}` ({sk_pct:.0}% of unrecognized strings; lengths {}..{}).",
                                                ss.min_len, ss.max_len,
                                            );
                                        }
                                    }
                                } else {
                                    let _ = writeln!(
                                        &mut out,
                                        "  Strings are {:.0}% {} (lengths {}..{}).",
                                        pct,
                                        format_name(fmt),
                                        ss.min_len,
                                        ss.max_len,
                                    );
                                }
                            }
                        }
                    }
                    if let Some(ns) = &n.numeric_stats {
                        if let Some(hint) = ns.range_hint() {
                            let mut suffix = String::new();
                            if let Some(guess) = ns.epoch_guess() {
                                suffix.push_str(&format!(" — looks like {guess}"));
                            }
                            if let (Some(p50), Some(p99)) =
                                (ns.sketch.quantile(0.5), ns.sketch.quantile(0.99))
                            {
                                if p50 > 0.0 && p99 / p50 >= 10.0 {
                                    suffix.push_str(&format!(
                                        " — long-tailed (p50≈{}, p99≈{})",
                                        format_num(p50, ns.all_integer_valued()),
                                        format_num(p99, ns.all_integer_valued()),
                                    ));
                                }
                            }
                            let _ = writeln!(&mut out, "  Numbers: {hint}{suffix}.");
                        }
                    }
                } else if kind == "object" {
                    summarize_object(&mut out, n.object.as_ref().unwrap(), self, "Each document");
                } else if kind == "array" {
                    summarize_array(&mut out, n.array.as_ref().unwrap(), self, "Each document");
                }
            }
            _ => {
                let _ = writeln!(
                    &mut out,
                    "Document shape varies across {} top-level kinds: {}.",
                    kinds.len(),
                    kinds.join(", "),
                );
                if !n.scalar_arms.is_empty() {
                    let _ = writeln!(&mut out, "  Scalar arms: {}.", scalar_arms_summary(n));
                }
                if let Some(obj) = &n.object {
                    summarize_object(&mut out, obj, self, "When object-shaped, each document");
                }
                if let Some(arr) = &n.array {
                    summarize_array(&mut out, arr, self, "When array-shaped, each document");
                }
            }
        }

        // Trailing newline to separate from the tree dump.
        let _ = writeln!(&mut out);
        out
    }

    fn write_node(&self, out: &mut String, id: NodeId, path: &str, depth: usize) {
        use std::fmt::Write as _;
        let n = &self.arena[id];
        let indent = "  ".repeat(depth);
        let _ = writeln!(
            out,
            "{indent}{path}  ({} obs, docs {}..{})",
            n.obs, n.first_doc, n.last_doc,
        );

        let mut arms: Vec<&'static str> = Vec::new();
        if !n.scalar_arms.is_empty() {
            arms.push("scalar");
        }
        if n.object.is_some() {
            arms.push("object");
        }
        if n.array.is_some() {
            arms.push("array");
        }
        if arms.len() > 1 {
            let _ = writeln!(
                out,
                "{indent}  variant arms observed: {}",
                arms.join(", ")
            );
        }

        if !n.scalar_arms.is_empty() {
            let total: u64 = n.scalar_arms.values().sum();
            let _ = writeln!(out, "{indent}  scalar arms:");
            for (kind, count) in &n.scalar_arms {
                let pct = 100.0 * (*count as f64) / (total.max(1) as f64);
                let _ = writeln!(
                    out,
                    "{indent}    {:?}  ×{}  ({:.1}%)",
                    kind, count, pct
                );
            }
        }

        if let Some(ss) = &n.string_stats {
            write_string_stats(out, ss, &indent, "string values");
        }
        if let Some(ns) = &n.numeric_stats {
            write_numeric_stats(out, ns, &indent);
        }

        if let Some(obj) = &n.object {
            self.write_object(out, obj, path, depth + 1);
        }
        if let Some(arr) = &n.array {
            self.write_array(out, arr, path, depth + 1);
        }
    }

    fn write_object(&self, out: &mut String, obj: &ObjectAcc, parent_path: &str, depth: usize) {
        use std::fmt::Write as _;
        let indent = "  ".repeat(depth);
        let decision = decide_object(obj);
        let total = obj.obs.max(1);
        let mean_keys = obj.key_count_sum as f64 / total as f64;

        let _ = writeln!(
            out,
            "{indent}object: {} obs, {} unique field(s), mean {:.1} keys/doc, record_view {}",
            obj.obs,
            obj.field_order.len(),
            mean_keys,
            if obj.record_alive { "alive" } else { "DROPPED" },
        );
        if let Some(line) = format_percentiles(&obj.key_count_sketch, true) {
            let _ = writeln!(out, "{indent}  keys-per-doc {line}");
        }
        match &decision {
            ObjectDecision::Record { reason } => {
                let _ = writeln!(out, "{indent}decision: RECORD  ({reason})");
            }
            ObjectDecision::Map { reason } => {
                let _ = writeln!(out, "{indent}decision: MAP     ({reason})");
            }
        }

        if obj.key_stats.count > 0 {
            write_string_stats(out, &obj.key_stats, &indent, "object keys");
        }

        match decision {
            ObjectDecision::Record { .. } => {
                let _ = writeln!(out, "{indent}fields:");
                for name in &obj.field_order {
                    let cid = obj.fields[name];
                    let child_obs = self.arena[cid].obs;
                    let presence = 100.0 * child_obs as f64 / total as f64;
                    let nullable = child_obs < obj.obs;
                    let null_tag = if nullable { "  nullable" } else { "  required" };
                    let _ = writeln!(
                        out,
                        "{indent}  .{name}  ({child_obs}/{} = {:.1}%{null_tag})",
                        obj.obs, presence,
                    );
                    let cp = if parent_path == "." {
                        format!(".{name}")
                    } else {
                        format!("{parent_path}.{name}")
                    };
                    self.write_node(out, cid, &cp, depth + 2);
                }
            }
            ObjectDecision::Map { .. } => {
                let wildcard = if parent_path == "." {
                    ".*".to_string()
                } else {
                    format!("{parent_path}.*")
                };
                let _ = writeln!(out, "{indent}canonical pattern: {wildcard}");
                if obj.record_alive && !obj.field_order.is_empty() {
                    let _ = writeln!(
                        out,
                        "{indent}(record_view still alive at {} unique keys — kept for reporting)",
                        obj.field_order.len()
                    );
                }
                let _ = writeln!(out, "{indent}map_value @ {wildcard}");
                self.write_node(out, obj.map_value, &wildcard, depth + 1);
            }
        }
    }

    fn write_array(&self, out: &mut String, arr: &ArrayAcc, parent_path: &str, depth: usize) {
        use std::fmt::Write as _;
        let indent = "  ".repeat(depth);
        let total = arr.obs.max(1);
        let decision = decide_array(arr);

        let min = if arr.min_length == u32::MAX { 0 } else { arr.min_length };
        let mean_len = arr.length_sum as f64 / total as f64;
        let _ = writeln!(
            out,
            "{indent}array: {} obs, lengths {}..{}, mean {:.1}/doc, positional_view {}",
            arr.obs,
            min,
            arr.max_length,
            mean_len,
            if arr.positional_alive { "alive" } else { "DROPPED" },
        );
        if let Some(line) = format_percentiles(&arr.length_sketch, true) {
            let _ = writeln!(out, "{indent}  length {line}");
        }
        let _ = writeln!(out, "{indent}length distribution:");
        for (len, count) in &arr.length_histogram {
            let pct = 100.0 * (*count as f64) / total as f64;
            let _ = writeln!(out, "{indent}  len={len}  ×{count}  ({:.1}%)", pct);
        }

        let cluster = &arr.element_cluster;
        let entries = cluster.entries();
        let _ = writeln!(
            out,
            "{indent}element cluster (Space-Saving cap={}): {} arms, {} evictions",
            cluster.cap,
            entries.len(),
            cluster.evictions,
        );
        for (sig, count) in &entries {
            let mut sig_str = String::new();
            write_sig(&mut sig_str, sig);
            let _ = writeln!(out, "{indent}  ×{count}  {sig_str}");
        }

        match &decision {
            ArrayDecision::Tuple { mode_len, mode_share } => {
                let _ = writeln!(
                    out,
                    "{indent}decision: TUPLE  (mode_len={mode_len}, mode_share={:.1}%)",
                    100.0 * mode_share,
                );
                let _ = writeln!(out, "{indent}positions:");
                for (i, cid) in arr.positional.iter().take(*mode_len as usize).enumerate() {
                    let cp = if parent_path == "." {
                        format!(".[{i}]")
                    } else {
                        format!("{parent_path}[{i}]")
                    };
                    self.write_node(out, *cid, &cp, depth + 1);
                }
            }
            ArrayDecision::Bag { reason } => {
                let _ = writeln!(out, "{indent}decision: BAG    ({reason})");
                let wildcard = if parent_path == "." {
                    ".[*]".to_string()
                } else {
                    format!("{parent_path}[*]")
                };
                let _ = writeln!(out, "{indent}canonical pattern: {wildcard}");
                let _ = writeln!(out, "{indent}bag_value @ {wildcard}");
                self.write_node(out, arr.bag_value, &wildcard, depth + 1);
                if arr.positional_alive && !arr.positional.is_empty() {
                    let _ = writeln!(
                        out,
                        "{indent}(positional view also retained at {} positions \u{2014} for completeness)",
                        arr.positional.len()
                    );
                    for (i, cid) in arr.positional.iter().enumerate() {
                        let obs = self.arena[*cid].obs;
                        let _ = writeln!(out, "{indent}  [{i}] {} obs (not used by bag decision)", obs);
                    }
                }
            }
        }
    }

    fn alloc(&mut self) -> NodeId {
        alloc_node(&mut self.arena)
    }

    fn obs_at(&mut self, id: NodeId) {
        let n = &mut self.arena[id];
        if n.obs == 0 {
            n.first_doc = self.current_doc;
        }
        n.obs += 1;
        n.last_doc = self.current_doc;
    }

    fn consume_target(&mut self) -> NodeId {
        if let Some(t) = self.pending.take() {
            return t;
        }
        let (arr_node, pos) = {
            let frame = self.stack.last_mut().expect("event outside document");
            match frame {
                Frame::Array { node, position, .. } => {
                    let arr_node = *node;
                    let pos = *position;
                    *position += 1;
                    (arr_node, pos)
                }
                Frame::Object { .. } => {
                    panic!("event inside object without preceding object_key")
                }
            }
        };
        self.array_child(arr_node, pos)
    }

    fn array_child(&mut self, arr_node: NodeId, position: u32) -> NodeId {
        let (bag_value, alive, positional_len, cap) = {
            let arr = self.arena[arr_node]
                .array
                .as_ref()
                .expect("array_child on non-array");
            (
                arr.bag_value,
                arr.positional_alive,
                arr.positional.len(),
                self.policy.positional_view_cap,
            )
        };
        if !alive {
            return bag_value;
        }
        if (position as usize) < positional_len {
            return self.arena[arr_node].array.as_ref().unwrap().positional[position as usize];
        }
        if (position as usize) >= cap {
            self.arena[arr_node].array.as_mut().unwrap().positional_alive = false;
            return bag_value;
        }
        let new_id = self.alloc();
        self.arena[arr_node].array.as_mut().unwrap().positional.push(new_id);
        new_id
    }

    fn ensure_object(&mut self, id: NodeId) {
        if self.arena[id].object.is_none() {
            let map_value = self.alloc();
            self.arena[id].object = Some(ObjectAcc {
                obs: 0,
                fields: BTreeMap::new(),
                field_order: Vec::new(),
                record_alive: true,
                map_value,
                key_count_sum: 0,
                key_stats: StringStats::default(),
                key_count_sketch: DistSketch::default(),
            });
        }
    }

    fn ensure_array(&mut self, id: NodeId) {
        if self.arena[id].array.is_none() {
            let bag_value = self.alloc();
            let cluster_cap = self.policy.cluster_cap;
            self.arena[id].array = Some(ArrayAcc {
                obs: 0,
                positional: Vec::new(),
                positional_alive: true,
                bag_value,
                length_sum: 0,
                min_length: u32::MAX,
                max_length: 0,
                length_histogram: BTreeMap::new(),
                length_sketch: DistSketch::default(),
                element_cluster: SpaceSaving::new(cluster_cap),
            });
        }
    }

    fn object_child_for_key(&mut self, obj_node: NodeId, key: &str) -> NodeId {
        let cap = self.policy.record_view_cap;
        if let Some(&child) = self.arena[obj_node].object.as_ref().unwrap().fields.get(key) {
            return child;
        }
        let (map_value, record_alive, fields_len) = {
            let obj = self.arena[obj_node].object.as_ref().unwrap();
            (obj.map_value, obj.record_alive, obj.fields.len())
        };
        if record_alive && fields_len < cap {
            let new_id = self.alloc();
            let obj = self.arena[obj_node].object.as_mut().unwrap();
            obj.fields.insert(key.to_string(), new_id);
            obj.field_order.push(key.to_string());
            return new_id;
        }
        if record_alive {
            self.arena[obj_node].object.as_mut().unwrap().record_alive = false;
        }
        map_value
    }

    fn observe_scalar(&mut self, kind: ScalarKind) {
        let t = self.consume_target();
        self.obs_at(t);
        *self.arena[t].scalar_arms.entry(kind).or_insert(0) += 1;
        self.emit_child_sig(Sig::Scalar(kind));
    }

    fn observe_number(&mut self, kind: ScalarKind, value: f64) {
        let t = self.consume_target();
        self.obs_at(t);
        *self.arena[t].scalar_arms.entry(kind).or_insert(0) += 1;
        self.arena[t]
            .numeric_stats
            .get_or_insert_with(NumericStats::default)
            .observe(value, kind);
        self.emit_child_sig(Sig::Scalar(kind));
    }

    fn observe_string(&mut self, s: &str) {
        let t = self.consume_target();
        self.obs_at(t);
        *self.arena[t].scalar_arms.entry(ScalarKind::String).or_insert(0) += 1;
        self.arena[t]
            .string_stats
            .get_or_insert_with(StringStats::default)
            .observe(s);
        self.emit_child_sig(Sig::Scalar(ScalarKind::String));
    }

    /// A child value has just completed. Inform the parent frame (the
    /// container we're inside): for arrays, push into the element
    /// cluster sketch and accumulate; for objects, attach to the
    /// pending key. If there's no parent frame, the value was a root
    /// document — nothing to report.
    fn emit_child_sig(&mut self, sig: Sig) {
        let frame = match self.stack.last_mut() {
            Some(f) => f,
            None => return,
        };
        match frame {
            Frame::Array { node, children, .. } => {
                let arr_node = *node;
                children.push(sig.clone());
                let evicted = {
                    let arr = self.arena[arr_node].array.as_mut().unwrap();
                    arr.element_cluster.observe(sig)
                };
                if evicted {
                    self.total_cluster_evictions += 1;
                }
            }
            Frame::Object { pending_key, children, .. } => {
                let key = pending_key.take().expect("object emitted child without preceding key");
                children.push((key, sig));
            }
        }
    }
}

fn alloc_node(arena: &mut Vec<Node>) -> NodeId {
    let id = arena.len();
    arena.push(Node::default());
    id
}

// ---------------------------------------------------------------------------
// Decisions (shared between Finalizer and the report printer)
// ---------------------------------------------------------------------------

enum ObjectDecision {
    Record { reason: &'static str },
    Map { reason: &'static str },
}

enum ArrayDecision {
    Tuple { mode_len: u32, mode_share: f64 },
    Bag { reason: &'static str },
}

fn decide_object(obj: &ObjectAcc) -> ObjectDecision {
    let total = obj.obs.max(1);
    let unique = obj.field_order.len();
    let mean_keys = obj.key_count_sum as f64 / total as f64;

    if !obj.record_alive {
        return ObjectDecision::Map { reason: "record_view OVERFLOWED at cap" };
    }
    if unique >= 16 && (unique as f64) > mean_keys.max(1.0) * 4.0 {
        return ObjectDecision::Map {
            reason: "unique key count >> typical per-doc keys",
        };
    }
    ObjectDecision::Record { reason: "stable low-cardinality key set" }
}

fn decide_array(arr: &ArrayAcc) -> ArrayDecision {
    let total = arr.obs.max(1);
    let (mode_len, mode_count) = arr
        .length_histogram
        .iter()
        .max_by_key(|(_, c)| **c)
        .map(|(l, c)| (*l, *c))
        .unwrap_or((0, 0));
    let mode_share = mode_count as f64 / total as f64;
    let positional_useful = arr.positional_alive && !arr.positional.is_empty();
    let small_mode = mode_len > 0 && mode_len <= 16;
    let tight = mode_share >= 0.9;

    if positional_useful && small_mode && tight {
        return ArrayDecision::Tuple { mode_len, mode_share };
    }
    let reason = if !positional_useful {
        "positional view unusable"
    } else if !small_mode {
        "mode length too large for tuple"
    } else {
        "arity not tight enough"
    };
    ArrayDecision::Bag { reason }
}

fn write_sig(out: &mut String, sig: &Sig) {
    match sig {
        Sig::Empty => out.push_str("empty"),
        Sig::Scalar(k) => out.push_str(scalar_name(*k)),
        Sig::Array(inner) => {
            out.push('[');
            write_sig(out, inner);
            out.push(']');
        }
        Sig::Record(fields) => {
            out.push_str("record{");
            for (i, (name, _)) in fields.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                out.push_str(name);
            }
            out.push('}');
        }
        Sig::Variant(arms) => {
            out.push('(');
            for (i, a) in arms.iter().enumerate() {
                if i > 0 {
                    out.push_str(" | ");
                }
                write_sig(out, a);
            }
            out.push(')');
        }
    }
}

// ---------------------------------------------------------------------------
// Summary helpers
// ---------------------------------------------------------------------------

fn scalar_arms_summary(n: &Node) -> String {
    let total: u64 = n.scalar_arms.values().sum();
    if n.scalar_arms.len() == 1 {
        let (k, _) = n.scalar_arms.iter().next().unwrap();
        return scalar_name(*k).into();
    }
    let mut parts: Vec<(ScalarKind, u64)> =
        n.scalar_arms.iter().map(|(k, c)| (*k, *c)).collect();
    parts.sort_by_key(|(_, c)| std::cmp::Reverse(*c));
    let render: Vec<String> = parts
        .iter()
        .map(|(k, c)| {
            let pct = 100.0 * (*c as f64) / (total.max(1) as f64);
            format!("{} {:.0}%", scalar_name(*k), pct)
        })
        .collect();
    format!("variant of {} — {}", parts.len(), render.join(", "))
}

fn summarize_object(out: &mut String, obj: &ObjectAcc, sa: &StreamingAnalyzer, subject: &str) {
    use std::fmt::Write as _;
    let decision = decide_object(obj);
    let mean_keys = obj.key_count_sum as f64 / obj.obs.max(1) as f64;
    match decision {
        ObjectDecision::Record { .. } => {
            let mut required: Vec<&str> = Vec::new();
            let mut optional: Vec<&str> = Vec::new();
            for name in &obj.field_order {
                let cid = obj.fields[name];
                if sa.arena[cid].obs < obj.obs {
                    optional.push(name.as_str());
                } else {
                    required.push(name.as_str());
                }
            }
            let _ = writeln!(
                out,
                "{subject} is a record: {} required field{}, {} optional.",
                required.len(),
                if required.len() == 1 { "" } else { "s" },
                optional.len(),
            );
            if !required.is_empty() {
                let _ = writeln!(out, "  Required: {}.", join_truncated(&required, 8));
            }
            if !optional.is_empty() {
                let _ = writeln!(out, "  Optional: {}.", join_truncated(&optional, 8));
            }
        }
        ObjectDecision::Map { reason } => {
            let _ = writeln!(
                out,
                "{subject} is a map ({reason}; mean {:.1} keys/doc, {} unique key{} captured).",
                mean_keys,
                obj.field_order.len(),
                if obj.field_order.len() == 1 { "" } else { "s" },
            );
            if let Some((fmt, _, pct)) = obj.key_stats.dominant_format() {
                if pct >= 90.0 {
                    if fmt == StringFormat::Other {
                        if let Some((sk, _, sk_pct)) = obj.key_stats.dominant_skeleton() {
                            if sk_pct >= 90.0 {
                                let _ = writeln!(
                                    out,
                                    "  Keys follow pattern `{sk}` ({sk_pct:.0}% of unrecognized strings; lengths {}..{}).",
                                    obj.key_stats.min_len, obj.key_stats.max_len,
                                );
                            }
                        }
                    } else {
                        let _ = writeln!(
                            out,
                            "  Keys are {:.0}% {} (lengths {}..{}).",
                            pct,
                            format_name(fmt),
                            obj.key_stats.min_len,
                            obj.key_stats.max_len,
                        );
                    }
                }
            }
            let map_value_node = &sa.arena[obj.map_value];
            if let Some(value_obj) = map_value_node.object.as_ref() {
                let nested = decide_object(value_obj);
                match nested {
                    ObjectDecision::Record { .. } => {
                        let _ = writeln!(
                            out,
                            "  Map values are records with {} field{} (see report below for the field list).",
                            value_obj.field_order.len(),
                            if value_obj.field_order.len() == 1 { "" } else { "s" },
                        );
                    }
                    ObjectDecision::Map { .. } => {
                        let _ = writeln!(out, "  Map values are themselves maps.");
                    }
                }
            } else if !map_value_node.scalar_arms.is_empty() {
                let _ = writeln!(
                    out,
                    "  Map values are {} (scalar).",
                    scalar_arms_summary(map_value_node),
                );
            } else if map_value_node.array.is_some() {
                let _ = writeln!(out, "  Map values are arrays.");
            }
        }
    }
}

fn summarize_array(out: &mut String, arr: &ArrayAcc, _sa: &StreamingAnalyzer, subject: &str) {
    use std::fmt::Write as _;
    let decision = decide_array(arr);
    let mean_len = arr.length_sum as f64 / arr.obs.max(1) as f64;
    let min = if arr.min_length == u32::MAX { 0 } else { arr.min_length };
    match decision {
        ArrayDecision::Tuple { mode_len, mode_share } => {
            let _ = writeln!(
                out,
                "{subject} is a {}-tuple (length always {}, {:.0}% of observations).",
                mode_len,
                mode_len,
                100.0 * mode_share,
            );
        }
        ArrayDecision::Bag { reason } => {
            let _ = writeln!(
                out,
                "{subject} is an array bag of {min}..{} elements, mean {mean_len:.1} ({reason}).",
                arr.max_length,
            );
            // Cluster summary
            let entries = arr.element_cluster.entries();
            let entries: Vec<_> = entries
                .into_iter()
                .filter(|(s, _)| !matches!(s, Sig::Empty))
                .collect();
            if !entries.is_empty() {
                let total: u64 = entries.iter().map(|(_, c)| *c).sum();
                if entries.len() == 1 {
                    let mut sig_str = String::new();
                    write_sig(&mut sig_str, &entries[0].0);
                    let _ = writeln!(
                        out,
                        "  Element shape is uniform: {} ({} observations).",
                        sig_str, entries[0].1,
                    );
                } else {
                    let top = &entries[0];
                    let top_pct = 100.0 * (top.1 as f64) / (total.max(1) as f64);
                    let mut top_sig = String::new();
                    write_sig(&mut top_sig, &top.0);
                    let _ = writeln!(
                        out,
                        "  Element clusters into {} distinct shape{} ({} evictions); top arm {} covers {:.1}%.",
                        entries.len(),
                        if entries.len() == 1 { "" } else { "s" },
                        arr.element_cluster.evictions,
                        top_sig,
                        top_pct,
                    );
                }
            }
        }
    }
}

fn join_truncated(items: &[&str], limit: usize) -> String {
    if items.len() <= limit {
        return items.join(", ");
    }
    let kept = &items[..limit];
    format!("{}, ... (+{} more)", kept.join(", "), items.len() - limit)
}

fn write_string_stats(out: &mut String, ss: &StringStats, indent: &str, label: &str) {
    use std::fmt::Write as _;
    let _ = writeln!(
        out,
        "{indent}  {label} stats: {} obs, lengths {}..{} (mean {:.1})",
        ss.count, ss.min_len, ss.max_len, ss.mean_len(),
    );
    if ss.count > 0 {
        if let Some(line) = format_percentiles(&ss.length_sketch, true) {
            let _ = writeln!(out, "{indent}    length {line}");
        }
        let mut fmts: Vec<(StringFormat, u64)> =
            ss.formats.iter().map(|(k, v)| (*k, *v)).collect();
        fmts.sort_by_key(|(_, c)| std::cmp::Reverse(*c));
        let mut rendered: Vec<String> = Vec::new();
        for (f, c) in &fmts {
            let pct = 100.0 * (*c as f64) / (ss.count as f64);
            rendered.push(format!("{} ×{} ({:.1}%)", format_name(*f), c, pct));
        }
        let _ = writeln!(out, "{indent}    formats: {}", rendered.join(", "));
        // Non-empty length buckets only.
        let mut bucket_lines: Vec<String> = Vec::new();
        for (i, c) in ss.length_buckets.iter().enumerate() {
            if *c == 0 {
                continue;
            }
            let lo = if i == 0 { 0 } else { 1u64 << i };
            let hi = 1u64 << (i + 1);
            bucket_lines.push(format!("[{lo}..{hi}) ×{c}"));
        }
        if !bucket_lines.is_empty() {
            let _ = writeln!(
                out,
                "{indent}    length buckets (log2): {}",
                bucket_lines.join("  "),
            );
        }
        if ss.other_count > 0 {
            let entries = ss.skeletons.entries();
            if !entries.is_empty() {
                let mut rendered: Vec<String> = Vec::new();
                for (sk, c) in entries.iter().take(5) {
                    let pct = 100.0 * (*c as f64) / (ss.other_count as f64);
                    rendered.push(format!("`{sk}` ×{c} ({pct:.1}%)"));
                }
                let suffix = if entries.len() > 5 {
                    format!(", +{} more", entries.len() - 5)
                } else {
                    String::new()
                };
                let _ = writeln!(
                    out,
                    "{indent}    skeletons (over {} Other strings): {}{suffix}",
                    ss.other_count,
                    rendered.join(", "),
                );
            }
        }
    }
}

fn write_numeric_stats(out: &mut String, ns: &NumericStats, indent: &str) {
    use std::fmt::Write as _;
    if ns.count == 0 {
        return;
    }
    let _ = writeln!(
        out,
        "{indent}  numeric values stats: {} obs, range [{}, {}], mean {:.3}",
        ns.count,
        format_num(ns.min, ns.all_integer_valued()),
        format_num(ns.max, ns.all_integer_valued()),
        ns.mean(),
    );
    let _ = writeln!(
        out,
        "{indent}    signs: {} negative, {} zero, {} positive",
        ns.negative, ns.zero, ns.positive,
    );
    let int_pct = 100.0 * ns.integer_valued as f64 / ns.count as f64;
    let _ = writeln!(
        out,
        "{indent}    integer-valued: {} / {} ({:.1}%)",
        ns.integer_valued, ns.count, int_pct,
    );
    if let Some(line) = format_percentiles(&ns.sketch, ns.all_integer_valued()) {
        let _ = writeln!(out, "{indent}    {line}");
    }
    if let Some(guess) = ns.epoch_guess() {
        let _ = writeln!(out, "{indent}    looks like: {guess}");
    }
}

/// Render a "p50/p90/p99" line for any sketch. Returns None when the
/// sketch is empty.
fn format_percentiles(d: &DistSketch, as_int: bool) -> Option<String> {
    let p50 = d.quantile(0.5)?;
    let p90 = d.quantile(0.9)?;
    let p99 = d.quantile(0.99)?;
    Some(format!(
        "percentiles: p50≈{}, p90≈{}, p99≈{}",
        format_num(p50, as_int),
        format_num(p90, as_int),
        format_num(p99, as_int),
    ))
}

fn format_num(v: f64, as_int: bool) -> String {
    if as_int && v.is_finite() {
        (v as i64).to_string()
    } else {
        format!("{v}")
    }
}

fn format_name(f: StringFormat) -> &'static str {
    match f {
        StringFormat::Uuid => "UUID",
        StringFormat::IsoTimestamp => "ISO-timestamp",
        StringFormat::IsoDate => "ISO-date",
        StringFormat::Ipv4 => "IPv4",
        StringFormat::Ipv6 => "IPv6",
        StringFormat::Email => "email",
        StringFormat::UrlHttp => "URL",
        StringFormat::AllDigits => "digits",
        StringFormat::AllAlpha => "alpha",
        StringFormat::AlphaNum => "alphanum",
        StringFormat::Other => "other",
    }
}

fn scalar_name(k: ScalarKind) -> &'static str {
    match k {
        ScalarKind::Null => "null",
        ScalarKind::Bool => "bool",
        ScalarKind::I64 => "i64",
        ScalarKind::U64 => "u64",
        ScalarKind::F64 => "f64",
        ScalarKind::String => "string",
    }
}

impl Default for StreamingAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonEventSink for StreamingAnalyzer {
    fn document_begin(&mut self, doc_ordinal: u64) {
        self.current_doc = doc_ordinal;
        self.doc_count += 1;
        debug_assert!(self.stack.is_empty());
        debug_assert!(self.pending.is_none());
        self.pending = Some(self.root);
    }

    fn document_end(&mut self) {
        debug_assert!(self.stack.is_empty(), "document ended with open containers");
        self.pending = None;
    }

    fn null(&mut self) { self.observe_scalar(ScalarKind::Null); }
    fn bool(&mut self, _v: bool) { self.observe_scalar(ScalarKind::Bool); }
    fn i64(&mut self, v: i64) { self.observe_number(ScalarKind::I64, v as f64); }
    fn u64(&mut self, v: u64) { self.observe_number(ScalarKind::U64, v as f64); }
    fn f64(&mut self, v: f64) { self.observe_number(ScalarKind::F64, v); }
    fn string(&mut self, s: &str) { self.observe_string(s); }

    fn array_begin(&mut self) {
        let t = self.consume_target();
        self.obs_at(t);
        self.ensure_array(t);
        self.arena[t].array.as_mut().unwrap().obs += 1;
        self.stack.push(Frame::Array { node: t, position: 0, children: Vec::new() });
    }

    fn array_end(&mut self) {
        let (node, length, children) = match self.stack.pop().expect("array_end without array_begin") {
            Frame::Array { node, position, children } => (node, position, children),
            Frame::Object { .. } => panic!("array_end in object frame"),
        };
        let arr = self.arena[node].array.as_mut().unwrap();
        arr.length_sum += length as u64;
        if length < arr.min_length {
            arr.min_length = length;
        }
        if length > arr.max_length {
            arr.max_length = length;
        }
        *arr.length_histogram.entry(length).or_insert(0) += 1;
        arr.length_sketch.observe(length as f64);
        let sig = Sig::Array(Box::new(homogenize(children)));
        self.emit_child_sig(sig);
    }

    fn object_begin(&mut self) {
        let t = self.consume_target();
        self.obs_at(t);
        self.ensure_object(t);
        self.arena[t].object.as_mut().unwrap().obs += 1;
        self.stack.push(Frame::Object { node: t, pending_key: None, children: Vec::new() });
    }

    fn object_key(&mut self, key: &str) {
        let frame = self.stack.last_mut().expect("object_key without object_begin");
        let obj_node = match frame {
            Frame::Object { node, pending_key, .. } => {
                debug_assert!(pending_key.is_none(), "object_key before previous value");
                *pending_key = Some(key.to_string());
                *node
            }
            Frame::Array { .. } => panic!("object_key in array frame"),
        };
        let child = self.object_child_for_key(obj_node, key);
        {
            let obj = self.arena[obj_node].object.as_mut().unwrap();
            obj.key_count_sum += 1;
            obj.key_stats.observe(key);
        }
        self.pending = Some(child);
    }

    fn object_end(&mut self) {
        let (node, mut children) = match self.stack.pop().expect("object_end without object_begin") {
            Frame::Object { node, children, .. } => (node, children),
            Frame::Array { .. } => panic!("object_end in array frame"),
        };
        let key_count = children.len() as f64;
        self.arena[node]
            .object
            .as_mut()
            .unwrap()
            .key_count_sketch
            .observe(key_count);
        children.sort_by(|a, b| a.0.cmp(&b.0));
        let sig = Sig::Record(children);
        self.emit_child_sig(sig);
    }
}

impl Analyzer for StreamingAnalyzer {
    fn finish(self) -> ShapeNode {
        let StreamingAnalyzer { arena, root, .. } = self;
        Finalizer { arena }.build(root)
    }
}

// ---------------------------------------------------------------------------
// Finalization
// ---------------------------------------------------------------------------

struct Finalizer {
    arena: Vec<Node>,
}

impl Finalizer {
    fn build(&self, id: NodeId) -> ShapeNode {
        let n = &self.arena[id];
        let mut arms: Vec<ShapeNode> = Vec::new();

        for (kind, count) in &n.scalar_arms {
            if *count == 0 {
                continue;
            }
            arms.push(ShapeNode {
                kind: ShapeKind::Type(kind.to_value_type()),
                stats: stats_with_obs(*count, n),
            });
        }
        if let Some(obj) = &n.object {
            arms.push(self.build_object(obj, n));
        }
        if let Some(arr) = &n.array {
            arms.push(self.build_array(arr, n));
        }

        match arms.len() {
            0 => ShapeNode {
                kind: ShapeKind::Absent,
                stats: stats_from(n),
            },
            1 => arms.into_iter().next().unwrap(),
            _ => ShapeNode {
                kind: ShapeKind::Variant { arms },
                stats: stats_from(n),
            },
        }
    }

    fn build_object(&self, obj: &ObjectAcc, n: &Node) -> ShapeNode {
        if matches!(decide_object(obj), ObjectDecision::Map { .. }) {
            let mut value_shape = self.build(obj.map_value);
            if matches!(value_shape.kind, ShapeKind::Absent) {
                let ids: Vec<NodeId> = obj
                    .field_order
                    .iter()
                    .filter_map(|f| obj.fields.get(f).copied())
                    .collect();
                value_shape = self.dominant_child_shape(&ids);
            }
            let values_nullable = contains_null(&value_shape);
            // Prefer the rich Map form when the value carries shape
            // structure that ValueType can't express.
            if shape_is_value_type_only(&value_shape) {
                return ShapeNode {
                    kind: ShapeKind::Type(ValueType::Map {
                        key_type: Box::new(ValueType::String),
                        value_type: Box::new(shape_to_value_type(&value_shape)),
                        values_nullable,
                    }),
                    stats: stats_from(n),
                };
            }
            return ShapeNode {
                kind: ShapeKind::Map {
                    key: Box::new(ShapeNode {
                        kind: ShapeKind::Type(ValueType::String),
                        stats: Stats::default(),
                    }),
                    value: Box::new(value_shape),
                    values_nullable,
                },
                stats: stats_from(n),
            };
        }

        let fields_rich: Vec<ShapeField> = obj
            .field_order
            .iter()
            .map(|name| {
                let cid = obj.fields[name];
                let shape = self.build(cid);
                let child_obs = self.arena[cid].obs;
                let nullable = child_obs < obj.obs || contains_null(&shape);
                ShapeField { name: name.clone(), shape, nullable }
            })
            .collect();

        if fields_rich.iter().all(|f| shape_is_value_type_only(&f.shape)) {
            let fields: Vec<StructField> = fields_rich
                .into_iter()
                .map(|f| StructField {
                    name: f.name,
                    human_name: String::new(),
                    value_type: shape_to_value_type(&f.shape),
                    nullable: f.nullable,
                })
                .collect();
            return ShapeNode {
                kind: ShapeKind::Type(ValueType::Struct { fields }),
                stats: stats_from(n),
            };
        }
        ShapeNode {
            kind: ShapeKind::Record { fields: fields_rich },
            stats: stats_from(n),
        }
    }

    fn build_array(&self, arr: &ArrayAcc, n: &Node) -> ShapeNode {
        if let ArrayDecision::Tuple { mode_len, .. } = decide_array(arr) {
            let take = (mode_len as usize).min(arr.positional.len());
            let positions: Vec<ShapeNode> = arr
                .positional
                .iter()
                .take(take)
                .map(|cid| self.build(*cid))
                .collect();
            return ShapeNode {
                kind: ShapeKind::Tuple { positions },
                stats: stats_from(n),
            };
        }

        // Bag path. Build element shape from the cluster sketch when
        // available — the cluster carries per-element variant structure
        // that per-position children lose.
        let element_shape = self.build_array_element_shape(arr);
        let elements_nullable = contains_null(&element_shape);

        if shape_is_value_type_only(&element_shape) {
            return ShapeNode {
                kind: ShapeKind::Type(ValueType::Array {
                    element_type: Box::new(shape_to_value_type(&element_shape)),
                    elements_nullable,
                }),
                stats: stats_from(n),
            };
        }
        ShapeNode {
            kind: ShapeKind::Array { element: Box::new(element_shape), elements_nullable },
            stats: stats_from(n),
        }
    }

    fn build_array_element_shape(&self, arr: &ArrayAcc) -> ShapeNode {
        // Prefer the cluster sketch's view if it has signal. A single
        // dominant signature => one element shape. Multiple => Variant.
        let entries = arr.element_cluster.entries();
        // Discard the empty marker if it ever shows up.
        let entries: Vec<(Sig, u64)> = entries
            .into_iter()
            .filter(|(s, _)| !matches!(s, Sig::Empty))
            .collect();

        if !entries.is_empty() {
            let arms: Vec<ShapeNode> = entries
                .iter()
                .map(|(sig, count)| sig_to_shape(sig, *count))
                .collect();
            if arms.len() == 1 {
                return arms.into_iter().next().unwrap();
            }
            let total: u64 = entries.iter().map(|(_, c)| *c).sum();
            return ShapeNode {
                kind: ShapeKind::Variant { arms },
                stats: Stats {
                    observation_count: total,
                    first_doc_ordinal: 0,
                    last_doc_ordinal: 0,
                    exemplars: Vec::new(),
                },
            };
        }

        // Fallback: bag_value if present, else dominant positional child.
        let bag = self.build(arr.bag_value);
        if !matches!(bag.kind, ShapeKind::Absent) {
            return bag;
        }
        self.dominant_child_shape(&arr.positional)
    }

    fn dominant_child_shape(&self, ids: &[NodeId]) -> ShapeNode {
        if ids.is_empty() {
            return ShapeNode {
                kind: ShapeKind::Absent,
                stats: Stats::default(),
            };
        }
        let mut best = ids[0];
        let mut best_obs = self.arena[best].obs;
        for &id in &ids[1..] {
            if self.arena[id].obs > best_obs {
                best = id;
                best_obs = self.arena[id].obs;
            }
        }
        self.build(best)
    }
}

fn stats_from(n: &Node) -> Stats {
    Stats {
        observation_count: n.obs,
        first_doc_ordinal: n.first_doc,
        last_doc_ordinal: n.last_doc,
        exemplars: Vec::new(),
    }
}

fn stats_with_obs(count: u64, n: &Node) -> Stats {
    Stats {
        observation_count: count,
        first_doc_ordinal: n.first_doc,
        last_doc_ordinal: n.last_doc,
        exemplars: Vec::new(),
    }
}

fn stats_count(count: u64) -> Stats {
    Stats {
        observation_count: count,
        first_doc_ordinal: 0,
        last_doc_ordinal: 0,
        exemplars: Vec::new(),
    }
}

/// True if a ShapeNode can be losslessly represented as a ValueType.
/// Variants, Tuples, Absents, and any rich compound subtree forbid the
/// flat ValueType form.
fn shape_is_value_type_only(s: &ShapeNode) -> bool {
    match &s.kind {
        ShapeKind::Type(_) => true,
        ShapeKind::Variant { .. } | ShapeKind::Tuple { .. } | ShapeKind::Absent => false,
        ShapeKind::Array { element, .. } => shape_is_value_type_only(element),
        ShapeKind::Record { fields } => fields.iter().all(|f| shape_is_value_type_only(&f.shape)),
        ShapeKind::Map { key, value, .. } => {
            shape_is_value_type_only(key) && shape_is_value_type_only(value)
        }
    }
}

fn shape_to_value_type(shape: &ShapeNode) -> ValueType {
    match &shape.kind {
        ShapeKind::Type(vt) => vt.clone(),
        ShapeKind::Variant { arms } => arms
            .iter()
            .filter(|a| !matches!(a.kind, ShapeKind::Type(ValueType::Null) | ShapeKind::Absent))
            .max_by_key(|a| a.stats.observation_count)
            .map(shape_to_value_type)
            .unwrap_or(ValueType::Null),
        ShapeKind::Tuple { positions } => {
            let elem = positions
                .first()
                .map(shape_to_value_type)
                .unwrap_or(ValueType::Null);
            ValueType::Array { element_type: Box::new(elem), elements_nullable: false }
        }
        ShapeKind::Absent => ValueType::Null,
        ShapeKind::Array { element, elements_nullable } => ValueType::Array {
            element_type: Box::new(shape_to_value_type(element)),
            elements_nullable: *elements_nullable,
        },
        ShapeKind::Record { fields } => ValueType::Struct {
            fields: fields
                .iter()
                .map(|f| StructField {
                    name: f.name.clone(),
                    human_name: String::new(),
                    value_type: shape_to_value_type(&f.shape),
                    nullable: f.nullable,
                })
                .collect(),
        },
        ShapeKind::Map { key, value, values_nullable } => ValueType::Map {
            key_type: Box::new(shape_to_value_type(key)),
            value_type: Box::new(shape_to_value_type(value)),
            values_nullable: *values_nullable,
        },
    }
}

fn contains_null(shape: &ShapeNode) -> bool {
    match &shape.kind {
        ShapeKind::Type(ValueType::Null) => true,
        ShapeKind::Variant { arms } => arms.iter().any(contains_null),
        _ => false,
    }
}

/// Build a ShapeNode from a single subtree signature observed by the
/// cluster sketch. Stats carry the observed count for that cluster.
fn sig_to_shape(sig: &Sig, count: u64) -> ShapeNode {
    match sig {
        Sig::Empty => ShapeNode { kind: ShapeKind::Absent, stats: stats_count(count) },
        Sig::Scalar(k) => ShapeNode {
            kind: ShapeKind::Type(k.to_value_type()),
            stats: stats_count(count),
        },
        Sig::Array(elem) => {
            let element = sig_to_shape(elem, count);
            if shape_is_value_type_only(&element) {
                ShapeNode {
                    kind: ShapeKind::Type(ValueType::Array {
                        element_type: Box::new(shape_to_value_type(&element)),
                        elements_nullable: false,
                    }),
                    stats: stats_count(count),
                }
            } else {
                ShapeNode {
                    kind: ShapeKind::Array { element: Box::new(element), elements_nullable: false },
                    stats: stats_count(count),
                }
            }
        }
        Sig::Record(fields) => {
            let f_rich: Vec<ShapeField> = fields
                .iter()
                .map(|(name, s)| ShapeField {
                    name: name.clone(),
                    shape: sig_to_shape(s, count),
                    nullable: false,
                })
                .collect();
            if f_rich.iter().all(|f| shape_is_value_type_only(&f.shape)) {
                ShapeNode {
                    kind: ShapeKind::Type(ValueType::Struct {
                        fields: f_rich
                            .into_iter()
                            .map(|f| StructField {
                                name: f.name,
                                human_name: String::new(),
                                value_type: shape_to_value_type(&f.shape),
                                nullable: f.nullable,
                            })
                            .collect(),
                    }),
                    stats: stats_count(count),
                }
            } else {
                ShapeNode { kind: ShapeKind::Record { fields: f_rich }, stats: stats_count(count) }
            }
        }
        Sig::Variant(arms) => {
            let arm_shapes: Vec<ShapeNode> = arms.iter().map(|s| sig_to_shape(s, count)).collect();
            ShapeNode { kind: ShapeKind::Variant { arms: arm_shapes }, stats: stats_count(count) }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn analyze<F: FnOnce(&mut StreamingAnalyzer)>(f: F) -> ShapeNode {
        let mut a = StreamingAnalyzer::new();
        f(&mut a);
        a.finish()
    }

    #[test]
    fn scalar_root_i64() {
        let s = analyze(|a| {
            for i in 0..10 {
                a.document_begin(i);
                a.i64(i as i64);
                a.document_end();
            }
        });
        match s.kind {
            ShapeKind::Type(ValueType::I64) => (),
            other => panic!("expected I64, got {other:?}"),
        }
        assert_eq!(s.stats.observation_count, 10);
    }

    #[test]
    fn record_two_fields_all_required() {
        let s = analyze(|a| {
            for i in 0..5 {
                a.document_begin(i);
                a.object_begin();
                a.object_key("id");
                a.string("u");
                a.object_key("age");
                a.i64(1);
                a.object_end();
                a.document_end();
            }
        });
        match s.kind {
            ShapeKind::Type(ValueType::Struct { fields }) => {
                assert_eq!(fields.len(), 2);
                let id = fields.iter().find(|f| f.name == "id").unwrap();
                assert_eq!(id.value_type, ValueType::String);
                assert!(!id.nullable);
                let age = fields.iter().find(|f| f.name == "age").unwrap();
                assert_eq!(age.value_type, ValueType::I64);
                assert!(!age.nullable);
            }
            other => panic!("expected Struct, got {other:?}"),
        }
    }

    #[test]
    fn record_optional_field_marked_nullable() {
        let s = analyze(|a| {
            for i in 0..10 {
                a.document_begin(i);
                a.object_begin();
                a.object_key("id");
                a.string("u");
                if i % 2 == 0 {
                    a.object_key("nick");
                    a.string("n");
                }
                a.object_end();
                a.document_end();
            }
        });
        match s.kind {
            ShapeKind::Type(ValueType::Struct { fields }) => {
                let nick = fields.iter().find(|f| f.name == "nick").unwrap();
                assert!(nick.nullable);
                let id = fields.iter().find(|f| f.name == "id").unwrap();
                assert!(!id.nullable);
            }
            other => panic!("expected Struct, got {other:?}"),
        }
    }

    #[test]
    fn map_keys_overflow_to_map() {
        let s = analyze(|a| {
            let mut next = 0;
            for d in 0..50 {
                a.document_begin(d);
                a.object_begin();
                for _ in 0..4 {
                    let key = format!("k{}", next);
                    next += 1;
                    a.object_key(&key);
                    a.bool(true);
                }
                a.object_end();
                a.document_end();
            }
        });
        match s.kind {
            ShapeKind::Type(ValueType::Map { value_type, .. }) => {
                assert_eq!(*value_type, ValueType::Bool);
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[test]
    fn tuple_detected_when_length_tight() {
        let s = analyze(|a| {
            for i in 0..10 {
                a.document_begin(i);
                a.array_begin();
                a.f64(1.0);
                a.f64(2.0);
                a.f64(3.0);
                a.array_end();
                a.document_end();
            }
        });
        match s.kind {
            ShapeKind::Tuple { positions } => {
                assert_eq!(positions.len(), 3);
                for p in positions {
                    assert!(matches!(p.kind, ShapeKind::Type(ValueType::F64)));
                }
            }
            other => panic!("expected Tuple, got {other:?}"),
        }
    }

    #[test]
    fn array_with_varied_length_is_bag() {
        let s = analyze(|a| {
            for i in 0..20 {
                a.document_begin(i);
                a.array_begin();
                for _ in 0..(2 + i % 4) {
                    a.i64(1);
                }
                a.array_end();
                a.document_end();
            }
        });
        match s.kind {
            ShapeKind::Type(ValueType::Array { element_type, .. }) => {
                assert_eq!(*element_type, ValueType::I64);
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn variant_at_leaf_for_mixed_scalar_types() {
        let s = analyze(|a| {
            for i in 0..10 {
                a.document_begin(i);
                if i % 2 == 0 {
                    a.i64(1);
                } else {
                    a.string("x");
                }
                a.document_end();
            }
        });
        match s.kind {
            ShapeKind::Variant { arms } => {
                assert_eq!(arms.len(), 2);
                let has_i64 = arms.iter().any(|a| matches!(a.kind, ShapeKind::Type(ValueType::I64)));
                let has_str = arms.iter().any(|a| matches!(a.kind, ShapeKind::Type(ValueType::String)));
                assert!(has_i64 && has_str);
            }
            other => panic!("expected Variant, got {other:?}"),
        }
    }

    #[test]
    fn dist_sketch_quantile_within_relative_error() {
        let mut s = DistSketch::new(0.02, 256);
        for v in 1..=1000i32 {
            s.observe(v as f64);
        }
        let p50 = s.quantile(0.5).unwrap();
        let p90 = s.quantile(0.9).unwrap();
        let p99 = s.quantile(0.99).unwrap();
        // DDSketch ε=2% guarantees |estimate - true| / true <= 0.02 on
        // any quantile. Allow a small slack for the bucket-midpoint
        // representative.
        let tol = 0.04;
        let close = |est: f64, target: f64| (est - target).abs() / target <= tol;
        assert!(close(p50, 500.0), "p50 {p50} not within {tol} of 500");
        assert!(close(p90, 900.0), "p90 {p90} not within {tol} of 900");
        assert!(close(p99, 990.0), "p99 {p99} not within {tol} of 990");
    }

    #[test]
    fn dist_sketch_handles_signed_values_and_zero() {
        let mut s = DistSketch::new(0.02, 256);
        s.observe(-100.0);
        s.observe(0.0);
        s.observe(100.0);
        // Three values: median is 0.
        assert_eq!(s.quantile(0.5), Some(0.0));
        // 99th percentile sits in the positive bucket near 100.
        let p99 = s.quantile(0.99).unwrap();
        assert!((p99 - 100.0).abs() / 100.0 <= 0.04, "p99 {p99}");
        // 1st percentile near -100.
        let p01 = s.quantile(0.01).unwrap();
        assert!((p01 + 100.0).abs() / 100.0 <= 0.04, "p01 {p01}");
    }

    #[test]
    fn iso_timestamp_accepts_common_variants() {
        // Strict RFC3339 (existing case).
        assert!(is_iso_timestamp("2024-01-15T10:30:00Z"));
        // Space-separator variant.
        assert!(is_iso_timestamp("2024-01-15 10:30:00"));
        // No timezone at all.
        assert!(is_iso_timestamp("2024-01-15T10:30:00"));
        // Fractional seconds.
        assert!(is_iso_timestamp("2024-01-15T10:30:00.123Z"));
        assert!(is_iso_timestamp("2024-01-15T10:30:00.123456789-08:00"));
        // ±HH:MM offset.
        assert!(is_iso_timestamp("2024-01-15T10:30:00+05:30"));
        assert!(is_iso_timestamp("2024-01-15T10:30:00-05:30"));
        // ±HHMM (no colon) offset.
        assert!(is_iso_timestamp("2024-01-15T10:30:00+0530"));

        // Rejects malformed cases.
        assert!(!is_iso_timestamp("2024-01-15"));
        assert!(!is_iso_timestamp("2024-01-15T10:30"));
        assert!(!is_iso_timestamp("hello"));
        assert!(!is_iso_timestamp("2024-01-15T10:30:00."));
        assert!(!is_iso_timestamp("2024-01-15T10:30:00+5"));
    }

    #[test]
    fn numeric_epoch_guess_classifies_known_ranges() {
        let now_sec = 1_705_319_400_i64;
        // Epoch seconds.
        let mut a = StreamingAnalyzer::new();
        for i in 0..50i64 {
            a.document_begin(i as u64);
            a.i64(now_sec + i);
            a.document_end();
        }
        let summary = a.summary();
        assert!(
            summary.contains("epoch seconds"),
            "expected 'epoch seconds' in summary; got:\n{summary}",
        );

        // Epoch millis.
        let mut a = StreamingAnalyzer::new();
        for i in 0..50i64 {
            a.document_begin(i as u64);
            a.i64((now_sec as i64) * 1000 + i);
            a.document_end();
        }
        assert!(a.summary().contains("epoch millis"), "millis: {}", a.summary());

        // Numbers in a small range shouldn't trigger.
        let mut a = StreamingAnalyzer::new();
        for i in 0..50i64 {
            a.document_begin(i as u64);
            a.i64(i);
            a.document_end();
        }
        assert!(
            !a.summary().contains("epoch"),
            "small ints shouldn't be flagged as epoch; got:\n{}",
            a.summary(),
        );

        // Negative numbers shouldn't trigger (signed epoch is too wild a
        // claim to make without further evidence).
        let mut a = StreamingAnalyzer::new();
        for i in 0..50i64 {
            a.document_begin(i as u64);
            a.i64(-(now_sec + i));
            a.document_end();
        }
        assert!(
            !a.summary().contains("epoch"),
            "negative values shouldn't be flagged as epoch; got:\n{}",
            a.summary(),
        );
    }

    #[test]
    fn skeleton_collapses_alphanumeric_runs_keeps_punct() {
        assert_eq!(skeleton("ORD-2024-001234"), "A-9-9");
        assert_eq!(skeleton("hello world"), "A A");
        assert_eq!(skeleton("Hi!"), "A!");
        assert_eq!(skeleton("foo@bar.com"), "A@A.A");
        assert_eq!(skeleton(""), "");
        assert_eq!(skeleton("..."), "...");
        // Very-many-punctuation strings get truncated with an ellipsis.
        assert!(skeleton("a-b-c-d-e-f-g-h-i-j-k-l-m").ends_with('…'));
    }

    #[test]
    fn numeric_stats_track_range_and_integer_valuedness() {
        let mut a = StreamingAnalyzer::new();
        for i in 1..=100i64 {
            a.document_begin(i as u64);
            a.i64(i);
            a.document_end();
        }
        let summary = a.summary();
        assert!(
            summary.contains("positive integers in [1, 100]"),
            "expected range hint in summary; got:\n{summary}",
        );
        let report = a.report();
        assert!(
            report.contains("numeric values stats: 100 obs, range [1, 100]"),
            "expected numeric stats line; got:\n{report}",
        );
        assert!(
            report.contains("integer-valued: 100 / 100 (100.0%)"),
            "expected integer-valued line; got:\n{report}",
        );
    }

    #[test]
    fn numeric_stats_detect_mixed_sign() {
        let mut a = StreamingAnalyzer::new();
        for i in -5..=5i64 {
            a.document_begin((i + 10) as u64);
            a.i64(i);
            a.document_end();
        }
        let summary = a.summary();
        assert!(
            summary.contains("mixed-sign integers in [-5, 5]"),
            "expected mixed-sign hint; got:\n{summary}",
        );
    }

    #[test]
    fn numeric_stats_detect_non_integer_floats() {
        let mut a = StreamingAnalyzer::new();
        for i in 0..10 {
            a.document_begin(i);
            a.f64(0.1 * (i as f64 + 1.0));
            a.document_end();
        }
        let summary = a.summary();
        assert!(
            summary.contains("positive numbers in"),
            "expected positive non-integer hint; got:\n{summary}",
        );
        assert!(
            !summary.contains("integers"),
            "should not call non-integer floats 'integers'; got:\n{summary}",
        );
    }

    #[test]
    fn string_stats_skeleton_dominates_for_structured_ids() {
        let mut a = StreamingAnalyzer::new();
        for i in 0..1000u64 {
            a.document_begin(i);
            a.string(&format!("ORD-2024-{:06}", i));
            a.document_end();
        }
        let summary = a.summary();
        assert!(
            summary.contains("`A-9-9`"),
            "expected skeleton in summary; got:\n{summary}",
        );
        let report = a.report();
        assert!(
            report.contains("skeletons (over"),
            "expected skeletons line in report; got:\n{report}",
        );
    }

    #[test]
    fn polymorphic_array_emits_variant_element() {
        let s = analyze(|a| {
            for i in 0..30 {
                a.document_begin(i);
                a.array_begin();
                // Varied length so the tuple heuristic doesn't latch on.
                let n = 2 + (i as u64 % 4);
                for j in 0..n {
                    a.object_begin();
                    a.object_key("type");
                    match (i + j) % 3 {
                        0 => {
                            a.string("post");
                            a.object_key("post_id");
                            a.string("uuid");
                        }
                        1 => {
                            a.string("like");
                            a.object_key("like_id");
                            a.string("uuid");
                        }
                        _ => {
                            a.string("follow");
                            a.object_key("follow_id");
                            a.string("uuid");
                        }
                    }
                    a.object_end();
                }
                a.array_end();
                a.document_end();
            }
        });
        let element = match &s.kind {
            ShapeKind::Array { element, .. } => element.as_ref(),
            other => panic!("expected rich Array, got {other:?}"),
        };
        let arms = match &element.kind {
            ShapeKind::Variant { arms } => arms,
            other => panic!("expected Variant at element, got {other:?}"),
        };
        assert_eq!(arms.len(), 3, "three variant arms expected");
    }
}
