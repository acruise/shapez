//! Generic batch / offline driver for the analyzer.
//!
//! Streaming is the default mode: a long-lived `StreamingAnalyzer`
//! takes events one at a time through the `JsonEventSink` interface
//! and never reaches "the end." This module is for the *other* mode —
//! a bounded source (file directory, Kafka offset range, SQL query,
//! time-windowed object set) drives docs into a fresh analyzer that
//! runs to completion and returns an `AnalysisOutcome`.
//!
//! `analyze(source, policy) -> AnalysisOutcome` is the single entry
//! point. Use cases: first-time analysis of an existing dataset,
//! operator-triggered re-analysis with adjusted policy (often called
//! *retraining*), ad-hoc investigations. The same machinery serves all
//! three; the only differences are the source you pass and what's in
//! `policy.reason`.
//!
//! Concrete `DocumentSource` implementations live in adapter crates so
//! the core stays free of backend dependencies (serde_json, tokio,
//! arrow, sqlx, rdkafka). The trait here is JSON-agnostic — sources
//! decode whatever format their backend speaks and feed the analyzer
//! through `JsonEventSink` events.

use std::time::SystemTime;

use crate::node::ShapeNode;
use crate::{Analyzer, AnalyzerPolicy, StreamingAnalyzer};

// ---------------------------------------------------------------------------
// DocumentSource trait
// ---------------------------------------------------------------------------

/// Pluggable source of documents for an analyzer run. Each call to
/// `drive` walks the source's documents in order, feeding each one
/// (with a monotonically increasing doc ordinal) into the supplied
/// analyzer. Returns the number of documents successfully driven into
/// the sink.
///
/// Implementations are expected to poll `sink.should_bail()` after
/// every document and exit the loop cleanly when it returns `Some`.
pub trait DocumentSource {
    fn drive(self, sink: &mut StreamingAnalyzer) -> Result<u64, BatchError>;

    /// Human-readable description carried into the audit trail.
    /// Conventionally something like `"jsonl:/var/log/events/"` or
    /// `"kafka:events@offsets=12345..23456"`.
    fn describe(&self) -> String;
}

#[derive(Debug)]
pub enum BatchError {
    Io(std::io::Error),
    /// Any source-specific error (parse failure, network, decode,
    /// schema mismatch, …) wrapped for transport. Sources box their
    /// own error types into this variant rather than each one being
    /// known to the core crate.
    Source(Box<dyn std::error::Error + Send + Sync + 'static>),
    Other(String),
}

impl std::fmt::Display for BatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BatchError::Io(e) => write!(f, "io: {e}"),
            BatchError::Source(e) => write!(f, "source: {e}"),
            BatchError::Other(s) => write!(f, "{s}"),
        }
    }
}
impl std::error::Error for BatchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BatchError::Io(e) => Some(e),
            BatchError::Source(e) => Some(e.as_ref()),
            BatchError::Other(_) => None,
        }
    }
}
impl From<std::io::Error> for BatchError {
    fn from(e: std::io::Error) -> Self {
        BatchError::Io(e)
    }
}

// ---------------------------------------------------------------------------
// TimeInterpreter — how to lift a raw extracted value into a SystemTime
// ---------------------------------------------------------------------------

/// Strategy for interpreting an extracted raw value as a `SystemTime`.
/// The *extraction* (where the raw value comes from) is per-source —
/// JSON path, broker header, native typed column, regex on raw bytes —
/// but the *interpretation* (how the raw value becomes a timestamp) is
/// data-shaped and reusable across backends.
///
/// New variants are additive; consumers don't pattern-match exhaustively
/// in stable code so renaming/removing requires a schema bump.
#[derive(Clone, Debug)]
pub enum TimeInterpreter {
    EpochSeconds,
    EpochMillis,
    EpochMicros,
    EpochNanos,
    /// Permissive string-timestamp parser backed by the `dateparser`
    /// crate. Tries many formats — ISO 8601 / RFC 3339, RFC 2822, US-
    /// and EU-style date/time, common log formats, numeric epoch as
    /// string, and others. Use when the field's exact format isn't
    /// pinned by the source — which is most of the time. ISO 8601
    /// should be universal; reality has other plans.
    ///
    /// The attached `NaiveZonePolicy` governs what to do with strings
    /// that lack explicit zone information (no `Z`, no `±HH:MM`
    /// offset, no recognized tz abbreviation). Default is `Refuse`;
    /// the operator overrides per field as needed.
    StringTimestamp(NaiveZonePolicy),
    /// Extract the 48-bit epoch-millis timestamp embedded in the
    /// first 12 hex chars of a UUIDv7 (RFC 9562). The 13th hex char
    /// is validated as the version nibble (must be `7`). Accepts both
    /// hyphenated (`8-4-4-4-12`) and unhyphenated (32-char) forms,
    /// case-insensitive. Use when a field holds UUIDv7-shaped
    /// identifiers and you'd rather not carry a separate `created_at`
    /// column.
    Uuidv7,
    /// Extract the 48-bit epoch-millis timestamp from the first 10
    /// Crockford Base32 characters of a ULID. Strings must be exactly
    /// 26 characters; the alphabet excludes `I`, `L`, `O`, `U`
    /// (Crockford's contract). Case-insensitive. Same use case as
    /// `Uuidv7` for the ULID world.
    Ulid,
}

/// Per-field policy for interpreting timestamp strings that arrive
/// without explicit zone information. Each timestamp field can carry
/// its own policy — `ts` might want `AssumeUtc` while
/// `order_submitted_time` wants a fixed regional offset, in the same
/// table.
///
/// The right *default* (when an operator picks `StringTimestamp` and
/// doesn't say more) is `Refuse`: silently picking any zone for data
/// that didn't name one is a correctness hazard, and a loud failure
/// puts the conversation in front of the operator instead of behind
/// their back. The other variants are explicit overrides for
/// deployments that actually know the answer.
///
/// PLANNED VARIANTS (not yet implemented):
/// - `AssumeNamed(String)` for IANA zones like `"America/Los_Angeles"`
///   — requires pulling in `chrono-tz` (~250 KB of compiled tz data).
/// - `StickyPrevious { fallback }` for "use the offset of the most
///   recent zoned value seen" — stateful, falls back to `fallback`
///   before the first zoned value.
/// - `FromSiblingField { path, interpreter }` for the very common
///   pattern of a separate zone hint elsewhere in the same record:
///   `{"ts": "...", "tz": "America/Los_Angeles"}` or `{"ts": "...",
///   "utc_offset_min": -480}`. Mechanically requires widening
///   `interpret`'s contract so a policy can see the surrounding
///   record, not just the extracted raw value — a small structural
///   change worth doing once one of these stateful / context-aware
///   policies is genuinely needed.
/// This policy isn't shapez-specific — every timestamp parser
/// downstream faces the same question — so the eventual home is
/// `_meta` (alongside `ValueType::Timestamp`) and shapez just
/// references it from there once the shared crate is ready.
#[derive(Clone, Debug, Default)]
pub enum NaiveZonePolicy {
    /// Reject zoneless timestamps (`interpret` returns `None`).
    /// Safest default; the operator must consciously opt out.
    #[default]
    Refuse,
    /// Treat zoneless timestamps as UTC.
    AssumeUtc,
    /// Treat zoneless timestamps as having the given fixed offset,
    /// in seconds east of UTC. Use `i32` to allow both signs; common
    /// values are `-25200` for `-07:00`, `19800` for `+05:30`.
    AssumeFixedOffset { seconds: i32 },
    /// Use the local timezone of the process running shapez. The
    /// current dateparser default — almost always wrong for shapez
    /// purposes, but available for compatibility with pre-policy
    /// behavior and for deployments where the host genuinely owns
    /// the timezone authority (rare).
    Local,
}

/// Tagged raw value handed to `TimeInterpreter::interpret`. Source
/// adapters lift their own value-types (serde_json::Number, a SQL
/// integer column, a Kafka header byte slice) to one of these variants
/// based on what makes sense for the backend.
#[derive(Clone, Copy, Debug)]
pub enum TimeRaw<'a> {
    Int(i64),
    Str(&'a str),
}

impl TimeInterpreter {
    pub fn interpret(&self, raw: TimeRaw<'_>) -> Option<SystemTime> {
        use std::time::Duration;
        match (self, raw) {
            (TimeInterpreter::EpochSeconds, TimeRaw::Int(n)) if n >= 0 => {
                Some(SystemTime::UNIX_EPOCH + Duration::from_secs(n as u64))
            }
            (TimeInterpreter::EpochMillis, TimeRaw::Int(n)) if n >= 0 => {
                Some(SystemTime::UNIX_EPOCH + Duration::from_millis(n as u64))
            }
            (TimeInterpreter::EpochMicros, TimeRaw::Int(n)) if n >= 0 => {
                Some(SystemTime::UNIX_EPOCH + Duration::from_micros(n as u64))
            }
            (TimeInterpreter::EpochNanos, TimeRaw::Int(n)) if n >= 0 => {
                Some(SystemTime::UNIX_EPOCH + Duration::from_nanos(n as u64))
            }
            (TimeInterpreter::StringTimestamp(policy), TimeRaw::Str(s)) => {
                parse_timestamp_str(s, policy)
            }
            (TimeInterpreter::Uuidv7, TimeRaw::Str(s)) => parse_uuidv7_timestamp(s),
            (TimeInterpreter::Ulid, TimeRaw::Str(s)) => parse_ulid_timestamp(s),
            _ => None,
        }
    }

    pub fn describe(&self) -> &'static str {
        match self {
            TimeInterpreter::EpochSeconds => "epoch-seconds",
            TimeInterpreter::EpochMillis => "epoch-millis",
            TimeInterpreter::EpochMicros => "epoch-micros",
            TimeInterpreter::EpochNanos => "epoch-nanos",
            TimeInterpreter::StringTimestamp(_) => "string-timestamp",
            TimeInterpreter::Uuidv7 => "uuid-v7",
            TimeInterpreter::Ulid => "ulid",
        }
    }
}

/// Permissive string-timestamp parser. Tries many formats via
/// `dateparser`: ISO 8601 / RFC 3339, RFC 2822, US-style M/D/Y, EU-
/// style D/M/Y, common log formats, numeric epoch as string, more.
/// Strings that lack an explicit timezone marker are routed through
/// `NaiveZonePolicy` instead of silently picking the host's local zone.
///
/// PERFORMANCE TODO: this is the cold path. `dateparser::parse` walks
/// its full format ladder for every value, but within a single field
/// the format almost never varies — once the first N records have
/// established that ".ts is RFC 3339 with Z," we should switch to a
/// pinned `chrono::DateTime::parse_from_str` with that one format
/// string and pay roughly 1/N the cost per record afterward. Same
/// sample → infer → commit dance the analyzer does for everything
/// else. Flagged so the perf path is obvious when throughput becomes
/// the question.
fn parse_timestamp_str(s: &str, policy: &NaiveZonePolicy) -> Option<SystemTime> {
    if has_explicit_zone(s) {
        return dateparser_to_systemtime(s);
    }
    match policy {
        NaiveZonePolicy::Refuse => None,
        NaiveZonePolicy::AssumeUtc => parse_naive_with_offset(s, 0),
        NaiveZonePolicy::AssumeFixedOffset { seconds } => parse_naive_with_offset(s, *seconds),
        NaiveZonePolicy::Local => dateparser_to_systemtime(s),
    }
}

fn dateparser_to_systemtime(s: &str) -> Option<SystemTime> {
    use std::time::Duration;
    let dt = dateparser::parse(s).ok()?;
    let nanos = dt.timestamp_nanos_opt()?;
    if nanos < 0 {
        SystemTime::UNIX_EPOCH.checked_sub(Duration::from_nanos((-nanos) as u64))
    } else {
        Some(SystemTime::UNIX_EPOCH + Duration::from_nanos(nanos as u64))
    }
}

/// Parse a naive timestamp string and apply the given fixed UTC
/// offset (in seconds east of UTC). Tries a small set of common
/// formats; the `dateparser` ladder isn't safe here because it would
/// re-apply local-tz semantics behind our back.
fn parse_naive_with_offset(s: &str, offset_secs: i32) -> Option<SystemTime> {
    use chrono::{FixedOffset, NaiveDate, NaiveDateTime, TimeZone};
    use std::time::Duration;
    let trimmed = s.trim();
    let naive = NAIVE_FORMATS
        .iter()
        .find_map(|fmt| NaiveDateTime::parse_from_str(trimmed, fmt).ok())
        .or_else(|| {
            NaiveDate::parse_from_str(trimmed, "%Y-%m-%d")
                .ok()
                .and_then(|d| d.and_hms_opt(0, 0, 0))
        })?;
    let tz = FixedOffset::east_opt(offset_secs)?;
    let dt = tz.from_local_datetime(&naive).single()?;
    let nanos = dt.timestamp_nanos_opt()?;
    if nanos < 0 {
        SystemTime::UNIX_EPOCH.checked_sub(Duration::from_nanos((-nanos) as u64))
    } else {
        Some(SystemTime::UNIX_EPOCH + Duration::from_nanos(nanos as u64))
    }
}

const NAIVE_FORMATS: &[&str] = &[
    "%Y-%m-%dT%H:%M:%S%.f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S%.f",
    "%Y-%m-%d %H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
    "%Y/%m/%dT%H:%M:%S",
];

/// Extract the 48-bit epoch-millis timestamp embedded in the first
/// 12 hex characters of a UUIDv7. Validates the 13th hex char as the
/// version nibble (must be `7`). Tolerates both hyphenated (36-char)
/// and unhyphenated (32-char) forms; case-insensitive.
fn parse_uuidv7_timestamp(s: &str) -> Option<SystemTime> {
    use std::time::Duration;
    let t = s.trim();
    if t.len() != 36 && t.len() != 32 {
        return None;
    }
    if t.len() == 36 {
        let b = t.as_bytes();
        if b[8] != b'-' || b[13] != b'-' || b[18] != b'-' || b[23] != b'-' {
            return None;
        }
    }
    let mut millis: u64 = 0;
    let mut count: u32 = 0;
    for &b in t.as_bytes() {
        if b == b'-' {
            continue;
        }
        let v = hex_digit(b)?;
        if count < 12 {
            millis = (millis << 4) | v as u64;
        } else {
            // count == 12: the version nibble.
            if v != 7 {
                return None;
            }
            return Some(SystemTime::UNIX_EPOCH + Duration::from_millis(millis));
        }
        count += 1;
    }
    // Ran out of input before reaching the version nibble.
    None
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Extract the 48-bit epoch-millis timestamp from the first 10
/// Crockford Base32 characters of a 26-character ULID. Case-
/// insensitive; rejects strings containing `I`, `L`, `O`, `U`
/// (excluded from the Crockford alphabet).
fn parse_ulid_timestamp(s: &str) -> Option<SystemTime> {
    use std::time::Duration;
    let t = s.trim();
    if t.len() != 26 {
        return None;
    }
    let bytes = t.as_bytes();
    // Validate all 26 chars against the Crockford alphabet first; a
    // bogus tail shouldn't be silently dropped just because the first
    // 10 chars parsed.
    for &b in bytes {
        if crockford_base32(b).is_none() {
            return None;
        }
    }
    let mut millis: u64 = 0;
    for &b in &bytes[..10] {
        let v = crockford_base32(b)?;
        millis = (millis << 5) | v as u64;
    }
    // 10 base32 chars hold 50 bits; the high 2 bits must be 0 for a
    // valid 48-bit millis value. Strict validation rejects malformed
    // prefixes.
    if millis >> 48 != 0 {
        return None;
    }
    Some(SystemTime::UNIX_EPOCH + Duration::from_millis(millis))
}

fn crockford_base32(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'A' | b'a' => Some(10),
        b'B' | b'b' => Some(11),
        b'C' | b'c' => Some(12),
        b'D' | b'd' => Some(13),
        b'E' | b'e' => Some(14),
        b'F' | b'f' => Some(15),
        b'G' | b'g' => Some(16),
        b'H' | b'h' => Some(17),
        b'J' | b'j' => Some(18),
        b'K' | b'k' => Some(19),
        b'M' | b'm' => Some(20),
        b'N' | b'n' => Some(21),
        b'P' | b'p' => Some(22),
        b'Q' | b'q' => Some(23),
        b'R' | b'r' => Some(24),
        b'S' | b's' => Some(25),
        b'T' | b't' => Some(26),
        b'V' | b'v' => Some(27),
        b'W' | b'w' => Some(28),
        b'X' | b'x' => Some(29),
        b'Y' | b'y' => Some(30),
        b'Z' | b'z' => Some(31),
        // I, L, O, U deliberately omitted (Crockford's contract).
        _ => None,
    }
}

/// True if `s` ends with a recognizable timezone marker: `Z`, an
/// offset like `±HH:MM` or `±HHMM`, or a common tz abbreviation at a
/// word boundary. Also true for all-digit numeric strings (which we
/// treat as epoch values, implicitly UTC). Conservative — false
/// negatives are fine since they just route through `NaiveZonePolicy`.
fn has_explicit_zone(s: &str) -> bool {
    let t = s.trim();
    if t.is_empty() {
        return false;
    }
    let bytes = t.as_bytes();
    let len = bytes.len();

    // All-digit (or digit + single leading sign) — epoch numeric.
    let digit_count = bytes.iter().filter(|b| b.is_ascii_digit()).count();
    let nondigit_count = bytes
        .iter()
        .filter(|b| !b.is_ascii_digit() && !b.is_ascii_whitespace())
        .count();
    if nondigit_count == 0 && digit_count >= 5 {
        return true;
    }

    if matches!(bytes[len - 1], b'Z' | b'z') {
        return true;
    }

    // ±HH:MM at end.
    if len >= 6 {
        let i = len - 6;
        if (bytes[i] == b'+' || bytes[i] == b'-')
            && bytes[i + 1].is_ascii_digit()
            && bytes[i + 2].is_ascii_digit()
            && bytes[i + 3] == b':'
            && bytes[i + 4].is_ascii_digit()
            && bytes[i + 5].is_ascii_digit()
        {
            return true;
        }
    }
    // ±HHMM at end.
    if len >= 5 {
        let i = len - 5;
        if (bytes[i] == b'+' || bytes[i] == b'-')
            && bytes[i + 1].is_ascii_digit()
            && bytes[i + 2].is_ascii_digit()
            && bytes[i + 3].is_ascii_digit()
            && bytes[i + 4].is_ascii_digit()
        {
            return true;
        }
    }

    // Common tz abbreviations at end (word-boundary checked).
    let upper = t.to_ascii_uppercase();
    for tz in &[
        "GMT", "UTC", "EST", "EDT", "PST", "PDT", "CST", "CDT", "MST", "MDT", "BST", "JST", "IST",
        "CET", "CEST",
    ] {
        if upper.ends_with(tz) {
            let prefix_len = upper.len() - tz.len();
            if prefix_len == 0 {
                continue; // whole string is the abbreviation
            }
            let last = upper.as_bytes()[prefix_len - 1];
            if !last.is_ascii_alphabetic() {
                return true;
            }
        }
    }

    false
}

// ---------------------------------------------------------------------------
// TimePredicate
// ---------------------------------------------------------------------------

/// Time predicate applied during a batch run. Sources interpret it
/// however makes sense for their backend:
///   - file-based sources (`JsonlDir`): file modification time.
///   - object_store / cloud: object last-modified header.
///   - Parquet / ORC: file modtime plus, if the schema includes one, a
///     min/max column predicate.
///   - Kafka: offset-range bounds derived from broker timestamps.
///   - SQL: synthesized WHERE clause on a designated time column.
///
/// `start` is inclusive, `end` is exclusive. Either bound may be None
/// to indicate "no lower / upper limit."
#[derive(Clone, Debug, Default)]
pub struct TimePredicate {
    pub start: Option<SystemTime>,
    pub end: Option<SystemTime>,
}

impl TimePredicate {
    pub fn contains(&self, t: SystemTime) -> bool {
        if let Some(s) = self.start {
            if t < s {
                return false;
            }
        }
        if let Some(e) = self.end {
            if t >= e {
                return false;
            }
        }
        true
    }

    pub fn describe(&self) -> String {
        match (self.start, self.end) {
            (None, None) => "no time filter".into(),
            (Some(s), None) => format!("start={s:?}"),
            (None, Some(e)) => format!("end={e:?}"),
            (Some(s), Some(e)) => format!("start={s:?} end={e:?}"),
        }
    }
}

#[cfg(test)]
mod time_tests {
    use super::*;
    use std::time::{Duration, SystemTime};

    fn t(secs: u64) -> SystemTime {
        SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
    }

    fn refuse() -> NaiveZonePolicy { NaiveZonePolicy::Refuse }
    fn utc() -> NaiveZonePolicy { NaiveZonePolicy::AssumeUtc }
    fn fixed(secs: i32) -> NaiveZonePolicy {
        NaiveZonePolicy::AssumeFixedOffset { seconds: secs }
    }
    fn local() -> NaiveZonePolicy { NaiveZonePolicy::Local }

    #[test]
    fn rfc3339_z_is_zoned_regardless_of_policy() {
        let parsed = parse_timestamp_str("2024-01-15T10:30:00Z", &refuse()).unwrap();
        assert_eq!(parsed, t(1_705_314_600));
        // The Z makes the zone explicit, so even Refuse accepts it.
    }

    #[test]
    fn offset_normalizes_to_utc_under_any_policy() {
        let with_offset = parse_timestamp_str("2024-01-15T15:30:00+05:00", &refuse()).unwrap();
        let utc = parse_timestamp_str("2024-01-15T10:30:00Z", &refuse()).unwrap();
        assert_eq!(with_offset, utc);
    }

    #[test]
    fn rfc2822_with_gmt_is_zoned() {
        let parsed = parse_timestamp_str("Mon, 15 Jan 2024 10:30:00 GMT", &refuse()).unwrap();
        assert_eq!(parsed, t(1_705_314_600));
    }

    #[test]
    fn numeric_epoch_as_string_is_implicitly_zoned() {
        let parsed = parse_timestamp_str("1705314600", &refuse()).unwrap();
        assert_eq!(parsed, t(1_705_314_600));
    }

    #[test]
    fn fractional_seconds_round_trip() {
        let plain = parse_timestamp_str("2024-01-15T10:30:00Z", &refuse()).unwrap();
        let frac = parse_timestamp_str("2024-01-15T10:30:00.500Z", &refuse()).unwrap();
        assert_eq!(frac, plain + Duration::from_millis(500));
    }

    #[test]
    fn rejects_obvious_garbage() {
        for p in [refuse(), utc(), local()] {
            assert!(parse_timestamp_str("nope", &p).is_none());
            assert!(parse_timestamp_str("", &p).is_none());
            assert!(parse_timestamp_str("not even close to a date", &p).is_none());
        }
    }

    // ---- Naive timestamps + policy ----

    #[test]
    fn refuse_rejects_naive_timestamps() {
        assert!(parse_timestamp_str("2024-01-15 10:30:00", &refuse()).is_none());
        assert!(parse_timestamp_str("2024-01-15T10:30:00", &refuse()).is_none());
    }

    #[test]
    fn assume_utc_parses_naive_as_utc() {
        let parsed = parse_timestamp_str("2024-01-15 10:30:00", &utc()).unwrap();
        assert_eq!(parsed, t(1_705_314_600));
        let t_form = parse_timestamp_str("2024-01-15T10:30:00", &utc()).unwrap();
        assert_eq!(t_form, t(1_705_314_600));
    }

    #[test]
    fn assume_fixed_offset_applies_offset() {
        // +05:00 means clock reads 15:30 when UTC is 10:30.
        let parsed = parse_timestamp_str("2024-01-15T15:30:00", &fixed(5 * 3600)).unwrap();
        let utc_ref = parse_timestamp_str("2024-01-15T10:30:00Z", &refuse()).unwrap();
        assert_eq!(parsed, utc_ref);

        // -07:00 means clock reads 03:30 when UTC is 10:30.
        let pst = parse_timestamp_str("2024-01-15T03:30:00", &fixed(-7 * 3600)).unwrap();
        assert_eq!(pst, utc_ref);
    }

    #[test]
    fn local_policy_falls_back_to_dateparser() {
        // Just verify it parses to some SystemTime. The exact value is
        // host-tz-dependent on purpose.
        assert!(parse_timestamp_str("2024-01-15 10:30:00", &local()).is_some());
    }

    #[test]
    fn epoch_seconds_interpret() {
        let ti = TimeInterpreter::EpochSeconds;
        assert_eq!(ti.interpret(TimeRaw::Int(1_705_314_600)).unwrap(), t(1_705_314_600));
        assert!(ti.interpret(TimeRaw::Int(-1)).is_none());
        assert!(ti.interpret(TimeRaw::Str("nope")).is_none());
    }

    #[test]
    fn epoch_millis_interpret() {
        let ti = TimeInterpreter::EpochMillis;
        let parsed = ti.interpret(TimeRaw::Int(1_705_314_600_123)).unwrap();
        assert_eq!(parsed, SystemTime::UNIX_EPOCH + Duration::from_millis(1_705_314_600_123));
    }

    #[test]
    fn string_timestamp_dispatch_only_takes_strings() {
        let ti = TimeInterpreter::StringTimestamp(NaiveZonePolicy::AssumeUtc);
        assert!(ti.interpret(TimeRaw::Str("2024-01-15T10:30:00Z")).is_some());
        assert!(ti.interpret(TimeRaw::Int(1)).is_none());
    }

    #[test]
    fn refuse_is_the_default_policy() {
        assert!(matches!(NaiveZonePolicy::default(), NaiveZonePolicy::Refuse));
    }

    #[test]
    fn has_explicit_zone_recognizes_canonical_forms() {
        assert!(has_explicit_zone("2024-01-15T10:30:00Z"));
        assert!(has_explicit_zone("2024-01-15T10:30:00+05:00"));
        assert!(has_explicit_zone("2024-01-15T10:30:00-0700"));
        assert!(has_explicit_zone("Mon, 15 Jan 2024 10:30:00 GMT"));
        assert!(has_explicit_zone("1705314600")); // numeric epoch
    }

    #[test]
    fn has_explicit_zone_rejects_naive_forms() {
        assert!(!has_explicit_zone("2024-01-15T10:30:00"));
        assert!(!has_explicit_zone("2024-01-15 10:30:00"));
        assert!(!has_explicit_zone("Jan 15 2024 10:30:00"));
    }

    // ---- UUIDv7 / ULID ----

    fn synthetic_uuidv7(millis: u64) -> String {
        // millis fits in 48 bits; first 8 hex chars = high 32 bits,
        // next 4 = next 16 bits. Padding the rest with deterministic
        // bytes so the version (`7`) and variant (`8`) nibbles are
        // correct.
        let hi32 = (millis >> 16) as u32;
        let lo16 = (millis & 0xFFFF) as u16;
        format!("{:08x}-{:04x}-7000-8000-000000000000", hi32, lo16)
    }

    #[test]
    fn uuidv7_round_trip() {
        let millis = 1_700_000_000_000_u64;
        let uuid = synthetic_uuidv7(millis);
        let parsed = parse_uuidv7_timestamp(&uuid).unwrap();
        assert_eq!(parsed, SystemTime::UNIX_EPOCH + Duration::from_millis(millis));
    }

    #[test]
    fn uuidv7_accepts_unhyphenated_form() {
        let uuid = synthetic_uuidv7(1_705_314_600_000);
        let stripped: String = uuid.chars().filter(|c| *c != '-').collect();
        let parsed = parse_uuidv7_timestamp(&stripped).unwrap();
        assert_eq!(parsed, SystemTime::UNIX_EPOCH + Duration::from_millis(1_705_314_600_000));
    }

    #[test]
    fn uuidv7_accepts_uppercase() {
        let uuid = synthetic_uuidv7(1_705_314_600_000).to_ascii_uppercase();
        let parsed = parse_uuidv7_timestamp(&uuid).unwrap();
        assert_eq!(parsed, SystemTime::UNIX_EPOCH + Duration::from_millis(1_705_314_600_000));
    }

    #[test]
    fn uuidv7_rejects_other_versions() {
        // UUIDv4 — random — should be rejected because version nibble is 4.
        let v4 = "f47ac10b-58cc-4372-a567-0e02b2c3d479";
        assert!(parse_uuidv7_timestamp(v4).is_none());
        // UUIDv1, v3, v5 — all rejected.
        let v1 = "550e8400-e29b-11d4-a716-446655440000";
        assert!(parse_uuidv7_timestamp(v1).is_none());
    }

    #[test]
    fn uuidv7_rejects_malformed() {
        assert!(parse_uuidv7_timestamp("nope").is_none());
        assert!(parse_uuidv7_timestamp("").is_none());
        // Wrong length.
        assert!(parse_uuidv7_timestamp("018bba90-8800-7000").is_none());
        // Missing hyphen.
        assert!(parse_uuidv7_timestamp("018bba908800-7000-8000-000000000000-").is_none());
    }

    #[test]
    fn uuidv7_via_interpreter() {
        let uuid = synthetic_uuidv7(1_700_000_000_000);
        let ti = TimeInterpreter::Uuidv7;
        let parsed = ti.interpret(TimeRaw::Str(&uuid)).unwrap();
        assert_eq!(parsed, SystemTime::UNIX_EPOCH + Duration::from_millis(1_700_000_000_000));
        // Should reject Int inputs.
        assert!(ti.interpret(TimeRaw::Int(1)).is_none());
    }

    fn synthetic_ulid(millis: u64) -> String {
        // Encode 48-bit millis as 10 Crockford base32 chars; pad
        // randomness section with zeros.
        const ALPHA: &[u8] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
        let mut chars = [0u8; 10];
        let mut m = millis;
        for slot in (0..10).rev() {
            chars[slot] = ALPHA[(m & 0x1F) as usize];
            m >>= 5;
        }
        let head = std::str::from_utf8(&chars).unwrap().to_string();
        format!("{head}{}", "0".repeat(16))
    }

    #[test]
    fn ulid_round_trip() {
        let millis = 1_700_000_000_000_u64;
        let ulid = synthetic_ulid(millis);
        let parsed = parse_ulid_timestamp(&ulid).unwrap();
        assert_eq!(parsed, SystemTime::UNIX_EPOCH + Duration::from_millis(millis));
    }

    #[test]
    fn ulid_accepts_lowercase() {
        let ulid = synthetic_ulid(1_705_314_600_000).to_ascii_lowercase();
        let parsed = parse_ulid_timestamp(&ulid).unwrap();
        assert_eq!(parsed, SystemTime::UNIX_EPOCH + Duration::from_millis(1_705_314_600_000));
    }

    #[test]
    fn ulid_rejects_excluded_letters() {
        // Substitute an 'I' for a valid char — should fail.
        let mut ulid = synthetic_ulid(1_700_000_000_000);
        // Replace the last char (originally '0') with 'I' — invalid in Crockford.
        let pos = ulid.len() - 1;
        ulid.replace_range(pos..pos + 1, "I");
        assert!(parse_ulid_timestamp(&ulid).is_none());
        // Same for L, O, U.
        for bad in &['L', 'O', 'U'] {
            let mut bad_ulid = synthetic_ulid(1_700_000_000_000);
            bad_ulid.replace_range(pos..pos + 1, &bad.to_string());
            assert!(parse_ulid_timestamp(&bad_ulid).is_none(), "{bad}");
        }
    }

    #[test]
    fn ulid_rejects_wrong_length() {
        assert!(parse_ulid_timestamp("").is_none());
        assert!(parse_ulid_timestamp("01HE1JFRZZ").is_none()); // 10 chars
        assert!(parse_ulid_timestamp("01HE1JFRZZAA00000000000000ABC").is_none()); // 29
    }

    #[test]
    fn ulid_via_interpreter() {
        let ulid = synthetic_ulid(1_700_000_000_000);
        let ti = TimeInterpreter::Ulid;
        let parsed = ti.interpret(TimeRaw::Str(&ulid)).unwrap();
        assert_eq!(parsed, SystemTime::UNIX_EPOCH + Duration::from_millis(1_700_000_000_000));
        assert!(ti.interpret(TimeRaw::Int(1)).is_none());
    }
}

// ---------------------------------------------------------------------------
// AnalysisOutcome + analyze() entry point
// ---------------------------------------------------------------------------

/// Outcome of a batch analyzer run. Carries enough provenance that an
/// audit trail can reproduce the run: which source, which policy, why
/// we were doing this. The `policy.reason` field is conventionally
/// empty for routine streaming ingest and populated for batch runs
/// the operator wants to label (investigations, follow-up re-runs,
/// scheduled re-analyses) — but the type itself doesn't distinguish.
#[derive(Debug)]
pub struct AnalysisOutcome {
    pub doc_count: u64,
    pub shape: ShapeNode,
    pub summary: String,
    pub report: String,
    pub source: String,
    pub policy: AnalyzerPolicy,
    /// `Some(reason)` if the analyzer bailed out before the source was
    /// exhausted (chaos threshold tripped, max_docs reached, etc.).
    /// `None` for a normal completion.
    pub bailed: Option<String>,
}

/// Drive a source through a fresh `StreamingAnalyzer` configured with
/// the given policy. Returns the inferred shape, the summary, the full
/// report, and provenance enough to reproduce the call.
///
/// This is the general entry point for any batch / bounded-source run
/// — first-time analysis, operator-triggered follow-ups with adjusted
/// policy, ad-hoc investigations. The only difference between those
/// uses is what's in the `policy` (especially `policy.reason`) and
/// which source you pass.
pub fn analyze<S: DocumentSource>(
    source: S,
    policy: AnalyzerPolicy,
) -> Result<AnalysisOutcome, BatchError> {
    let description = source.describe();
    let mut analyzer = StreamingAnalyzer::with_policy(policy.clone());
    let doc_count = source.drive(&mut analyzer)?;
    let bailed = analyzer.should_bail().map(|s| s.to_string());
    let summary = analyzer.summary();
    let report = analyzer.report();
    let shape = analyzer.finish();
    Ok(AnalysisOutcome {
        doc_count,
        shape,
        summary,
        report,
        source: description,
        policy,
        bailed,
    })
}
