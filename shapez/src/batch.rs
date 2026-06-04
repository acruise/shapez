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
