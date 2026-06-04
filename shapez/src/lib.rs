//! shapez: structural shape inference over streams of typed values.
//!
//! Input is a stream of `meta_types::value::Value`. Output is a `ShapeNode`
//! tree plus a stream of `ShapeException`s when assertions are falsified.

pub mod analyzer;
pub mod assertions;
pub mod batch;
pub mod exceptions;
pub mod ingest;
pub mod node;
pub mod path;
pub mod session;
pub mod stats;

pub use analyzer::{AnalyzerPolicy, StreamingAnalyzer};
pub use batch::{analyze, AnalysisOutcome, BatchError, DocumentSource, TimePredicate};
pub use ingest::{Analyzer, JsonEventSink};
