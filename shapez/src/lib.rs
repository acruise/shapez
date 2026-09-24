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
pub mod report;
pub mod session;
pub mod state;
pub mod stats;

pub use analyzer::{AnalyzerPolicy, StreamingAnalyzer};
pub use batch::{
    analyze, AnalysisOutcome, BatchError, DocumentSource, NaiveZonePolicy, TimeInterpreter,
    TimePredicate, TimeRaw,
};
pub use ingest::{Analyzer, JsonEventSink};
pub use report::{analyzer_report_bytes, analyzer_to_proto, shape_node_to_proto};
pub use state::{analyzer_from_state_bytes, analyzer_state_bytes};
