//! Ingest-side input contract for the analyzer.
//!
//! `JsonEventSink` is a SAX-style trait: adapters (an `EventDriver` over
//! `serde_json::Value`, an eventual simd-json tape walker, etc.) drive
//! events; the analyzer accumulates state. Document boundaries are
//! explicit so the analyzer can finalize per-doc accumulators. Scalars are
//! reported with their concrete type (i64/u64/f64 split). Strings cross
//! the boundary as `&str` so adapters can borrow from their underlying
//! buffer without forcing an owned allocation.

use crate::node::ShapeNode;

pub trait JsonEventSink {
    fn document_begin(&mut self, doc_ordinal: u64);
    fn document_end(&mut self);

    fn null(&mut self);
    fn bool(&mut self, v: bool);
    fn i64(&mut self, v: i64);
    fn u64(&mut self, v: u64);
    fn f64(&mut self, v: f64);
    fn string(&mut self, s: &str);

    fn array_begin(&mut self);
    fn array_end(&mut self);

    fn object_begin(&mut self);
    /// Reported between `object_begin` and `object_end`, once per key,
    /// immediately before the value events for that key.
    fn object_key(&mut self, key: &str);
    fn object_end(&mut self);
}

/// An analyzer is a `JsonEventSink` that can be consumed to produce a
/// `ShapeNode` tree. Consumers drive events via the sink, then call
/// `finish` to read the final shape.
pub trait Analyzer: JsonEventSink {
    fn finish(self) -> ShapeNode;
}
