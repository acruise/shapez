//! Protobuf serialization for the analyzer's *in-progress* state.
//!
//! Designed for Spark UDAGG-style use: serialize between documents,
//! ship the bytes through a Spark Aggregator's intermediate buffer,
//! deserialize on the other side, keep feeding. The schema lives at
//! `shapez/proto/state.proto`; see [`crate::report`] for the public
//! *report* schema (a different concern — finalize output, not
//! intermediate state).
//!
//! Conversion methods on the internal analyzer types are
//! `pub(crate)`; the public API on this module is
//! [`analyzer_state_bytes`] and [`analyzer_from_state_bytes`] plus
//! the inherent methods [`crate::StreamingAnalyzer::to_state_proto`]
//! and [`crate::StreamingAnalyzer::from_state_proto`].
//!
//! Mid-document serialization is forbidden: the traversal stack and
//! `pending` pointer are intentionally not captured. Producers must
//! reach quiescence (after `document_end`) before calling
//! `to_state_proto`; doing otherwise will panic. Spark UDAGGs
//! naturally hit this boundary because the per-row reducer always
//! finishes its document before returning.
//!
//! Wire-format stability: not yet promised. The schema is shapez-
//! internal; no consumer outside this crate is meant to author it.
//! Proto3 evolution rules apply (fields are append-only), but we
//! reserve the right to renumber across shapez revisions until the
//! analyzer's internal types stabilize.

use prost::Message;

use crate::analyzer::StreamingAnalyzer;

pub mod proto {
    //! Prost-generated types. Re-exported behind `crate::state::proto`
    //! so callers don't need to know about `OUT_DIR`.
    include!(concat!(env!("OUT_DIR"), "/shapez.state.rs"));
}

/// Encode the analyzer's current state to bytes. Panics if the
/// analyzer is mid-document; callers must let the current document
/// finish first.
pub fn analyzer_state_bytes(analyzer: &StreamingAnalyzer) -> Vec<u8> {
    let state = analyzer.to_state_proto();
    let mut buf = Vec::with_capacity(state.encoded_len());
    state.encode(&mut buf).expect("prost encode into Vec is infallible");
    buf
}

/// Decode an `AnalyzerState` from bytes and reconstruct the analyzer.
pub fn analyzer_from_state_bytes(
    bytes: &[u8],
) -> Result<StreamingAnalyzer, prost::DecodeError> {
    let state = proto::AnalyzerState::decode(bytes)?;
    Ok(StreamingAnalyzer::from_state_proto(&state))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::JsonEventSink;

    fn feed_object(a: &mut StreamingAnalyzer, doc: u64, key: &str, val: i64) {
        a.document_begin(doc);
        a.object_begin();
        a.object_key(key);
        a.i64(val);
        a.object_end();
        a.document_end();
    }

    fn feed_string(a: &mut StreamingAnalyzer, doc: u64, key: &str, val: &str) {
        a.document_begin(doc);
        a.object_begin();
        a.object_key(key);
        a.string(val);
        a.object_end();
        a.document_end();
    }

    #[test]
    fn empty_analyzer_round_trips() {
        let a = StreamingAnalyzer::new();
        let bytes = analyzer_state_bytes(&a);
        let b = analyzer_from_state_bytes(&bytes).expect("decode");
        assert_eq!(a.doc_count(), b.doc_count());
        // Reports should be byte-identical after a no-op round trip.
        assert_eq!(a.report(), b.report());
    }

    #[test]
    fn record_only_data_round_trips_to_identical_report() {
        let mut a = StreamingAnalyzer::new();
        for i in 0..10 {
            feed_object(&mut a, i, "x", i as i64);
        }
        let bytes = analyzer_state_bytes(&a);
        let b = analyzer_from_state_bytes(&bytes).expect("decode");
        assert_eq!(a.doc_count(), 10);
        assert_eq!(b.doc_count(), 10);
        assert_eq!(a.report(), b.report());
        // The proto-shaped reports should also match.
        assert_eq!(a.report_proto(), b.report_proto());
    }

    #[test]
    fn variant_round_trips_with_numeric_and_string_stats() {
        let mut a = StreamingAnalyzer::new();
        for i in 0..5 {
            feed_object(&mut a, i, "k", i as i64);
        }
        for i in 5..10 {
            feed_string(&mut a, i, "k", &format!("v{i}"));
        }
        let bytes = analyzer_state_bytes(&a);
        let b = analyzer_from_state_bytes(&bytes).expect("decode");
        assert_eq!(a.report(), b.report());
        assert_eq!(a.report_proto(), b.report_proto());
    }

    #[test]
    fn array_with_cluster_sketch_round_trips() {
        let mut a = StreamingAnalyzer::new();
        // Arrays of mixed-shape elements exercise the SpaceSaving<Sig>
        // sketch on the array node.
        for i in 0..8 {
            a.document_begin(i);
            a.object_begin();
            a.object_key("xs");
            a.array_begin();
            a.i64(1);
            a.string("two");
            a.i64(3);
            a.array_end();
            a.object_end();
            a.document_end();
        }
        let bytes = analyzer_state_bytes(&a);
        let b = analyzer_from_state_bytes(&bytes).expect("decode");
        assert_eq!(a.report(), b.report());
        assert_eq!(a.report_proto(), b.report_proto());
    }

    #[test]
    fn continue_feeding_after_round_trip_matches_continuous_run() {
        // Feed half, snapshot, resume from snapshot, feed the other half.
        // Compare to a continuously-fed analyzer.
        let mut continuous = StreamingAnalyzer::new();
        for i in 0..20 {
            feed_object(&mut continuous, i, "x", (i % 5) as i64);
        }

        let mut split = StreamingAnalyzer::new();
        for i in 0..10 {
            feed_object(&mut split, i, "x", (i % 5) as i64);
        }
        let bytes = analyzer_state_bytes(&split);
        let mut split = analyzer_from_state_bytes(&bytes).expect("decode");
        for i in 10..20 {
            feed_object(&mut split, i, "x", (i % 5) as i64);
        }

        assert_eq!(continuous.doc_count(), split.doc_count());
        assert_eq!(continuous.report(), split.report());
        assert_eq!(continuous.report_proto(), split.report_proto());
    }

    #[test]
    #[should_panic(expected = "must be quiescent")]
    fn serializing_mid_document_panics() {
        let mut a = StreamingAnalyzer::new();
        a.document_begin(0);
        a.object_begin();
        a.object_key("k");
        // Now serialize — should panic, traversal stack is non-empty.
        let _ = analyzer_state_bytes(&a);
    }

    #[test]
    fn policy_round_trips() {
        let mut a = crate::StreamingAnalyzer::with_policy(crate::AnalyzerPolicy {
            record_view_cap: 128,
            positional_view_cap: 96,
            cluster_cap: 32,
            reason: "test-run".into(),
            max_eviction_rate: Some(0.25),
            max_docs: Some(1_000_000),
            min_docs_before_bail: 200,
        });
        feed_object(&mut a, 0, "x", 1);
        let bytes = analyzer_state_bytes(&a);
        let b = analyzer_from_state_bytes(&bytes).expect("decode");
        let p = b.policy();
        assert_eq!(p.record_view_cap, 128);
        assert_eq!(p.positional_view_cap, 96);
        assert_eq!(p.cluster_cap, 32);
        assert_eq!(p.reason, "test-run");
        assert_eq!(p.max_eviction_rate, Some(0.25));
        assert_eq!(p.max_docs, Some(1_000_000));
        assert_eq!(p.min_docs_before_bail, 200);
    }
}
