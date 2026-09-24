//! Protobuf serialization for analyzer output.
//!
//! Schema is defined in `shapez/proto/report.proto` and compiled into a
//! Rust module at build time via prost-build. It is intentionally a
//! shapez-specific, hand-rolled contract: only the leaf categories the
//! analyzer actually emits today (`Null`, `Bool`, `I64`, `U64`, `F64`,
//! `String`) appear in the report, and compound shapes are always
//! described via `ShapeKind` rather than nested upstream types. When
//! the analyzer compresses a uniform subtree to `Type(ValueType::Array
//! {...})` etc., the serializer expands it back into the equivalent
//! rich `ShapeKind` form on the wire, so the proto never needs to
//! carry the upstream `ValueType` recursion.
//!
//! If shapez ever needs additional leaf categories (e.g. promoting
//! format-detected UUIDs to a first-class `LEAF_TYPE_UUID` rather than
//! leaving them as `LEAF_TYPE_STRING` with a side-channel format
//! tag), extend `LeafType` in the proto and the match below.

use meta_types::value::ValueType;
use prost::Message;

use crate::analyzer::StreamingAnalyzer;
use crate::node::{ShapeKind, ShapeNode};
use crate::stats::Stats;

pub mod proto {
    //! Prost-generated types. Re-exported behind `crate::report::proto`
    //! so callers don't need to know about `OUT_DIR`.
    include!(concat!(env!("OUT_DIR"), "/shapez.report.rs"));
}

use proto::shape_kind::Variant as KindVariant;

/// Build the structured analysis report for an analyzer's current
/// state. Walks `&self` so the analyzer can keep ingesting after.
pub fn analyzer_to_proto(analyzer: &StreamingAnalyzer) -> proto::AnalysisReport {
    let shape = analyzer.build_shape();
    proto::AnalysisReport {
        doc_count: analyzer.doc_count(),
        shape: Some(shape_node_to_proto(&shape)),
    }
}

/// Convert an internal `ShapeNode` into the proto-shaped public type.
pub fn shape_node_to_proto(node: &ShapeNode) -> proto::ShapeNode {
    proto::ShapeNode {
        stats: Some(stats_to_proto(&node.stats)),
        kind: Some(Box::new(proto::ShapeKind {
            variant: Some(shape_kind_to_proto(&node.kind)),
        })),
    }
}

fn stats_to_proto(s: &Stats) -> proto::Stats {
    proto::Stats {
        observation_count: s.observation_count,
        first_doc_ordinal: s.first_doc_ordinal,
        last_doc_ordinal: s.last_doc_ordinal,
    }
}

fn shape_kind_to_proto(kind: &ShapeKind) -> KindVariant {
    match kind {
        ShapeKind::Type(vt) => value_type_to_kind(vt),
        ShapeKind::Variant { arms } => KindVariant::VariantKind(proto::VariantKind {
            arms: arms.iter().map(shape_node_to_proto).collect(),
        }),
        ShapeKind::Tuple { positions } => KindVariant::Tuple(proto::TupleKind {
            positions: positions.iter().map(shape_node_to_proto).collect(),
        }),
        ShapeKind::Absent => KindVariant::Absent(proto::AbsentKind {}),
        ShapeKind::Array { element, elements_nullable } => KindVariant::Array(Box::new(proto::ArrayKind {
            element: Some(Box::new(shape_node_to_proto(element))),
            elements_nullable: *elements_nullable,
        })),
        ShapeKind::Record { fields } => KindVariant::Record(proto::RecordKind {
            fields: fields
                .iter()
                .map(|f| proto::ShapeField {
                    name: f.name.clone(),
                    shape: Some(shape_node_to_proto(&f.shape)),
                    nullable: f.nullable,
                })
                .collect(),
        }),
        ShapeKind::Map { key, value, values_nullable } => KindVariant::Map(Box::new(proto::MapKind {
            key: Some(Box::new(shape_node_to_proto(key))),
            value: Some(Box::new(shape_node_to_proto(value))),
            values_nullable: *values_nullable,
        })),
    }
}

/// Map a `meta::ValueType` to a proto `ShapeKind` variant. Leaf
/// categories become `LeafKind`; compressed compound types are
/// expanded into the equivalent rich `ShapeKind` (synthesizing default
/// `Stats` for the leaves below them, since the analyzer chose not to
/// keep per-position counts when it compressed).
fn value_type_to_kind(vt: &ValueType) -> KindVariant {
    match vt {
        ValueType::Null => leaf(proto::LeafType::Null),
        ValueType::Bool => leaf(proto::LeafType::Bool),
        ValueType::I64 => leaf(proto::LeafType::I64),
        ValueType::U64 => leaf(proto::LeafType::U64),
        ValueType::F64 => leaf(proto::LeafType::F64),
        ValueType::String => leaf(proto::LeafType::String),

        // Compressed compounds — expand into their rich proto form.
        ValueType::Array { element_type, elements_nullable } => {
            KindVariant::Array(Box::new(proto::ArrayKind {
                element: Some(Box::new(leaf_shape_node(element_type))),
                elements_nullable: *elements_nullable,
            }))
        }
        ValueType::Map { key_type, value_type, values_nullable } => {
            KindVariant::Map(Box::new(proto::MapKind {
                key: Some(Box::new(leaf_shape_node(key_type))),
                value: Some(Box::new(leaf_shape_node(value_type))),
                values_nullable: *values_nullable,
            }))
        }
        ValueType::Struct { fields } => KindVariant::Record(proto::RecordKind {
            fields: fields
                .iter()
                .map(|f| proto::ShapeField {
                    name: f.name.clone(),
                    shape: Some(leaf_shape_node(&f.value_type)),
                    nullable: f.nullable,
                })
                .collect(),
        }),

        // Variants the analyzer doesn't emit today. We don't drop them
        // silently — bug if this fires.
        other => {
            debug_assert!(
                false,
                "shapez analyzer emitted ValueType variant outside the reporting alphabet: {other:?}",
            );
            leaf(proto::LeafType::Unspecified)
        }
    }
}

fn leaf(t: proto::LeafType) -> KindVariant {
    KindVariant::Leaf(proto::LeafKind { r#type: t as i32 })
}

/// Synthesize a `ShapeNode` carrying a leaf-or-compound for one of the
/// expanded compressed-compound paths. Stats default to zeros — the
/// internal compressed form didn't keep per-position counts.
fn leaf_shape_node(vt: &ValueType) -> proto::ShapeNode {
    proto::ShapeNode {
        stats: Some(proto::Stats {
            observation_count: 0,
            first_doc_ordinal: 0,
            last_doc_ordinal: 0,
        }),
        kind: Some(Box::new(proto::ShapeKind {
            variant: Some(value_type_to_kind(vt)),
        })),
    }
}

/// Encode an analyzer's current report to bytes.
pub fn analyzer_report_bytes(analyzer: &StreamingAnalyzer) -> Vec<u8> {
    let report = analyzer_to_proto(analyzer);
    let mut buf = Vec::with_capacity(report.encoded_len());
    report.encode(&mut buf).expect("prost encode into Vec is infallible");
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest::JsonEventSink;
    use crate::StreamingAnalyzer;

    fn feed_object(a: &mut StreamingAnalyzer, doc: u64, key: &str, val_i64: i64) {
        a.document_begin(doc);
        a.object_begin();
        a.object_key(key);
        a.i64(val_i64);
        a.object_end();
        a.document_end();
    }

    #[test]
    fn empty_analyzer_round_trips() {
        let a = StreamingAnalyzer::new();
        let bytes = analyzer_report_bytes(&a);
        let decoded = proto::AnalysisReport::decode(&*bytes).expect("decode");
        assert_eq!(decoded.doc_count, 0);
        assert!(decoded.shape.is_some());
    }

    #[test]
    fn simple_record_round_trips() {
        let mut a = StreamingAnalyzer::new();
        for i in 0..5 {
            feed_object(&mut a, i, "x", i as i64);
        }
        let bytes = analyzer_report_bytes(&a);
        let decoded = proto::AnalysisReport::decode(&*bytes).expect("decode");
        assert_eq!(decoded.doc_count, 5);
        let shape = decoded.shape.expect("shape present");
        let kind = shape.kind.expect("kind present").variant.expect("variant present");
        // 5 stable rows = Record (rich) or Record (expanded from Type{Struct})
        // — either way it surfaces here as Record on the wire.
        let record = match kind {
            KindVariant::Record(r) => r,
            other => panic!("expected Record at root, got {other:?}"),
        };
        assert_eq!(record.fields.len(), 1);
        assert_eq!(record.fields[0].name, "x");
        // The field's leaf should be I64.
        let leaf_kind = record.fields[0]
            .shape
            .as_ref()
            .unwrap()
            .kind
            .as_ref()
            .unwrap()
            .variant
            .as_ref()
            .unwrap();
        let leaf = match leaf_kind {
            KindVariant::Leaf(l) => l,
            other => panic!("expected Leaf for .x, got {other:?}"),
        };
        assert_eq!(leaf.r#type, proto::LeafType::I64 as i32);
    }

    #[test]
    fn variant_arms_round_trip() {
        let mut a = StreamingAnalyzer::new();
        for i in 0..3 {
            feed_object(&mut a, i, "k", i as i64);
        }
        a.document_begin(3);
        a.object_begin();
        a.object_key("k");
        a.string("hi");
        a.object_end();
        a.document_end();

        let bytes = analyzer_report_bytes(&a);
        let decoded = proto::AnalysisReport::decode(&*bytes).expect("decode");
        assert_eq!(decoded.doc_count, 4);

        let root_kind = decoded
            .shape
            .unwrap()
            .kind
            .unwrap()
            .variant
            .unwrap();
        let record = match root_kind {
            KindVariant::Record(r) => r,
            other => panic!("expected Record at root, got {other:?}"),
        };
        let k_field = record.fields.iter().find(|f| f.name == "k").expect("k field");
        let k_kind = k_field
            .shape
            .as_ref()
            .unwrap()
            .kind
            .as_ref()
            .unwrap()
            .variant
            .as_ref()
            .unwrap();
        let arms = match k_kind {
            KindVariant::VariantKind(v) => &v.arms,
            other => panic!("expected Variant at .k, got {other:?}"),
        };
        let mut leaf_types: Vec<i32> = arms
            .iter()
            .filter_map(|n| match n.kind.as_ref()?.variant.as_ref()? {
                KindVariant::Leaf(l) => Some(l.r#type),
                _ => None,
            })
            .collect();
        leaf_types.sort();
        assert!(
            leaf_types.contains(&(proto::LeafType::I64 as i32))
                && leaf_types.contains(&(proto::LeafType::String as i32)),
            "expected I64 and String leaf arms; got {leaf_types:?}",
        );
    }

    #[test]
    fn compressed_array_value_type_expands_to_arraykind() {
        // value_type_to_kind is the path that compresses Type(Array{...})
        // back into rich ArrayKind on the wire. Exercise it directly.
        let kind = value_type_to_kind(&ValueType::Array {
            element_type: Box::new(ValueType::I64),
            elements_nullable: false,
        });
        let array = match kind {
            KindVariant::Array(a) => a,
            other => panic!("expected ArrayKind, got {other:?}"),
        };
        assert_eq!(array.elements_nullable, false);
        let leaf_kind = array.element.unwrap().kind.unwrap().variant.unwrap();
        let leaf = match leaf_kind {
            KindVariant::Leaf(l) => l,
            other => panic!("expected Leaf, got {other:?}"),
        };
        assert_eq!(leaf.r#type, proto::LeafType::I64 as i32);
    }

    #[test]
    fn compressed_struct_value_type_expands_to_recordkind() {
        use meta_types::value::StructField as MetaStructField;
        let kind = value_type_to_kind(&ValueType::Struct {
            fields: vec![
                MetaStructField {
                    name: "n".into(),
                    human_name: "".into(),
                    value_type: ValueType::I64,
                    nullable: false,
                },
                MetaStructField {
                    name: "s".into(),
                    human_name: "".into(),
                    value_type: ValueType::String,
                    nullable: true,
                },
            ],
        });
        let record = match kind {
            KindVariant::Record(r) => r,
            other => panic!("expected RecordKind, got {other:?}"),
        };
        assert_eq!(record.fields.len(), 2);
        assert_eq!(record.fields[0].name, "n");
        assert_eq!(record.fields[1].name, "s");
        assert_eq!(record.fields[1].nullable, true);
    }
}
