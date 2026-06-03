//! Round-trip property test: generate values from each corpus schema,
//! drive them through the analyzer, and assert the inferred shape
//! matches the schema's intent.

use std::path::PathBuf;

use meta_types::value::ValueType;
use rand::{SeedableRng, rngs::StdRng};
use shapez::{Analyzer, StreamingAnalyzer};
use shapez::node::{ShapeKind, ShapeNode};
use shapez_gen::instantiate::instantiate;
use shapez_gen::Corpus;

fn corpus_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("..").join("shapez-gen").join("corpus")
}

fn analyze_schema(name: &str, count: usize, seed: u64) -> ShapeNode {
    let corpus = Corpus::load(corpus_path()).unwrap();
    let entry_idx = corpus
        .manifest()
        .schemas
        .iter()
        .position(|e| e.name == name)
        .unwrap_or_else(|| panic!("no schema named {name}"));
    let shape = &corpus.shapes()[entry_idx];

    let mut rng = StdRng::seed_from_u64(seed);
    let mut analyzer = StreamingAnalyzer::new();
    for ord in 0..count {
        let value = instantiate(shape, &mut rng);
        shapez_json::drive_document(&mut analyzer, ord as u64, &value);
    }
    analyzer.finish()
}

#[test]
fn scalar_root_int_infers_i64() {
    let shape = analyze_schema("atomic/scalar_root_int", 1000, 1);
    match &shape.kind {
        ShapeKind::Type(ValueType::I64) => (),
        other => panic!("expected I64, got {other:?}"),
    }
    assert_eq!(shape.stats.observation_count, 1000);
}

#[test]
fn record_stable_infers_required_struct() {
    let shape = analyze_schema("atomic/record_stable", 1000, 2);
    let fields = match &shape.kind {
        ShapeKind::Type(ValueType::Struct { fields }) => fields,
        other => panic!("expected Struct, got {other:?}"),
    };
    let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
    assert_eq!(names, vec!["created_at", "email", "id", "name"], "field order");
    for f in fields {
        assert!(!f.nullable, "field {} should not be nullable", f.name);
        assert_eq!(f.value_type, ValueType::String, "field {} should be String", f.name);
    }
}

#[test]
fn record_optional_marks_optional_fields_nullable() {
    let shape = analyze_schema("atomic/record_optional", 1000, 3);
    let fields = match &shape.kind {
        ShapeKind::Type(ValueType::Struct { fields }) => fields,
        other => panic!("expected Struct, got {other:?}"),
    };
    let id = fields.iter().find(|f| f.name == "id").expect("id");
    assert!(!id.nullable, "id is required");
    for opt in ["nickname", "bio", "avatar_url"] {
        let f = fields.iter().find(|f| f.name == opt).unwrap_or_else(|| panic!("{opt}"));
        assert!(f.nullable, "{opt} should be nullable");
    }
}

#[test]
fn map_uuid_keys_infers_map() {
    let shape = analyze_schema("atomic/map_uuid_keys", 1000, 4);
    let value_type = match &shape.kind {
        ShapeKind::Type(ValueType::Map { key_type, value_type, .. }) => {
            assert_eq!(**key_type, ValueType::String);
            value_type
        }
        other => panic!("expected Map, got {other:?}"),
    };
    let fields = match value_type.as_ref() {
        ValueType::Struct { fields } => fields,
        other => panic!("expected map value to be Struct, got {other:?}"),
    };
    let enabled = fields.iter().find(|f| f.name == "enabled").expect("enabled");
    assert_eq!(enabled.value_type, ValueType::Bool);
    let rollout = fields.iter().find(|f| f.name == "rollout_pct").expect("rollout_pct");
    assert_eq!(rollout.value_type, ValueType::I64);
    assert!(rollout.nullable, "rollout_pct is optional in the schema");
}

#[test]
fn tuple_heterogeneous_infers_tuple() {
    let shape = analyze_schema("atomic/tuple_heterogeneous", 1000, 5);
    let positions = match &shape.kind {
        ShapeKind::Tuple { positions } => positions,
        other => panic!("expected Tuple, got {other:?}"),
    };
    assert_eq!(positions.len(), 3);
    for (i, p) in positions.iter().enumerate() {
        assert!(
            matches!(p.kind, ShapeKind::Type(ValueType::F64)),
            "position {i} should be F64, got {:?}",
            p.kind,
        );
    }
}

#[test]
fn polymorphic_array_infers_variant_arms() {
    // With Space-Saving subtree clustering, the element should be a
    // Variant of three Record arms — one per discriminator value.
    let shape = analyze_schema("atomic/polymorphic_array_discriminated", 1000, 6);
    let element = match &shape.kind {
        ShapeKind::Array { element, .. } => element.as_ref(),
        other => panic!("expected rich Array root, got {other:?}"),
    };
    let arms = match &element.kind {
        ShapeKind::Variant { arms } => arms,
        other => panic!("expected Variant element, got {other:?}"),
    };
    assert_eq!(arms.len(), 3, "three discriminator arms expected, got {}", arms.len());
    for arm in arms {
        let fields: Vec<&str> = match &arm.kind {
            ShapeKind::Type(ValueType::Struct { fields }) => fields.iter().map(|f| f.name.as_str()).collect(),
            ShapeKind::Record { fields } => fields.iter().map(|f| f.name.as_str()).collect(),
            other => panic!("expected each arm to be a record, got {other:?}"),
        };
        assert!(fields.contains(&"type"), "arm missing discriminator: {fields:?}");
    }
}
