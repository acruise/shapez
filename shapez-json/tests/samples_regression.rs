//! Regression tests over the committed `samples/*.jsonl` fixtures.
//!
//! Each test drives one sample through `analyze()` and asserts
//! structural invariants — root shape kind, key decisions, format
//! detectors firing where expected. Wording-level details (exact
//! report text, percentile values, etc.) are deliberately not pinned;
//! we only catch *behavioral* regressions in the analyzer.
//!
//! If you change the analyzer materially and these break, ask: is the
//! new behavior actually wrong, or is the assertion stale? When the
//! latter, update the assertion. When the former, fix the analyzer.

use std::path::PathBuf;

use meta_types::value::ValueType;
use shapez::node::{ShapeKind, ShapeNode};
use shapez::AnalyzerPolicy;
use shapez::batch::analyze;
use shapez_json::batch::JsonlDir;

fn sample(name: &str) -> ShapeNode {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("samples");
    let target = name.to_string();
    let source = JsonlDir::new(dir).filter(move |p| {
        p.file_name()
            .and_then(|n| n.to_str())
            .map(|n| n == format!("{target}.jsonl"))
            .unwrap_or(false)
    });
    let outcome =
        analyze(source, AnalyzerPolicy::default()).expect("analyze sample");
    assert!(outcome.doc_count > 0, "no documents loaded from sample {name}");
    outcome.shape
}

fn report(name: &str) -> String {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("samples");
    let target = name.to_string();
    let source = JsonlDir::new(dir).filter(move |p| {
        p.file_name()
            .and_then(|n| n.to_str())
            .map(|n| n == format!("{target}.jsonl"))
            .unwrap_or(false)
    });
    let outcome =
        analyze(source, AnalyzerPolicy::default()).expect("analyze sample");
    outcome.report
}

// --- The boring cases ----------------------------------------------------

#[test]
fn scalar_root_int_still_an_i64() {
    let s = sample("scalar_root_int");
    assert!(
        matches!(s.kind, ShapeKind::Type(ValueType::I64)),
        "expected i64 root, got {:?}",
        s.kind,
    );
}

#[test]
fn record_stable_still_four_required_strings() {
    let s = sample("record_stable");
    match s.kind {
        ShapeKind::Type(ValueType::Struct { fields }) => {
            assert_eq!(fields.len(), 4);
            for f in &fields {
                assert!(!f.nullable, "{} should not be nullable", f.name);
                assert_eq!(f.value_type, ValueType::String);
            }
        }
        other => panic!("expected struct, got {other:?}"),
    }
    // The four fields' string-format detectors should still fire.
    let r = report("record_stable");
    assert!(r.contains("UUID"), "expected UUID format on id field");
    assert!(r.contains("ISO-timestamp"), "expected ISO-timestamp on created_at");
    assert!(r.contains("email"), "expected email format on email field");
}

#[test]
fn record_optional_still_has_three_optionals() {
    let s = sample("record_optional");
    match s.kind {
        ShapeKind::Type(ValueType::Struct { fields }) => {
            let id = fields.iter().find(|f| f.name == "id").unwrap();
            assert!(!id.nullable, "id should be required");
            for opt in ["nickname", "bio", "avatar_url"] {
                let f = fields.iter().find(|f| f.name == opt).unwrap();
                assert!(f.nullable, "{opt} should be nullable");
            }
        }
        other => panic!("expected struct, got {other:?}"),
    }
}

// --- The load-bearing cases (the actual fun) -----------------------------

#[test]
fn map_uuid_keys_still_a_map_of_records() {
    let s = sample("map_uuid_keys");
    let value_type = match s.kind {
        ShapeKind::Type(ValueType::Map { value_type, .. }) => value_type,
        other => panic!("expected map root, got {other:?}"),
    };
    match value_type.as_ref() {
        ValueType::Struct { fields } => {
            assert!(fields.iter().any(|f| f.name == "enabled"));
        }
        other => panic!("expected struct map values, got {other:?}"),
    }
    let r = report("map_uuid_keys");
    assert!(r.contains("100.0% UUID") || r.contains("UUID"), "UUID format on keys");
    assert!(r.contains("decision: MAP"));
}

#[test]
fn tuple_heterogeneous_still_a_four_tuple() {
    let s = sample("tuple_heterogeneous");
    let positions = match s.kind {
        ShapeKind::Tuple { positions } => positions,
        other => panic!("expected tuple root, got {other:?}"),
    };
    let kinds: Vec<&ValueType> = positions
        .iter()
        .map(|p| match &p.kind {
            ShapeKind::Type(vt) => vt,
            other => panic!("tuple position not a scalar: {other:?}"),
        })
        .collect();
    assert_eq!(
        kinds,
        vec![&ValueType::String, &ValueType::F64, &ValueType::I64, &ValueType::Bool],
    );
}

#[test]
fn polymorphic_array_still_emits_three_variant_arms() {
    let s = sample("polymorphic_array_discriminated");
    let element = match &s.kind {
        ShapeKind::Array { element, .. } => element.as_ref(),
        other => panic!("expected rich Array, got {other:?}"),
    };
    let arms = match &element.kind {
        ShapeKind::Variant { arms } => arms,
        other => panic!("expected Variant element, got {other:?}"),
    };
    assert_eq!(arms.len(), 3, "expected three discriminator arms");
}

#[test]
fn nested_wildcards_still_collapses_to_map_of_map() {
    let s = sample("nested_wildcards");
    let outer_value = match &s.kind {
        ShapeKind::Type(ValueType::Map { value_type, .. }) => value_type.as_ref(),
        other => panic!("expected outer Map, got {other:?}"),
    };
    match outer_value {
        ValueType::Map { .. } => (),
        other => panic!("expected nested Map, got {other:?}"),
    }
    let r = report("nested_wildcards");
    assert_eq!(
        r.matches("decision: MAP").count(),
        2,
        "expected two MAP decisions in nested wildcards report",
    );
    assert!(r.contains(".*.*"), "expected double-wildcard pattern");
}

#[test]
fn templated_skew_still_clusters_four_template_arms() {
    let s = sample("templated_skew");
    let element = match &s.kind {
        ShapeKind::Array { element, .. } => element.as_ref(),
        other => panic!("expected Array root, got {other:?}"),
    };
    let arms = match &element.kind {
        ShapeKind::Variant { arms } => arms,
        other => panic!("expected Variant element, got {other:?}"),
    };
    assert!(
        arms.len() >= 4,
        "expected at least 4 cluster arms for the templated_skew distribution, got {}",
        arms.len(),
    );
    let r = report("templated_skew");
    // The four template signatures should appear in the cluster top-K.
    assert!(r.contains("record{evt, page, ts, user}"), "missing view arm");
    assert!(r.contains("record{evt, order, ts, user}"), "missing purchase arm");
    assert!(r.contains("record{evt, host, metric, ts}"), "missing metric arm");
    assert!(r.contains("record{attempt, evt, reason, ts}"), "missing auth_fail arm");
}

#[test]
fn epoch_events_still_detects_epoch_millis() {
    let r = report("epoch_events");
    assert!(
        r.contains("looks like: epoch millis"),
        "epoch-millis detection should fire on .ts field; report:\n{r}",
    );
}

#[test]
fn skeleton_ids_still_resolves_to_a_9_9() {
    let r = report("skeleton_ids");
    assert!(
        r.contains("`A-9-9`"),
        "ORD-YYYY-NNNNNN skeleton should be `A-9-9`; report:\n{r}",
    );
}

// --- The chaos survival cases --------------------------------------------

#[test]
fn random_tree_survives() {
    // No structural assertion — just that analyze() returns without
    // panicking and produces *something*. The whole point of the chaos
    // case is the analyzer doesn't lose its mind.
    let s = sample("random_tree");
    assert!(matches!(
        s.kind,
        ShapeKind::Variant { .. }
            | ShapeKind::Type(_)
            | ShapeKind::Record { .. }
            | ShapeKind::Array { .. }
            | ShapeKind::Map { .. }
    ));
}

#[test]
fn deep_mixed_spine_survives() {
    // Deep spine alternates object/array — the analyzer ends up with a
    // ShapeKind::Record (rich form) at the root because the inner
    // levels carry Tuple structure that flat ValueType can't express.
    // Accept either rich Record or flat Type(Struct).
    let s = sample("deep_mixed_spine_150");
    let names: Vec<String> = match &s.kind {
        ShapeKind::Type(ValueType::Struct { fields }) => {
            fields.iter().map(|f| f.name.clone()).collect()
        }
        ShapeKind::Record { fields } => fields.iter().map(|f| f.name.clone()).collect(),
        other => panic!("expected record root, got {other:?}"),
    };
    assert!(names.iter().any(|n| n == "next"), "fields={names:?}");
}

#[test]
fn wide_object_200_keys_still_drops_record_view() {
    let r = report("wide_object_200_keys");
    assert!(r.contains("record_view DROPPED"));
    assert!(r.contains("decision: MAP"));
}

#[test]
fn wide_array_200_still_drops_positional_view() {
    let r = report("wide_array_200");
    assert!(r.contains("positional_view DROPPED"));
    assert!(r.contains("decision: BAG"));
}
