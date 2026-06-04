//! Adversarial-shape stress test: generate truly random JSON with
//! arbitrary root types, mixed scalar types, and varying nesting depth.
//! The analyzer must accept every document without panicking, finish()
//! must return a `ShapeNode`, and the report must render.
//!
//! These tests do not assert shape correctness — chaos input has no
//! "correct" shape — only that the analyzer survives.

use rand::{Rng, SeedableRng, rngs::StdRng};
use serde_json::{Map, Number, Value};

use shapez::node::{ShapeKind, ShapeNode};
use shapez::{Analyzer, StreamingAnalyzer};
use shapez_json::chaos::{
    random_string, random_scalar, random_value, nested_array_spine, nested_mixed_spine,
};

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

fn run_chaos(seed: u64, count: usize, depth: u32, branch: u32) -> (ShapeNode, String, u64) {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut analyzer = StreamingAnalyzer::new();
    for ord in 0..count {
        let v = random_value(&mut rng, depth, branch);
        shapez_json::drive_document(&mut analyzer, ord as u64, &v);
    }
    let docs = analyzer.doc_count();
    let report = analyzer.report();
    let shape = analyzer.finish();
    (shape, report, docs)
}

fn count_nodes(s: &ShapeNode) -> usize {
    1 + match &s.kind {
        ShapeKind::Type(_) | ShapeKind::Absent => 0,
        ShapeKind::Variant { arms } | ShapeKind::Tuple { positions: arms } => {
            arms.iter().map(count_nodes).sum()
        }
        ShapeKind::Array { element, .. } => count_nodes(element),
        ShapeKind::Record { fields } => fields.iter().map(|f| count_nodes(&f.shape)).sum(),
        ShapeKind::Map { key, value, .. } => count_nodes(key) + count_nodes(value),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn chaos_arbitrary_root_survives() {
    // Moderate scale: 1000 docs, depth 6, branching 8. Should exercise
    // every code path in the analyzer at least once.
    let (shape, report, docs) = run_chaos(0x5_4A_9E_2C_4A_05_u64, 1000, 6, 8);
    assert_eq!(docs, 1000);
    assert!(!report.is_empty());
    assert!(count_nodes(&shape) > 0);
}

#[test]
fn chaos_runs_across_many_seeds() {
    // Many short runs across distinct seeds, catching seed-dependent
    // crashes that a single seed would miss.
    for seed in 0..32 {
        let (_shape, report, docs) = run_chaos(seed, 100, 5, 6);
        assert_eq!(docs, 100, "seed {seed}");
        assert!(!report.is_empty(), "seed {seed}");
    }
}

#[test]
fn chaos_deep_nesting_survives() {
    // High depth, low branching → tall narrow trees. Stresses the path
    // stack and the recursive finalizer.
    let (shape, _report, docs) = run_chaos(42, 200, 20, 2);
    assert_eq!(docs, 200);
    assert!(count_nodes(&shape) > 0);
}

#[test]
fn chaos_wide_object_keys_drop_record_view() {
    // Force a single object root with many distinct keys per doc so the
    // record_view (K=64) certainly drops and the finalizer hits the map
    // branch. Verifies the path through MAP decision is panic-free under
    // chaos input.
    let mut rng = StdRng::seed_from_u64(7);
    let mut analyzer = StreamingAnalyzer::new();
    for doc in 0..50 {
        let mut map = Map::new();
        // 100 distinct keys per doc, ensuring record_view drops fast.
        for _ in 0..100 {
            let key = random_string(&mut rng, 6..10);
            map.insert(key, random_value(&mut rng, 3, 4));
        }
        shapez_json::drive_document(&mut analyzer, doc, &Value::Object(map));
    }
    let report = analyzer.report();
    let shape = analyzer.finish();
    assert!(report.contains("MAP"), "expected MAP decision in report");
    match shape.kind {
        ShapeKind::Type(_) | ShapeKind::Map { .. } => (),
        other => panic!("expected map/struct root, got {other:?}"),
    }
}

#[test]
fn chaos_wide_arrays_drop_positional_view() {
    // Arrays of length >> positional_view_cap (32). Forces the bag-view
    // path in the finalizer.
    let mut rng = StdRng::seed_from_u64(11);
    let mut analyzer = StreamingAnalyzer::new();
    for doc in 0..50 {
        let n = rng.gen_range(64..=128);
        let items: Vec<Value> = (0..n).map(|_| random_scalar(&mut rng)).collect();
        shapez_json::drive_document(&mut analyzer, doc, &Value::Array(items));
    }
    let report = analyzer.report();
    let _shape = analyzer.finish();
    assert!(
        report.contains("BAG") || report.contains("positional_view DROPPED"),
        "expected bag decision or positional drop in report, got:\n{report}"
    );
}

#[test]
fn chaos_unsigned_above_i64_max_is_u64() {
    // Pin down that numbers larger than i64::MAX are observed as U64,
    // not coerced to F64 or dropped.
    let big = (i64::MAX as u64) + 17;
    let v = Value::Number(Number::from(big));
    let mut analyzer = StreamingAnalyzer::new();
    for ord in 0..10 {
        shapez_json::drive_document(&mut analyzer, ord, &v);
    }
    let shape = analyzer.finish();
    use meta_types::value::ValueType;
    match shape.kind {
        ShapeKind::Type(ValueType::U64) => (),
        other => panic!("expected U64 root, got {other:?}"),
    }
}

#[test]
fn chaos_extreme_depth_500() {
    // Build a 500-level nested array-of-array-of-...-of-scalar value.
    // Stresses (a) drive_value's recursion, (b) the analyzer's path
    // stack, and (c) the finalizer's recursive build. Run on a thread
    // with an explicitly-sized stack so we test the analyzer, not the
    // default test-thread stack budget.
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            let v = nested_array_spine(500);
            let mut analyzer = StreamingAnalyzer::new();
            for ord in 0..20 {
                shapez_json::drive_document(&mut analyzer, ord, &v);
            }
            let report = analyzer.report();
            let shape = analyzer.finish();
            assert!(report.len() > 10_000, "report should be substantial");
            // 500 nested arrays + 1 scalar at the bottom + 1 root = ~502 nodes.
            let nodes = count_nodes(&shape);
            assert!(nodes >= 500, "expected >=500 nodes, got {nodes}");
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn chaos_extreme_depth_mixed_arrays_and_objects() {
    // Same depth budget but alternating object/array containers along
    // the spine. Exercises the path-stack frame discriminator and
    // object_key plumbing under deep nesting.
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            let v = nested_mixed_spine(400);
            let mut analyzer = StreamingAnalyzer::new();
            for ord in 0..10 {
                shapez_json::drive_document(&mut analyzer, ord, &v);
            }
            let _report = analyzer.report();
            let shape = analyzer.finish();
            assert!(count_nodes(&shape) >= 400);
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn chaos_extreme_wide_object_500_keys() {
    // 500 stable keys per doc forces record_view (K=64) to drop and
    // map_value to absorb the rest. After 50 docs we have 500 distinct
    // keys × 50 obs each in the map_value's accumulator.
    let mut analyzer = StreamingAnalyzer::new();
    for doc in 0..50 {
        let mut map = Map::new();
        for k in 0..500 {
            map.insert(format!("k{k:04}"), Value::Number(Number::from(doc as i64)));
        }
        shapez_json::drive_document(&mut analyzer, doc, &Value::Object(map));
    }
    let report = analyzer.report();
    let shape = analyzer.finish();
    assert!(report.contains("MAP"), "expected MAP decision\n{report}");
    use meta_types::value::ValueType;
    let value_type = match &shape.kind {
        ShapeKind::Type(ValueType::Map { value_type, .. }) => value_type,
        other => panic!("expected Type(Map), got {other:?}"),
    };
    assert_eq!(**value_type, ValueType::I64);
}

#[test]
fn chaos_extreme_wide_array_300_positions() {
    // 300-element arrays for 100 docs (30k total scalar observations).
    // positional_view cap (32) is blown through fast; bag_value absorbs
    // the rest. Tuple decision declines because mode_len=300 exceeds
    // the small-mode threshold.
    let mut analyzer = StreamingAnalyzer::new();
    for doc in 0..100 {
        let items: Vec<Value> = (0..300).map(|i| Value::Number(Number::from(i as i64))).collect();
        shapez_json::drive_document(&mut analyzer, doc, &Value::Array(items));
    }
    let report = analyzer.report();
    let _shape = analyzer.finish();
    assert!(report.contains("BAG"), "expected BAG decision\n{report}");
    assert!(
        report.contains("positional_view DROPPED"),
        "expected positional drop note\n{report}",
    );
}

#[test]
fn chaos_extreme_combined_deep_and_wide() {
    // A wide object root (200 keys) where each value is itself a
    // 100-level nested array. Pathological but legal. Stresses memory
    // (200 × 100 = 20k arena nodes per analyzer) and recursion.
    fn deep_array(depth: u32) -> Value {
        if depth == 0 {
            Value::String("leaf".into())
        } else {
            Value::Array(vec![deep_array(depth - 1)])
        }
    }
    std::thread::Builder::new()
        .stack_size(16 * 1024 * 1024)
        .spawn(|| {
            let mut analyzer = StreamingAnalyzer::new();
            for doc in 0..5 {
                let mut map = Map::new();
                for k in 0..200 {
                    map.insert(format!("k{k:03}"), deep_array(100));
                }
                shapez_json::drive_document(&mut analyzer, doc, &Value::Object(map));
            }
            let report = analyzer.report();
            let shape = analyzer.finish();
            assert!(report.contains("MAP"), "expected MAP at root");
            assert!(count_nodes(&shape) >= 100, "deep arrays should produce many nodes");
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn chaos_low_entropy_templates_cluster_correctly() {
    // The load-bearing case for the design: documents are wide+deep but
    // the SHAPE distribution has low entropy — a small number of recurring
    // templates dominate. The Space-Saving cluster sketch should identify
    // each template as its own variant arm, with counts reflecting the
    // skewed distribution. A 5% trickle of one-off noise shapes exercises
    // the eviction path without displacing the heavy hitters.

    fn template_a(rng: &mut StdRng) -> Value {
        // ~70%: page-view event.
        let mut user = Map::new();
        user.insert("id".into(), Value::String(random_string(rng, 8..12)));
        user.insert("tier".into(), Value::String("paid".into()));
        let mut page = Map::new();
        page.insert("url".into(), Value::String(random_string(rng, 8..16)));
        page.insert("load_ms".into(), Value::Number(Number::from(rng.gen_range(50..2000i64))));
        let mut root = Map::new();
        root.insert("evt".into(), Value::String("view".into()));
        root.insert("user".into(), Value::Object(user));
        root.insert("page".into(), Value::Object(page));
        Value::Object(root)
    }

    fn template_b(rng: &mut StdRng) -> Value {
        // ~20%: purchase event with an items array.
        let items: Vec<Value> = (0..rng.gen_range(1..4))
            .map(|_| Value::String(random_string(rng, 4..8)))
            .collect();
        let mut order = Map::new();
        order.insert("items".into(), Value::Array(items));
        order.insert("total_cents".into(), Value::Number(Number::from(rng.gen_range(100..50_000i64))));
        let mut root = Map::new();
        root.insert("evt".into(), Value::String("purchase".into()));
        root.insert("order".into(), Value::Object(order));
        Value::Object(root)
    }

    fn template_c(rng: &mut StdRng) -> Value {
        // ~10%: system metric.
        let mut metric = Map::new();
        metric.insert("name".into(), Value::String(random_string(rng, 6..10)));
        metric.insert(
            "value".into(),
            Number::from_f64(rng.gen_range(0.0..100.0))
                .map(Value::Number)
                .unwrap_or(Value::Null),
        );
        let mut root = Map::new();
        root.insert("evt".into(), Value::String("metric".into()));
        root.insert("metric".into(), Value::Object(metric));
        Value::Object(root)
    }

    let mut rng = StdRng::seed_from_u64(0xC1_05_7E_27);
    let mut analyzer = StreamingAnalyzer::new();

    let mut expected: std::collections::HashMap<&'static str, u64> =
        [("view", 0), ("purchase", 0), ("metric", 0), ("noise", 0)].into_iter().collect();

    for doc in 0..100 {
        let mut items = Vec::with_capacity(200);
        for _ in 0..200 {
            let r: f64 = rng.gen();
            if r < 0.70 {
                items.push(template_a(&mut rng));
                *expected.get_mut("view").unwrap() += 1;
            } else if r < 0.90 {
                items.push(template_b(&mut rng));
                *expected.get_mut("purchase").unwrap() += 1;
            } else if r < 0.95 {
                items.push(template_c(&mut rng));
                *expected.get_mut("metric").unwrap() += 1;
            } else {
                // 5% noise: a fresh random shape every time, which the
                // sketch will mostly evict.
                items.push(random_value(&mut rng, 4, 3));
                *expected.get_mut("noise").unwrap() += 1;
            }
        }
        shapez_json::drive_document(&mut analyzer, doc, &Value::Array(items));
    }
    let report = analyzer.report();
    let shape = analyzer.finish();

    // Root is Array; element is Variant with at least the three template arms.
    let element = match &shape.kind {
        ShapeKind::Array { element, .. } => element.as_ref(),
        other => panic!("expected rich Array root, got {other:?}"),
    };
    let arms = match &element.kind {
        ShapeKind::Variant { arms } => arms,
        other => panic!("expected Variant at element, got {other:?}"),
    };
    assert!(
        arms.len() >= 3,
        "expected at least 3 variant arms, got {}\n{report}",
        arms.len(),
    );

    // Each template's discriminator-bearing record should be present
    // somewhere in the variant. Project arms to their field sets.
    let mut arm_field_sets: Vec<Vec<String>> = arms
        .iter()
        .filter_map(|a| match &a.kind {
            ShapeKind::Type(meta_types::value::ValueType::Struct { fields }) => {
                Some(fields.iter().map(|f| f.name.clone()).collect())
            }
            ShapeKind::Record { fields } => Some(fields.iter().map(|f| f.name.clone()).collect()),
            _ => None,
        })
        .collect();
    for set in &mut arm_field_sets {
        set.sort();
    }

    let has_view = arm_field_sets.iter().any(|s| s.contains(&"page".to_string()));
    let has_purchase = arm_field_sets.iter().any(|s| s.contains(&"order".to_string()));
    let has_metric = arm_field_sets
        .iter()
        .any(|s| s.contains(&"metric".to_string()));
    assert!(has_view, "expected page-view arm; arms={arm_field_sets:?}");
    assert!(has_purchase, "expected purchase arm; arms={arm_field_sets:?}");
    assert!(has_metric, "expected metric arm; arms={arm_field_sets:?}");

    // Noise should have produced cluster evictions (it's distributed
    // across many one-off signatures), but the head of the distribution
    // should remain dominant in the report's cluster top-K.
    assert!(
        report.contains("evictions"),
        "report should mention cluster evictions",
    );
    assert!(report.contains("decision: BAG"), "expected BAG decision");
}

#[test]
fn chaos_nested_high_cardinality_keys_pool_via_wildcards() {
    // Documents are `{ uuid_outer: { uuid_inner: bool } }`. Both key
    // levels have high cardinality (unique values per doc and across
    // docs), so each level's record_view should overflow into its own
    // map_value, injecting a wildcard at each level. The leaf bool type
    // must be recovered because all leaf observations pool through the
    // chained map_value accumulators — that's the whole point of
    // wildcard injection: extreme cardinality at intermediate steps
    // does not block analysis at deeper paths.

    let mut rng = StdRng::seed_from_u64(0xBE_EF_CA_FE);
    let mut analyzer = StreamingAnalyzer::new();
    for doc in 0..100 {
        let mut outer = Map::new();
        for _ in 0..20 {
            let mut inner = Map::new();
            for _ in 0..10 {
                let k = random_string(&mut rng, 16..20);
                inner.insert(k, Value::Bool(rng.gen()));
            }
            outer.insert(random_string(&mut rng, 16..20), Value::Object(inner));
        }
        shapez_json::drive_document(&mut analyzer, doc, &Value::Object(outer));
    }
    let report = analyzer.report();
    let shape = analyzer.finish();

    use meta_types::value::ValueType;
    let value_type = match &shape.kind {
        ShapeKind::Type(ValueType::Map { value_type, .. }) => value_type.as_ref(),
        other => panic!("expected outer Map root, got {other:?}"),
    };
    let inner_value_type = match value_type {
        ValueType::Map { value_type, .. } => value_type.as_ref(),
        other => panic!("expected nested Map under wildcard, got {other:?}"),
    };
    assert_eq!(
        *inner_value_type,
        ValueType::Bool,
        "leaf type lost in nested-wildcard pooling: got {inner_value_type:?}",
    );

    // Both levels must surface MAP decisions, and the canonical path
    // pattern must show the double wildcard.
    let map_decisions = report.matches("decision: MAP").count();
    assert_eq!(map_decisions, 2, "expected two MAP decisions, got {map_decisions}\n{report}");
    assert!(
        report.contains(".*.*"),
        "expected `.*.*` canonical pattern in report\n{report}",
    );
}

#[test]
fn chaos_empty_containers_survive() {
    let mut analyzer = StreamingAnalyzer::new();
    for ord in 0..50 {
        shapez_json::drive_document(&mut analyzer, ord, &Value::Array(vec![]));
        shapez_json::drive_document(&mut analyzer, ord + 100, &Value::Object(Map::new()));
    }
    let _report = analyzer.report();
    let shape = analyzer.finish();
    // Root is Variant of [array, struct] given the mix.
    match shape.kind {
        ShapeKind::Variant { arms } => assert_eq!(arms.len(), 2),
        other => panic!("expected variant of array+object, got {other:?}"),
    }
}
