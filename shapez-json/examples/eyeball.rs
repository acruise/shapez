//! Per-scenario: generate N records, dump as JSONL, run them through
//! the analyzer, and print the inferred shape plus a one-line summary
//! of what the scenario is exercising. Full report written to disk.
//!
//! Scenarios span a tonal range:
//!   - foundational ("the boring stuff works"): scalar_root_int,
//!     record_stable
//!   - load-bearing ("the actual fun"): record_optional, map_uuid_keys,
//!     tuple_heterogeneous, polymorphic_array_discriminated, plus
//!     nested_wildcards, templated_skew
//!   - chaos ("we survive in a chaotic universe"): random_tree, deep
//!     spines, pathologically wide objects/arrays
//!
//! Usage: `cargo run -p shapez-json --example eyeball [count] [seed]`
//! Output: `target/eyeball/<scenario>.jsonl` + `<scenario>.report.txt`
//! per scenario; a short header per scenario prints to stdout.

use std::env;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use meta_types::value::{StructField, ValueType};
use rand::SeedableRng;
use rand::rngs::StdRng;

use shapez::node::{ShapeField, ShapeKind, ShapeNode};
use shapez::{Analyzer, StreamingAnalyzer};
use shapez_gen::Corpus;
use shapez_gen::instantiate::instantiate;
use shapez_json::chaos;

fn main() {
    let mut args = env::args().skip(1);
    let count: usize = args
        .next()
        .map(|s| s.parse().expect("count must be a positive integer"))
        .unwrap_or(1000);
    let seed: u64 = args
        .next()
        .map(|s| s.parse().expect("seed must be u64"))
        .unwrap_or(0);

    let corpus_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("shapez-gen")
        .join("corpus");
    let corpus = Corpus::load(&corpus_dir).expect("load corpus");

    let out_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("target")
        .join("eyeball");
    fs::create_dir_all(&out_dir).expect("create out dir");

    // --- Corpus scenarios ------------------------------------------------
    // Each schema-driven scenario carries its own `shows` description in
    // the manifest; the runner surfaces it verbatim.
    let entries = &corpus.manifest().schemas;
    let shapes = corpus.shapes();
    for (i, entry) in entries.iter().enumerate() {
        let mut rng = StdRng::seed_from_u64(seed.wrapping_add(i as u64));
        let shape = &shapes[i];
        run_scenario(&out_dir, &entry.name, &entry.shows, count, |_ord| {
            instantiate(shape, &mut rng)
        });
    }

    // --- Chaos / generative scenarios ------------------------------------
    // These have no JSON Schema; the description lives at the call site.

    let mut rng = StdRng::seed_from_u64(seed.wrapping_add(100_000));
    run_scenario(
        &out_dir,
        "random_tree",
        "We survive in a chaotic universe: every node is independently any scalar or any compound, depth-6 budget, branching-8. No structure to find — just confirm the analyzer doesn't panic, OOM, or otherwise lose its mind.",
        count,
        |_ord| chaos::random_value(&mut rng, 6, 8),
    );

    let deep_value = chaos::nested_mixed_spine(150);
    run_scenario(
        &out_dir,
        "deep_mixed_spine_150",
        "A 150-level deep alternating object/array spine. Single shape repeated — verifies no stack overflow at extreme depth and that the report scales to deeply nested trees.",
        count,
        |_ord| deep_value.clone(),
    );

    let mut rng = StdRng::seed_from_u64(seed.wrapping_add(100_001));
    run_scenario(
        &out_dir,
        "wide_object_200_keys",
        "200 fresh keys per doc forces the record_view to drop and the MAP branch to fire at the root. The leaf type must still be recovered through map_value pooling.",
        count,
        |_ord| {
            let mut map = serde_json::Map::new();
            for k in 0..200 {
                map.insert(format!("k{k:03}"), chaos::random_scalar(&mut rng));
            }
            serde_json::Value::Object(map)
        },
    );

    let mut rng = StdRng::seed_from_u64(seed.wrapping_add(100_002));
    run_scenario(
        &out_dir,
        "wide_array_200",
        "200-element arrays per doc with mixed scalar elements. Blows past the positional_view cap (32) and triggers the BAG decision; the cluster sketch should surface a Variant of every scalar kind it saw.",
        count,
        |_ord| {
            let items: Vec<serde_json::Value> =
                (0..200).map(|_| chaos::random_scalar(&mut rng)).collect();
            serde_json::Value::Array(items)
        },
    );

    let mut rng = StdRng::seed_from_u64(seed.wrapping_add(100_004));
    run_scenario(
        &out_dir,
        "nested_wildcards",
        "Two stacked high-cardinality map levels — the canonical wildcard-injection case. Verifies that .*.*.<leaf> still resolves to a typed leaf record, even though no individual outer or inner key sees enough data on its own.",
        count,
        |_ord| {
            let mut outer = serde_json::Map::new();
            for _ in 0..15 {
                let mut inner = serde_json::Map::new();
                for _ in 0..8 {
                    let mut leaf = serde_json::Map::new();
                    leaf.insert(
                        "enabled".into(),
                        serde_json::Value::Bool(rand::Rng::gen::<bool>(&mut rng)),
                    );
                    leaf.insert(
                        "count".into(),
                        serde_json::Value::Number(serde_json::Number::from(
                            rand::Rng::gen_range(&mut rng, 0..1000i64),
                        )),
                    );
                    inner.insert(
                        chaos::random_string(&mut rng, 16..20),
                        serde_json::Value::Object(leaf),
                    );
                }
                outer.insert(
                    chaos::random_string(&mut rng, 16..20),
                    serde_json::Value::Object(inner),
                );
            }
            serde_json::Value::Object(outer)
        },
    );

    // Epoch-shaped numbers: numeric ts fields that look like Unix
    // millisecond timestamps. NumericStats.epoch_guess should flag this.
    let mut rng = StdRng::seed_from_u64(seed.wrapping_add(100_006));
    run_scenario(
        &out_dir,
        "epoch_events",
        "Records with numeric `ts` fields in the epoch-millis range (~2023..~2030). NumericStats should recognise the magnitude window and flag the field as a likely timestamp even though it arrived as a plain integer.",
        count,
        |_ord| {
            let mut root = serde_json::Map::new();
            root.insert(
                "event_id".into(),
                serde_json::Value::Number(serde_json::Number::from(
                    rand::Rng::gen_range(&mut rng, 1..1_000_000_000_i64),
                )),
            );
            root.insert(
                "ts".into(),
                serde_json::Value::Number(serde_json::Number::from(
                    rand::Rng::gen_range(&mut rng, 1_700_000_000_000i64..1_900_000_000_000),
                )),
            );
            root.insert(
                "level".into(),
                serde_json::Value::Number(serde_json::Number::from(
                    rand::Rng::gen_range(&mut rng, 0..4i64),
                )),
            );
            serde_json::Value::Object(root)
        },
    );

    // Structured-but-unrecognized identifiers: keys are formatted like
    // ORD-2024-001234 — no built-in format matches, but the punctuation
    // skeleton `A-9-9` characterizes 100% of them. Splunk's _punct
    // applied to map keys.
    let mut rng = StdRng::seed_from_u64(seed.wrapping_add(100_005));
    run_scenario(
        &out_dir,
        "skeleton_ids",
        "Custom identifier strings (ORD-YYYY-NNNNNN) that don't match any built-in format. The _punct-style skeleton sketch should identify `A-9-9` as the universal pattern, demonstrating that we can characterize syntactic structure at high-cardinality positions even when no known format matches.",
        count,
        |ord| {
            let mut map = serde_json::Map::new();
            for i in 0..30 {
                let key = format!(
                    "ORD-{}-{:06}",
                    2020 + rand::Rng::gen_range(&mut rng, 0..6u32),
                    ord * 30 + i,
                );
                map.insert(
                    key,
                    serde_json::Value::Number(serde_json::Number::from(
                        rand::Rng::gen_range(&mut rng, 0..10_000i64),
                    )),
                );
            }
            serde_json::Value::Object(map)
        },
    );

    let mut rng = StdRng::seed_from_u64(seed.wrapping_add(100_003));
    run_scenario(
        &out_dir,
        "templated_skew",
        "Realistic analytics-event arrays drawn from four templates in a 70/20/8/2 distribution plus 2% pure noise. This is the load-bearing case for the design: wide + deep individual docs, but a low-entropy SHAPE distribution. The Space-Saving cluster sketch must identify the heavy hitters as variant arms and evict noise without displacing them.",
        count,
        |_ord| {
            let mut items = Vec::with_capacity(120);
            for _ in 0..120 {
                let r: f64 = rand::Rng::gen(&mut rng);
                let item = if r < 0.70 {
                    template_view_event(&mut rng)
                } else if r < 0.90 {
                    template_purchase_event(&mut rng)
                } else if r < 0.98 {
                    template_system_metric(&mut rng)
                } else if r < 1.00 - 0.005 {
                    template_auth_failure(&mut rng)
                } else {
                    chaos::random_value(&mut rng, 4, 4)
                };
                items.push(item);
            }
            serde_json::Value::Array(items)
        },
    );
}

// ---------------------------------------------------------------------------
// Analytics-event templates (rich, low-entropy distribution scenario)
// ---------------------------------------------------------------------------

fn template_view_event(rng: &mut StdRng) -> serde_json::Value {
    use rand::Rng;
    let mut profile = serde_json::Map::new();
    profile.insert("tier".into(), serde_json::Value::String(
        ["free", "paid", "trial"][rng.gen_range(0..3)].into(),
    ));
    profile.insert("country".into(), serde_json::Value::String(
        chaos::random_string(rng, 2..3).to_uppercase(),
    ));
    let mut user = serde_json::Map::new();
    user.insert("id".into(), serde_json::Value::String(chaos::random_string(rng, 8..12)));
    user.insert("session".into(), serde_json::Value::String(chaos::random_string(rng, 8..12)));
    user.insert("profile".into(), serde_json::Value::Object(profile));
    let mut page = serde_json::Map::new();
    page.insert("url".into(), serde_json::Value::String(chaos::random_string(rng, 8..20)));
    page.insert("category".into(), serde_json::Value::String(chaos::random_string(rng, 4..10)));
    page.insert("load_ms".into(), serde_json::Value::Number(serde_json::Number::from(rng.gen_range(50..3000i64))));
    let mut root = serde_json::Map::new();
    root.insert("evt".into(), serde_json::Value::String("view".into()));
    root.insert("ts".into(), serde_json::Value::Number(serde_json::Number::from(rng.gen_range(1_700_000_000..1_900_000_000i64))));
    root.insert("user".into(), serde_json::Value::Object(user));
    root.insert("page".into(), serde_json::Value::Object(page));
    serde_json::Value::Object(root)
}

fn template_purchase_event(rng: &mut StdRng) -> serde_json::Value {
    use rand::Rng;
    let items: Vec<serde_json::Value> = (0..rng.gen_range(1..5))
        .map(|_| serde_json::Value::String(chaos::random_string(rng, 4..10)))
        .collect();
    let mut order = serde_json::Map::new();
    order.insert("id".into(), serde_json::Value::String(chaos::random_string(rng, 12..16)));
    order.insert("items".into(), serde_json::Value::Array(items));
    order.insert("total_cents".into(), serde_json::Value::Number(serde_json::Number::from(rng.gen_range(100..200_000i64))));
    order.insert("currency".into(), serde_json::Value::String(
        ["USD", "EUR", "GBP", "JPY"][rng.gen_range(0..4)].into(),
    ));
    let mut profile = serde_json::Map::new();
    profile.insert("tier".into(), serde_json::Value::String("paid".into()));
    profile.insert("country".into(), serde_json::Value::String(
        chaos::random_string(rng, 2..3).to_uppercase(),
    ));
    let mut user = serde_json::Map::new();
    user.insert("id".into(), serde_json::Value::String(chaos::random_string(rng, 8..12)));
    user.insert("session".into(), serde_json::Value::String(chaos::random_string(rng, 8..12)));
    user.insert("profile".into(), serde_json::Value::Object(profile));
    let mut root = serde_json::Map::new();
    root.insert("evt".into(), serde_json::Value::String("purchase".into()));
    root.insert("ts".into(), serde_json::Value::Number(serde_json::Number::from(rng.gen_range(1_700_000_000..1_900_000_000i64))));
    root.insert("user".into(), serde_json::Value::Object(user));
    root.insert("order".into(), serde_json::Value::Object(order));
    serde_json::Value::Object(root)
}

fn template_system_metric(rng: &mut StdRng) -> serde_json::Value {
    use rand::Rng;
    let mut tags = serde_json::Map::new();
    tags.insert("service".into(), serde_json::Value::String(chaos::random_string(rng, 4..10)));
    tags.insert("env".into(), serde_json::Value::String(
        ["prod", "staging", "dev"][rng.gen_range(0..3)].into(),
    ));
    let mut metric = serde_json::Map::new();
    metric.insert("name".into(), serde_json::Value::String(chaos::random_string(rng, 6..14)));
    metric.insert(
        "value".into(),
        serde_json::Number::from_f64(rng.gen_range(0.0..100.0))
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
    );
    metric.insert("tags".into(), serde_json::Value::Object(tags));
    let mut host = serde_json::Map::new();
    host.insert("id".into(), serde_json::Value::String(chaos::random_string(rng, 6..10)));
    host.insert("region".into(), serde_json::Value::String(chaos::random_string(rng, 4..8)));
    let mut root = serde_json::Map::new();
    root.insert("evt".into(), serde_json::Value::String("metric".into()));
    root.insert("ts".into(), serde_json::Value::Number(serde_json::Number::from(rng.gen_range(1_700_000_000..1_900_000_000i64))));
    root.insert("host".into(), serde_json::Value::Object(host));
    root.insert("metric".into(), serde_json::Value::Object(metric));
    serde_json::Value::Object(root)
}

fn template_auth_failure(rng: &mut StdRng) -> serde_json::Value {
    use rand::Rng;
    let mut attempt = serde_json::Map::new();
    attempt.insert("ip".into(), serde_json::Value::String(format!(
        "{}.{}.{}.{}",
        rng.gen_range(1..255u32),
        rng.gen_range(0..255u32),
        rng.gen_range(0..255u32),
        rng.gen_range(0..255u32),
    )));
    attempt.insert("ua".into(), serde_json::Value::String(chaos::random_string(rng, 16..32)));
    let mut root = serde_json::Map::new();
    root.insert("evt".into(), serde_json::Value::String("auth_fail".into()));
    root.insert("ts".into(), serde_json::Value::Number(serde_json::Number::from(rng.gen_range(1_700_000_000..1_900_000_000i64))));
    root.insert("reason".into(), serde_json::Value::String(
        ["bad_password", "no_mfa", "rate_limited"][rng.gen_range(0..3)].into(),
    ));
    root.insert("attempt".into(), serde_json::Value::Object(attempt));
    serde_json::Value::Object(root)
}

// ---------------------------------------------------------------------------
// Scenario runner
// ---------------------------------------------------------------------------

fn run_scenario<F>(out_dir: &Path, name: &str, shows: &str, count: usize, mut next_doc: F)
where
    F: FnMut(u64) -> serde_json::Value,
{
    let jsonl_path = out_dir.join(format!("{name}.jsonl"));
    let report_path = out_dir.join(format!("{name}.report.txt"));

    let file = fs::File::create(&jsonl_path).expect("create jsonl");
    let mut writer = BufWriter::new(file);
    let mut analyzer = StreamingAnalyzer::new();

    for ord in 0..count {
        let v = next_doc(ord as u64);
        serde_json::to_writer(&mut writer, &v).expect("write json");
        writer.write_all(b"\n").expect("write newline");
        shapez_json::drive_document(&mut analyzer, ord as u64, &v);
    }
    drop(writer);

    let analyzer_report = analyzer.report();
    let report_body = if shows.is_empty() {
        analyzer_report
    } else {
        format!(
            "# scenario: {name}\n\n{shows}\n\n---\n\n{analyzer_report}"
        )
    };
    fs::write(&report_path, &report_body).expect("write report");

    let shape = analyzer.finish();
    let mut rendered = format_shape(&shape);
    let truncated = rendered.len() > 300;
    if truncated {
        rendered.truncate(297);
        rendered.push_str("...");
    }

    println!("=== {name} ({count} records) ===");
    if !shows.is_empty() {
        for line in wrap(shows, 76) {
            println!("  {line}");
        }
    }
    println!("  jsonl:    {}", jsonl_path.display());
    println!("  report:   {}", report_path.display());
    if truncated {
        println!("  inferred: {rendered}  (truncated; full shape in report)");
    } else {
        println!("  inferred: {rendered}");
    }
    println!();
}

/// Hard-wrap a single-line description for terminal output. Breaks on
/// word boundaries; lines stay under `width` columns. Cheap and good
/// enough for human-readable headers.
fn wrap(s: &str, width: usize) -> Vec<String> {
    let mut lines = Vec::new();
    let mut current = String::new();
    for word in s.split_whitespace() {
        if !current.is_empty() && current.len() + 1 + word.len() > width {
            lines.push(std::mem::take(&mut current));
        }
        if !current.is_empty() {
            current.push(' ');
        }
        current.push_str(word);
    }
    if !current.is_empty() {
        lines.push(current);
    }
    lines
}

// ---------------------------------------------------------------------------
// Compact shape pretty-printer
// ---------------------------------------------------------------------------

fn format_shape(s: &ShapeNode) -> String {
    let mut out = String::new();
    write_shape(&mut out, s);
    out
}

fn write_shape(out: &mut String, s: &ShapeNode) {
    match &s.kind {
        ShapeKind::Absent => out.push_str("absent"),
        ShapeKind::Type(vt) => write_vt(out, vt),
        ShapeKind::Tuple { positions } => {
            out.push('(');
            for (i, p) in positions.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_shape(out, p);
            }
            out.push(')');
        }
        ShapeKind::Variant { arms } => {
            out.push('(');
            for (i, a) in arms.iter().enumerate() {
                if i > 0 {
                    out.push_str(" | ");
                }
                write_shape(out, a);
            }
            out.push(')');
        }
        ShapeKind::Array { element, elements_nullable } => {
            out.push('[');
            write_shape(out, element);
            if *elements_nullable {
                out.push('?');
            }
            out.push(']');
        }
        ShapeKind::Record { fields } => write_record_fields(out, fields),
        ShapeKind::Map { key, value, values_nullable } => {
            out.push_str("map<");
            write_shape(out, key);
            out.push_str(", ");
            write_shape(out, value);
            if *values_nullable {
                out.push('?');
            }
            out.push('>');
        }
    }
}

fn write_record_fields(out: &mut String, fields: &[ShapeField]) {
    out.push_str("{ ");
    for (i, f) in fields.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(&f.name);
        out.push_str(": ");
        write_shape(out, &f.shape);
        if f.nullable {
            out.push('?');
        }
    }
    out.push_str(" }");
}

fn write_vt(out: &mut String, vt: &ValueType) {
    match vt {
        ValueType::Null => out.push_str("null"),
        ValueType::Bool => out.push_str("bool"),
        ValueType::I8 => out.push_str("i8"),
        ValueType::I16 => out.push_str("i16"),
        ValueType::I32 => out.push_str("i32"),
        ValueType::I64 => out.push_str("i64"),
        ValueType::U8 => out.push_str("u8"),
        ValueType::U16 => out.push_str("u16"),
        ValueType::U32 => out.push_str("u32"),
        ValueType::U64 => out.push_str("u64"),
        ValueType::F32 => out.push_str("f32"),
        ValueType::F64 => out.push_str("f64"),
        ValueType::Date => out.push_str("date"),
        ValueType::Uuid => out.push_str("uuid"),
        ValueType::Ipv4 => out.push_str("ipv4"),
        ValueType::Ipv6 => out.push_str("ipv6"),
        ValueType::Blob => out.push_str("blob"),
        ValueType::Clob => out.push_str("clob"),
        ValueType::String => out.push_str("string"),
        ValueType::Decimal { precision, scale } => {
            out.push_str(&format!("decimal({precision},{scale})"))
        }
        ValueType::Timestamp { precision, timezone } => {
            out.push_str(&format!("timestamp({precision:?},{timezone:?})"))
        }
        ValueType::Enum { values } => out.push_str(&format!("enum[{}]", values.len())),
        ValueType::Array { element_type, elements_nullable } => {
            out.push('[');
            write_vt(out, element_type);
            if *elements_nullable {
                out.push('?');
            }
            out.push(']');
        }
        ValueType::Map { key_type, value_type, values_nullable } => {
            out.push_str("map<");
            write_vt(out, key_type);
            out.push_str(", ");
            write_vt(out, value_type);
            if *values_nullable {
                out.push('?');
            }
            out.push('>');
        }
        ValueType::Struct { fields } => write_struct(out, fields),
        ValueType::EntityRef { target_type_id, .. } => {
            out.push_str(&format!("entity_ref<{target_type_id}>"))
        }
    }
}

fn write_struct(out: &mut String, fields: &[StructField]) {
    out.push_str("{ ");
    for (i, f) in fields.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        out.push_str(&f.name);
        out.push_str(": ");
        write_vt(out, &f.value_type);
        if f.nullable {
            out.push('?');
        }
    }
    out.push_str(" }");
}
