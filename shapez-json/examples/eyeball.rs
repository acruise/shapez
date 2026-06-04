//! Per-corpus-schema: generate N records, dump as JSONL, run them
//! through the analyzer, and print the inferred shape.
//!
//! Usage: `cargo run -p shapez-json --example eyeball [count] [seed]`
//! Output: `target/eyeball/<schema-name>.jsonl` plus a printed shape
//! per schema on stdout.

use std::env;
use std::fs;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use meta_types::value::{StructField, ValueType};
use rand::SeedableRng;
use rand::rngs::StdRng;

use shapez::node::{ShapeField, ShapeKind, ShapeNode};
use shapez::{Analyzer, StreamingAnalyzer};
use shapez_gen::Corpus;
use shapez_gen::instantiate::instantiate;

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

    let entries = &corpus.manifest().schemas;
    let shapes = corpus.shapes();

    for (i, entry) in entries.iter().enumerate() {
        let safe_name = entry.name.replace('/', "__");
        let path = out_dir.join(format!("{safe_name}.jsonl"));
        let file = fs::File::create(&path).expect("create file");
        let mut writer = BufWriter::new(file);

        let mut rng = StdRng::seed_from_u64(seed.wrapping_add(i as u64));
        let mut analyzer = StreamingAnalyzer::new();

        for ord in 0..count {
            let v = instantiate(&shapes[i], &mut rng);
            serde_json::to_writer(&mut writer, &v).expect("write json");
            writer.write_all(b"\n").expect("write newline");
            shapez_json::drive_document(&mut analyzer, ord as u64, &v);
        }
        drop(writer);

        let report = analyzer.report();
        let report_path = out_dir.join(format!("{safe_name}.report.txt"));
        fs::write(&report_path, &report).expect("write report");

        let shape = analyzer.finish();
        println!("=== {} ({} records) ===", entry.name, count);
        println!("jsonl:    {}", path.display());
        println!("report:   {}", report_path.display());
        println!("inferred: {}", format_shape(&shape));
        println!();
    }
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
        ValueType::Enum { values } => {
            out.push_str(&format!("enum[{}]", values.len()))
        }
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
