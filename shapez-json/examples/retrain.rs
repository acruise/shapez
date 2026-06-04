//! Demonstrate the general analyze() entry point as a retrain use case:
//! re-analyze any of the `target/eyeball/*.jsonl` files (or any
//! directory of JSONL) under a chosen policy. The same machinery
//! handles first-time ingest — the only difference here is that we
//! override caps and stamp a retrain reason.
//!
//! Usage:
//!   cargo run -p shapez-json --example retrain -- <dir> [<glob-substring>]
//!
//! Examples:
//!   cargo run -p shapez-json --example retrain -- target/eyeball
//!   cargo run -p shapez-json --example retrain -- target/eyeball map_uuid
//!   cargo run -p shapez-json --example retrain -- target/eyeball '' 256
//!     ^ the third arg overrides record_view_cap (default 64) so a retrain
//!       can retain more keys at high-cardinality positions.

use std::env;
use std::process;

use shapez::AnalyzerPolicy;
use shapez::batch::analyze;
use shapez_json::batch::JsonlDir;

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("usage: {} <jsonl-dir> [<path-substring>] [<record_view_cap>]", args[0]);
        process::exit(2);
    }
    let dir = &args[1];
    let needle: Option<&str> = args.get(2).map(|s| s.as_str()).filter(|s| !s.is_empty());
    let record_view_cap: usize = args
        .get(3)
        .and_then(|s| s.parse().ok())
        .unwrap_or(64);

    let needle_owned = needle.map(|s| s.to_string());
    let source = JsonlDir::new(dir).filter(move |p| {
        let is_jsonl = p.extension().map(|e| e == "jsonl").unwrap_or(false);
        let matches_needle = needle_owned
            .as_ref()
            .map(|n| p.to_string_lossy().contains(n))
            .unwrap_or(true);
        is_jsonl && matches_needle
    });

    let policy = AnalyzerPolicy {
        record_view_cap,
        reason: format!(
            "retrain demo: dir={dir}, filter={}, record_view_cap={record_view_cap}",
            needle.unwrap_or("*")
        ),
        ..AnalyzerPolicy::default()
    };

    match analyze(source, policy) {
        Ok(outcome) => {
            println!("=== analyze outcome ===");
            println!("source:  {}", outcome.source);
            println!("docs:    {}", outcome.doc_count);
            println!("policy:  record_view_cap={}, positional_view_cap={}, cluster_cap={}",
                outcome.policy.record_view_cap,
                outcome.policy.positional_view_cap,
                outcome.policy.cluster_cap,
            );
            println!("reason:  {}", outcome.policy.reason);
            println!();
            print!("{}", outcome.summary);
        }
        Err(e) => {
            eprintln!("analyze failed: {e}");
            process::exit(1);
        }
    }
}
