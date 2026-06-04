//! JSON-shaped `DocumentSource` implementations.
//!
//! Generic batch / ingest plumbing — the trait, the entry point, the
//! time predicate, the outcome and error types — lives in
//! `shapez::batch`. This module provides JSON-shaped sources that
//! produce `serde_json::Value` as the intermediate record form.
//!
//! Architecture:
//!
//! - `RecordDecoder` (trait): format-specific decoder. Each impl knows
//!   how to read records from a byte stream and yield them as
//!   `serde_json::Value`s. Currently only `JsonlDecoder`; future
//!   adapters add `CsvDecoder`, `AvroDecoder`, `ParquetDecoder`, etc.
//!
//! - `FilesystemDir<D: RecordDecoder>`: filesystem walk + path filter
//!   + time predicate + `.at(path)` subtree projection. The
//!   format-agnostic plumbing. Drives any decoder over a directory
//!   tree.
//!
//! - `JsonlDir`: thin newtype over `FilesystemDir<JsonlDecoder>` for
//!   ergonomic construction. Same pattern would produce `CsvDir`,
//!   `AvroDir`, etc.
//!
//! An analogous lifting will arise for Kafka (`MessageDecoder` over
//! topic ranges) and SQL (`RowDecoder` over query cursors) when those
//! sources land — the value-decoding strategy is orthogonal to the
//! source's iteration shape, so it factors out naturally.

use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read};
use std::ops::ControlFlow;
use std::path::PathBuf;

use serde::Deserialize;
use shapez::batch::{BatchError, DocumentSource, TimePredicate};
use shapez::path::{Path, PathStep};
use shapez::StreamingAnalyzer;

use crate::drive_document;

fn boxed(e: serde_json::Error) -> BatchError {
    BatchError::Source(Box::new(e))
}

// ---------------------------------------------------------------------------
// RecordDecoder — format-specific record reader
// ---------------------------------------------------------------------------

/// Decode records from a byte stream into `serde_json::Value`s. One
/// impl per file format. The decoder owns its own buffering strategy
/// (line-by-line for JSONL, row-iteration for CSV, schema-driven for
/// Avro / Parquet, …) and emits each record via the `on_record`
/// callback.
///
/// Returning `ControlFlow::Break(())` from the callback aborts
/// decoding cleanly — used by `FilesystemDir` to honor analyzer
/// bailout signals without per-decoder bookkeeping.
pub trait RecordDecoder {
    fn decode<R, F>(&self, reader: R, on_record: F) -> Result<u64, BatchError>
    where
        R: Read,
        F: FnMut(&serde_json::Value) -> ControlFlow<()>;

    /// Short label used in the audit-trail describe() string, e.g.
    /// `"jsonl"`, `"csv"`, `"avro"`.
    fn label(&self) -> &'static str;
}

/// Line-delimited JSON: one value per non-blank line, parsed with
/// `serde_json`'s recursion limit disabled so arbitrarily-deep input
/// is accepted (the actual stack-overflow guard is the OS thread
/// stack, not serde_json's limit).
pub struct JsonlDecoder;

impl RecordDecoder for JsonlDecoder {
    fn decode<R, F>(&self, reader: R, mut on_record: F) -> Result<u64, BatchError>
    where
        R: Read,
        F: FnMut(&serde_json::Value) -> ControlFlow<()>,
    {
        let buf = BufReader::new(reader);
        let mut count: u64 = 0;
        for line in buf.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let mut de = serde_json::Deserializer::from_str(&line);
            de.disable_recursion_limit();
            let value = serde_json::Value::deserialize(&mut de).map_err(boxed)?;
            count += 1;
            if let ControlFlow::Break(_) = on_record(&value) {
                break;
            }
        }
        Ok(count)
    }

    fn label(&self) -> &'static str {
        "jsonl"
    }
}

// ---------------------------------------------------------------------------
// FilesystemDir — generic filesystem-walking source
// ---------------------------------------------------------------------------

/// Walks a local directory tree, applying a closure-based path filter
/// and a `TimePredicate` (file mtime granularity), then drives each
/// matching file through the supplied `RecordDecoder`. Each decoded
/// value is optionally projected through an `.at(path)` subtree
/// selector before being fed to the analyzer.
///
/// Files and directories are visited in deterministic sorted order.
/// Per-file decode is delegated to the decoder; the surrounding
/// walk + filter + project + bailout-poll loop is identical for
/// every format and lives here.
pub struct FilesystemDir<D: RecordDecoder> {
    root: PathBuf,
    decoder: D,
    filter: Option<Box<dyn Fn(&std::path::Path) -> bool>>,
    time: TimePredicate,
    at: Option<Path>,
}

impl<D: RecordDecoder> FilesystemDir<D> {
    pub fn new(root: impl Into<PathBuf>, decoder: D) -> Self {
        Self {
            root: root.into(),
            decoder,
            filter: None,
            time: TimePredicate::default(),
            at: None,
        }
    }

    /// Restrict to files whose path matches the closure. Callers plug
    /// in `regex::Regex`, glob match, or any predicate without this
    /// crate taking on the dependency.
    pub fn filter(mut self, f: impl Fn(&std::path::Path) -> bool + 'static) -> Self {
        self.filter = Some(Box::new(f));
        self
    }

    /// Restrict to files whose modification time falls inside the
    /// predicate window. File granularity — sources whose backend
    /// offers per-record time data (SQL columns, Parquet row groups,
    /// object metadata) should expose their own `.within(...)` with
    /// the finer-grained semantics.
    pub fn within(mut self, t: TimePredicate) -> Self {
        self.time = t;
        self
    }

    /// Drive a *subtree* of each decoded value into the analyzer.
    /// Documents whose path doesn't resolve are silently skipped.
    pub fn at(mut self, path: Path) -> Self {
        self.at = Some(path);
        self
    }
}

impl<D: RecordDecoder> DocumentSource for FilesystemDir<D> {
    fn describe(&self) -> String {
        let filt = if self.filter.is_some() { " (filtered)" } else { "" };
        let time = match (self.time.start, self.time.end) {
            (None, None) => String::new(),
            _ => format!(" [{}]", self.time.describe()),
        };
        let at = match &self.at {
            Some(p) => format!(" at={p}"),
            None => String::new(),
        };
        format!("{}:{}{filt}{time}{at}", self.decoder.label(), self.root.display())
    }

    fn drive(self, sink: &mut StreamingAnalyzer) -> Result<u64, BatchError> {
        let mut files: Vec<PathBuf> = Vec::new();
        walk(&self.root, &mut files)?;
        files.sort();
        let mut ordinal: u64 = 0;
        'files: for path in &files {
            if let Some(f) = &self.filter {
                if !f(path) {
                    continue;
                }
            }
            if self.time.start.is_some() || self.time.end.is_some() {
                let mtime = fs::metadata(path)?.modified()?;
                if !self.time.contains(mtime) {
                    continue;
                }
            }
            let file = File::open(path)?;
            let at = self.at.as_ref();
            self.decoder.decode(file, |value| {
                let target = match at {
                    Some(p) => match navigate(value, p) {
                        Some(v) => v,
                        None => return ControlFlow::Continue(()),
                    },
                    None => value,
                };
                drive_document(sink, ordinal, target);
                ordinal += 1;
                if sink.should_bail().is_some() {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            })?;
            if sink.should_bail().is_some() {
                break 'files;
            }
        }
        Ok(ordinal)
    }
}

/// Walk into a `serde_json::Value` along a shapez `Path`. Returns None
/// if any step doesn't resolve (missing field, out-of-bounds index,
/// non-matching kind).
fn navigate<'a>(v: &'a serde_json::Value, path: &Path) -> Option<&'a serde_json::Value> {
    let mut cur = v;
    for step in &path.0 {
        cur = match step {
            PathStep::Field(name) => cur.get(name.as_str())?,
            PathStep::Index(i) => cur.get(*i as usize)?,
        };
    }
    Some(cur)
}

fn walk(dir: &std::path::Path, out: &mut Vec<PathBuf>) -> Result<(), BatchError> {
    if !dir.is_dir() {
        return Err(BatchError::Other(format!("not a directory: {}", dir.display())));
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let p = entry.path();
        if p.is_dir() {
            walk(&p, out)?;
        } else {
            out.push(p);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// JsonlDir — ergonomic newtype over FilesystemDir<JsonlDecoder>
// ---------------------------------------------------------------------------

/// Local directory of `.jsonl` files. Thin newtype over
/// `FilesystemDir<JsonlDecoder>` — exists for `JsonlDir::new(path)`
/// readability. Future Csv/Avro/Parquet sources follow the same
/// pattern with their own decoder + newtype.
pub struct JsonlDir(FilesystemDir<JsonlDecoder>);

impl JsonlDir {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self(FilesystemDir::new(root, JsonlDecoder))
    }

    pub fn filter(self, f: impl Fn(&std::path::Path) -> bool + 'static) -> Self {
        Self(self.0.filter(f))
    }

    pub fn within(self, t: TimePredicate) -> Self {
        Self(self.0.within(t))
    }

    pub fn at(self, path: Path) -> Self {
        Self(self.0.at(path))
    }
}

impl DocumentSource for JsonlDir {
    fn describe(&self) -> String {
        self.0.describe()
    }
    fn drive(self, sink: &mut StreamingAnalyzer) -> Result<u64, BatchError> {
        self.0.drive(sink)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use shapez::batch::analyze;
    use shapez::AnalyzerPolicy;
    use std::io::Write;

    fn write_jsonl(dir: &std::path::Path, name: &str, lines: &[&str]) {
        let p = dir.join(name);
        let mut f = File::create(&p).unwrap();
        for line in lines {
            f.write_all(line.as_bytes()).unwrap();
            f.write_all(b"\n").unwrap();
        }
    }

    #[test]
    fn jsonl_dir_drives_all_files() {
        let tmp = std::env::temp_dir().join("shapez_batch_test_jsonl_dir");
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();
        write_jsonl(&tmp, "a.jsonl", &["1", "2", "3"]);
        write_jsonl(&tmp, "b.jsonl", &["4", "5"]);
        let outcome = analyze(JsonlDir::new(&tmp), AnalyzerPolicy::default()).unwrap();
        assert_eq!(outcome.doc_count, 5);
        assert!(outcome.summary.contains("5 documents"));
        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn jsonl_dir_respects_filter() {
        let tmp = std::env::temp_dir().join("shapez_batch_test_filter");
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();
        write_jsonl(&tmp, "wanted.jsonl", &["1", "2"]);
        write_jsonl(&tmp, "skipme.txt", &["999"]);
        let src = JsonlDir::new(&tmp).filter(|p| {
            p.extension().map(|e| e == "jsonl").unwrap_or(false)
        });
        let outcome = analyze(src, AnalyzerPolicy::default()).unwrap();
        assert_eq!(outcome.doc_count, 2);
        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn analyze_bails_on_max_docs() {
        let tmp = std::env::temp_dir().join("shapez_batch_test_bail");
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();
        let lines: Vec<String> = (0..500).map(|i| i.to_string()).collect();
        let refs: Vec<&str> = lines.iter().map(|s| s.as_str()).collect();
        write_jsonl(&tmp, "many.jsonl", &refs);
        let policy = AnalyzerPolicy {
            max_docs: Some(42),
            min_docs_before_bail: 0,
            ..AnalyzerPolicy::default()
        };
        let outcome = analyze(JsonlDir::new(&tmp), policy).unwrap();
        assert_eq!(outcome.doc_count, 42);
        assert_eq!(outcome.bailed.as_deref(), Some("doc count cap reached"));
        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn at_path_targets_subtree_of_each_doc() {
        let tmp = std::env::temp_dir().join("shapez_batch_test_at");
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();
        write_jsonl(
            &tmp,
            "events.jsonl",
            &[
                r#"{"event":"a","payload":{"id":1,"ok":true}}"#,
                r#"{"event":"b","payload":{"id":2,"ok":false}}"#,
                r#"{"event":"c","payload":{"id":3,"ok":true}}"#,
            ],
        );
        let path: Path = ".payload".parse().unwrap();
        let outcome = analyze(JsonlDir::new(&tmp).at(path), AnalyzerPolicy::default()).unwrap();
        assert_eq!(outcome.doc_count, 3);
        use shapez::node::ShapeKind;
        match &outcome.shape.kind {
            ShapeKind::Type(meta_types::value::ValueType::Struct { fields }) => {
                let names: Vec<&str> = fields.iter().map(|f| f.name.as_str()).collect();
                assert_eq!(names, vec!["id", "ok"], "fields={names:?}");
            }
            other => panic!("expected payload struct, got {other:?}"),
        }
        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn at_path_skips_docs_with_no_match() {
        let tmp = std::env::temp_dir().join("shapez_batch_test_at_skip");
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();
        write_jsonl(
            &tmp,
            "mixed.jsonl",
            &[
                r#"{"event":"a","payload":{"id":1}}"#,
                r#"{"event":"b"}"#,
                r#"{"event":"c","payload":{"id":3}}"#,
                r#"{"meta":"unrelated"}"#,
            ],
        );
        let path: Path = ".payload".parse().unwrap();
        let outcome = analyze(JsonlDir::new(&tmp).at(path), AnalyzerPolicy::default()).unwrap();
        assert_eq!(outcome.doc_count, 2);
        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn time_predicate_excludes_files_outside_window() {
        use std::time::{Duration, SystemTime};
        let tmp = std::env::temp_dir().join("shapez_batch_test_time");
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();
        write_jsonl(&tmp, "a.jsonl", &["1"]);
        write_jsonl(&tmp, "b.jsonl", &["2"]);
        let cutoff = SystemTime::now() + Duration::from_secs(60);
        let predicate = TimePredicate { start: Some(cutoff), end: None };
        let outcome =
            analyze(JsonlDir::new(&tmp).within(predicate), AnalyzerPolicy::default()).unwrap();
        assert_eq!(outcome.doc_count, 0);
        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn analyze_carries_policy_and_provenance() {
        let tmp = std::env::temp_dir().join("shapez_batch_test_provenance");
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();
        write_jsonl(&tmp, "x.jsonl", &["1"]);
        let policy = AnalyzerPolicy {
            record_view_cap: 128,
            reason: "investigating widget cardinality alert".into(),
            ..AnalyzerPolicy::default()
        };
        let outcome = analyze(JsonlDir::new(&tmp), policy.clone()).unwrap();
        assert_eq!(outcome.policy.record_view_cap, 128);
        assert_eq!(outcome.policy.reason, policy.reason);
        assert!(outcome.source.starts_with("jsonl:"));
        assert!(outcome.bailed.is_none(), "natural completion should not flag bail");
        let _ = fs::remove_dir_all(&tmp);
    }

    /// Sanity-check that the generic `FilesystemDir` works directly,
    /// without going through the JsonlDir newtype. Same data, same
    /// expected outcome.
    #[test]
    fn filesystem_dir_works_without_newtype() {
        let tmp = std::env::temp_dir().join("shapez_batch_test_fs_direct");
        let _ = fs::remove_dir_all(&tmp);
        fs::create_dir_all(&tmp).unwrap();
        write_jsonl(&tmp, "data.jsonl", &["1", "2", "3"]);
        let src = FilesystemDir::new(&tmp, JsonlDecoder);
        let outcome = analyze(src, AnalyzerPolicy::default()).unwrap();
        assert_eq!(outcome.doc_count, 3);
        let _ = fs::remove_dir_all(&tmp);
    }
}
