//! JSON feedstock adapter for shapez.
//!
//! Two entry points:
//!
//! - `drive_document` — walks a `serde_json::Value` and emits SAX-style
//!   events into any `JsonEventSink`. This is the primary ingest path:
//!   the analyzer never sees `serde_json` types.
//!
//! - `lower` — materializes a `serde_json::Value` as a
//!   `meta_types::value::Value`. Reserved for the exception-exemplar
//!   path, where a violating document needs to be captured as a typed
//!   value for downstream serialization.

pub mod chaos;
pub mod batch;

use std::collections::BTreeMap;

use meta_types::value::{MapKey, Value};
use shapez::ingest::JsonEventSink;

/// Drive a single document into a `JsonEventSink`. Brackets the walk
/// with `document_begin(doc_ordinal)` / `document_end()`.
pub fn drive_document<S: JsonEventSink>(
    sink: &mut S,
    doc_ordinal: u64,
    value: &serde_json::Value,
) {
    sink.document_begin(doc_ordinal);
    drive_value(sink, value);
    sink.document_end();
}

fn drive_value<S: JsonEventSink>(sink: &mut S, value: &serde_json::Value) {
    match value {
        serde_json::Value::Null => sink.null(),
        serde_json::Value::Bool(b) => sink.bool(*b),
        serde_json::Value::Number(n) => drive_number(sink, n),
        serde_json::Value::String(s) => sink.string(s),
        serde_json::Value::Array(items) => {
            sink.array_begin();
            for item in items {
                drive_value(sink, item);
            }
            sink.array_end();
        }
        serde_json::Value::Object(map) => {
            sink.object_begin();
            for (k, v) in map {
                sink.object_key(k);
                drive_value(sink, v);
            }
            sink.object_end();
        }
    }
}

fn drive_number<S: JsonEventSink>(sink: &mut S, n: &serde_json::Number) {
    if let Some(i) = n.as_i64() {
        sink.i64(i);
    } else if let Some(u) = n.as_u64() {
        sink.u64(u);
    } else if let Some(f) = n.as_f64() {
        sink.f64(f);
    } else {
        // serde_json::Number always represents one of i64, u64, f64.
        sink.null();
    }
}

pub fn lower(json: &serde_json::Value) -> Value {
    match json {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => lower_number(n),
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(items) => {
            Value::Array(items.iter().map(lower).collect())
        }
        serde_json::Value::Object(map) => {
            let mut out = BTreeMap::new();
            for (k, v) in map {
                out.insert(MapKey::String(k.clone()), lower(v));
            }
            Value::Map(out)
        }
    }
}

fn lower_number(n: &serde_json::Number) -> Value {
    if let Some(i) = n.as_i64() {
        Value::I64(i)
    } else if let Some(u) = n.as_u64() {
        Value::U64(u)
    } else if let Some(f) = n.as_f64() {
        Value::F64(f)
    } else {
        Value::Null
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // --- lower() tests ---

    #[test]
    fn lower_null() {
        assert_eq!(lower(&json!(null)), Value::Null);
    }

    #[test]
    fn lower_bool() {
        assert_eq!(lower(&json!(true)), Value::Bool(true));
        assert_eq!(lower(&json!(false)), Value::Bool(false));
    }

    #[test]
    fn lower_signed_integer() {
        assert_eq!(lower(&json!(-42)), Value::I64(-42));
        assert_eq!(lower(&json!(0)), Value::I64(0));
        assert_eq!(lower(&json!(i64::MAX)), Value::I64(i64::MAX));
    }

    #[test]
    fn lower_large_unsigned_uses_u64() {
        let big = (i64::MAX as u64) + 1;
        let v = serde_json::Value::Number(serde_json::Number::from(big));
        assert_eq!(lower(&v), Value::U64(big));
    }

    #[test]
    fn lower_float() {
        assert_eq!(lower(&json!(1.5)), Value::F64(1.5));
        assert_eq!(lower(&json!(-2.25)), Value::F64(-2.25));
    }

    #[test]
    fn lower_string_preserves_unicode() {
        assert_eq!(lower(&json!("")), Value::String(String::new()));
        assert_eq!(lower(&json!("hello")), Value::String("hello".into()));
        assert_eq!(
            lower(&json!("\u{1f600}\u{1f44d}")),
            Value::String("\u{1f600}\u{1f44d}".into()),
        );
    }

    #[test]
    fn lower_empty_array() {
        assert_eq!(lower(&json!([])), Value::Array(vec![]));
    }

    #[test]
    fn lower_nested_array() {
        let v = json!([1, [2, 3], "x"]);
        let expected = Value::Array(vec![
            Value::I64(1),
            Value::Array(vec![Value::I64(2), Value::I64(3)]),
            Value::String("x".into()),
        ]);
        assert_eq!(lower(&v), expected);
    }

    #[test]
    fn lower_empty_object() {
        assert_eq!(lower(&json!({})), Value::Map(BTreeMap::new()));
    }

    #[test]
    fn lower_object_keys_become_mapkey_string() {
        let v = json!({"a": 1, "b": "x"});
        let mut expected = BTreeMap::new();
        expected.insert(MapKey::String("a".into()), Value::I64(1));
        expected.insert(MapKey::String("b".into()), Value::String("x".into()));
        assert_eq!(lower(&v), Value::Map(expected));
    }

    #[test]
    fn lower_nested_object() {
        let v = json!({"outer": {"inner": [1, 2]}});
        let mut inner = BTreeMap::new();
        inner.insert(
            MapKey::String("inner".into()),
            Value::Array(vec![Value::I64(1), Value::I64(2)]),
        );
        let mut outer = BTreeMap::new();
        outer.insert(MapKey::String("outer".into()), Value::Map(inner));
        assert_eq!(lower(&v), Value::Map(outer));
    }

    // --- drive_document() tests ---

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Event {
        DocBegin(u64),
        DocEnd,
        Null,
        Bool(bool),
        I64(i64),
        U64(u64),
        F64Bits(u64),
        String(String),
        ArrBegin,
        ArrEnd,
        ObjBegin,
        Key(String),
        ObjEnd,
    }

    #[derive(Default)]
    struct Recorder {
        events: Vec<Event>,
    }

    impl JsonEventSink for Recorder {
        fn document_begin(&mut self, doc_ordinal: u64) { self.events.push(Event::DocBegin(doc_ordinal)); }
        fn document_end(&mut self) { self.events.push(Event::DocEnd); }
        fn null(&mut self) { self.events.push(Event::Null); }
        fn bool(&mut self, v: bool) { self.events.push(Event::Bool(v)); }
        fn i64(&mut self, v: i64) { self.events.push(Event::I64(v)); }
        fn u64(&mut self, v: u64) { self.events.push(Event::U64(v)); }
        fn f64(&mut self, v: f64) { self.events.push(Event::F64Bits(v.to_bits())); }
        fn string(&mut self, s: &str) { self.events.push(Event::String(s.into())); }
        fn array_begin(&mut self) { self.events.push(Event::ArrBegin); }
        fn array_end(&mut self) { self.events.push(Event::ArrEnd); }
        fn object_begin(&mut self) { self.events.push(Event::ObjBegin); }
        fn object_key(&mut self, key: &str) { self.events.push(Event::Key(key.into())); }
        fn object_end(&mut self) { self.events.push(Event::ObjEnd); }
    }

    fn record(value: serde_json::Value) -> Vec<Event> {
        let mut r = Recorder::default();
        drive_document(&mut r, 0, &value);
        r.events
    }

    #[test]
    fn drive_scalar_root() {
        assert_eq!(
            record(json!(42)),
            vec![Event::DocBegin(0), Event::I64(42), Event::DocEnd],
        );
        assert_eq!(
            record(json!(null)),
            vec![Event::DocBegin(0), Event::Null, Event::DocEnd],
        );
        assert_eq!(
            record(json!("hi")),
            vec![Event::DocBegin(0), Event::String("hi".into()), Event::DocEnd],
        );
    }

    #[test]
    fn drive_number_split() {
        let big = serde_json::Value::Number(serde_json::Number::from((i64::MAX as u64) + 1));
        assert_eq!(
            record(big),
            vec![Event::DocBegin(0), Event::U64((i64::MAX as u64) + 1), Event::DocEnd],
        );
        assert_eq!(
            record(json!(1.5)),
            vec![Event::DocBegin(0), Event::F64Bits(1.5_f64.to_bits()), Event::DocEnd],
        );
    }

    #[test]
    fn drive_array_events() {
        assert_eq!(
            record(json!([1, "x", true])),
            vec![
                Event::DocBegin(0),
                Event::ArrBegin,
                Event::I64(1),
                Event::String("x".into()),
                Event::Bool(true),
                Event::ArrEnd,
                Event::DocEnd,
            ],
        );
    }

    #[test]
    fn drive_object_events() {
        assert_eq!(
            record(json!({"a": 1, "b": null})),
            vec![
                Event::DocBegin(0),
                Event::ObjBegin,
                Event::Key("a".into()),
                Event::I64(1),
                Event::Key("b".into()),
                Event::Null,
                Event::ObjEnd,
                Event::DocEnd,
            ],
        );
    }

    #[test]
    fn drive_nested() {
        let v = json!({"xs": [1, [2, 3]], "y": {"z": "ok"}});
        let evs = record(v);
        // Spot-check the key sequence and brackets balance.
        let opens = evs.iter().filter(|e| matches!(e, Event::ArrBegin | Event::ObjBegin)).count();
        let closes = evs.iter().filter(|e| matches!(e, Event::ArrEnd | Event::ObjEnd)).count();
        assert_eq!(opens, closes);
        assert!(evs.contains(&Event::Key("xs".into())));
        assert!(evs.contains(&Event::Key("z".into())));
    }

    #[test]
    fn doc_ordinal_propagates() {
        let mut r = Recorder::default();
        drive_document(&mut r, 7, &json!(1));
        drive_document(&mut r, 8, &json!(2));
        assert_eq!(r.events.first(), Some(&Event::DocBegin(7)));
        assert!(r.events.contains(&Event::DocBegin(8)));
    }
}
