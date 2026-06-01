//! JSON feedstock adapter for shapez.
//!
//! Lowers `serde_json::Value` into `meta_types::value::Value`, the
//! ingest-side input to the shapez analyzer.

use std::collections::BTreeMap;

use meta_types::value::{MapKey, Value};

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
        // serde_json::Number always represents one of i64, u64, or f64.
        Value::Null
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
}
