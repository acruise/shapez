use std::collections::HashSet;
use std::fmt;

use serde_json::Value;

use crate::shape::*;

#[derive(Debug)]
pub enum SchemaError {
    InvalidType(String),
    Missing(&'static str),
    Other(String),
}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaError::InvalidType(s) => write!(f, "invalid type: {}", s),
            SchemaError::Missing(s) => write!(f, "missing key: {}", s),
            SchemaError::Other(s) => write!(f, "{}", s),
        }
    }
}

impl std::error::Error for SchemaError {}

pub fn parse(json: &Value) -> Result<Shape, SchemaError> {
    if let Some(Value::Array(arms)) = json.get("oneOf").or_else(|| json.get("anyOf")) {
        let arms: Result<Vec<_>, _> = arms.iter().map(parse).collect();
        return Ok(Shape::Variant(arms?));
    }

    if let Some(Value::Array(values)) = json.get("enum") {
        return Ok(Shape::Enum(values.clone()));
    }

    if let Some(value) = json.get("const") {
        return Ok(Shape::Const(value.clone()));
    }

    let type_str = json
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or(SchemaError::Missing("type"))?;

    match type_str {
        "null" => Ok(Shape::Null),
        "boolean" => Ok(Shape::Bool),
        "integer" => Ok(Shape::Int),
        "number" => Ok(Shape::Float),
        "string" => Ok(Shape::String(
            json.get("format").and_then(|v| v.as_str()).and_then(parse_format),
        )),
        "array" => parse_array(json),
        "object" => parse_object(json),
        other => Err(SchemaError::InvalidType(other.into())),
    }
}

fn parse_format(s: &str) -> Option<StringFormat> {
    match s {
        "uuid" => Some(StringFormat::Uuid),
        "date-time" => Some(StringFormat::DateTime),
        "date" => Some(StringFormat::Date),
        "email" => Some(StringFormat::Email),
        "ipv4" => Some(StringFormat::Ipv4),
        "ipv6" => Some(StringFormat::Ipv6),
        "uri" | "url" => Some(StringFormat::Url),
        _ => None,
    }
}

fn parse_array(json: &Value) -> Result<Shape, SchemaError> {
    let min_len = json.get("minItems").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
    let max_len = json.get("maxItems").and_then(|v| v.as_u64()).unwrap_or(5) as u32;

    if let Some(Value::Array(prefix)) = json.get("prefixItems") {
        let positions: Result<Vec<_>, _> = prefix.iter().map(parse).collect();
        let positions = positions?;
        let prefix_len = positions.len() as u32;
        return Ok(Shape::Array(ArraySpec {
            kind: ArrayKind::Tuple(positions),
            min_len: min_len.max(prefix_len),
            max_len: max_len.max(prefix_len),
        }));
    }

    let items = json.get("items").ok_or(SchemaError::Missing("items"))?;
    let element = parse(items)?;
    Ok(Shape::Array(ArraySpec {
        kind: ArrayKind::Uniform(Box::new(element)),
        min_len,
        max_len,
    }))
}

fn parse_object(json: &Value) -> Result<Shape, SchemaError> {
    let properties = json.get("properties").and_then(|v| v.as_object());
    let additional = json.get("additionalProperties");

    let is_map =
        properties.map_or(true, |p| p.is_empty()) && additional.is_some_and(|v| v.is_object());

    if is_map {
        let additional = additional.unwrap();
        let value_shape = parse(additional)?;
        let key_format = json
            .get("propertyNames")
            .and_then(|v| v.get("format"))
            .and_then(|v| v.as_str())
            .and_then(parse_format);
        let min_size = json.get("minProperties").and_then(|v| v.as_u64()).unwrap_or(2) as u32;
        let max_size = json.get("maxProperties").and_then(|v| v.as_u64()).unwrap_or(6) as u32;
        return Ok(Shape::Object(ObjectSpec::Map {
            key_format,
            value: Box::new(value_shape),
            min_size,
            max_size,
        }));
    }

    let required: HashSet<String> = json
        .get("required")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();

    let mut fields = Vec::new();
    if let Some(props) = properties {
        for (name, prop_schema) in props {
            let shape = parse(prop_schema)?;
            fields.push(FieldSpec {
                name: name.clone(),
                shape,
                required: required.contains(name),
            });
        }
    }

    Ok(Shape::Object(ObjectSpec::Record { fields }))
}
