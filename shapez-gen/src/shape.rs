use serde_json::Value;

/// Internal generation template parsed from a JSON Schema. Distinct from
/// `shapez::ShapeNode`: this drives generation, not inference.
#[derive(Clone, Debug)]
pub enum Shape {
    Null,
    Bool,
    Int,
    Float,
    String(Option<StringFormat>),
    Enum(Vec<Value>),
    Const(Value),
    Variant(Vec<Shape>),
    Array(ArraySpec),
    Object(ObjectSpec),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StringFormat {
    Uuid,
    DateTime,
    Date,
    Email,
    Ipv4,
    Ipv6,
    Url,
}

#[derive(Clone, Debug)]
pub struct ArraySpec {
    pub kind: ArrayKind,
    pub min_len: u32,
    pub max_len: u32,
}

#[derive(Clone, Debug)]
pub enum ArrayKind {
    Uniform(Box<Shape>),
    Tuple(Vec<Shape>),
}

#[derive(Clone, Debug)]
pub enum ObjectSpec {
    Record {
        fields: Vec<FieldSpec>,
    },
    Map {
        key_format: Option<StringFormat>,
        value: Box<Shape>,
        min_size: u32,
        max_size: u32,
    },
}

#[derive(Clone, Debug)]
pub struct FieldSpec {
    pub name: String,
    pub shape: Shape,
    pub required: bool,
}
