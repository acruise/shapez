use meta_types::value::ValueType;

use crate::stats::Stats;

#[derive(Clone, Debug)]
pub struct ShapeNode {
    pub kind: ShapeKind,
    pub stats: Stats,
}

/// Identity-bearing shape kind.
///
/// `Type` wraps the closed `ValueType` set; the remaining variants are
/// shape-language-only meta-nodes that ValueType cannot represent on its
/// own. `Variant`, `Tuple`, and `Absent` are always shape-language; the
/// compound variants `Array`, `Record`, and `Map` are emitted instead of
/// `Type(...)` when their children carry shape-language structure (a
/// Variant element, per-field Variants, etc.) that would be lost if
/// flattened to ValueType.
#[derive(Clone, Debug)]
pub enum ShapeKind {
    Type(ValueType),
    Variant { arms: Vec<ShapeNode> },
    Tuple { positions: Vec<ShapeNode> },
    Absent,
    /// Homogeneous array whose element shape is a `ShapeNode` (may itself
    /// be a Variant). Use this in preference to `Type(Array{...})` when
    /// the element carries shape-language structure.
    Array {
        element: Box<ShapeNode>,
        elements_nullable: bool,
    },
    /// Object inferred as a record (stable, low-cardinality key set).
    /// Each field's shape is a full `ShapeNode`. Prefer this over
    /// `Type(Struct{...})` when any field carries shape-language
    /// structure.
    Record { fields: Vec<ShapeField> },
    /// Object inferred as a high-cardinality map. Key and value shapes
    /// are full `ShapeNode`s.
    Map {
        key: Box<ShapeNode>,
        value: Box<ShapeNode>,
        values_nullable: bool,
    },
}

#[derive(Clone, Debug)]
pub struct ShapeField {
    pub name: String,
    pub shape: ShapeNode,
    pub nullable: bool,
}
