use meta_types::value::ValueType;

use crate::stats::Stats;

#[derive(Clone, Debug)]
pub struct ShapeNode {
    pub kind: ShapeKind,
    pub stats: Stats,
}

/// Identity-bearing shape kind. `Type` wraps the closed `ValueType` set;
/// `Variant`, `Tuple`, and `Absent` are shape-language-only meta-nodes.
/// `Tuple` is produced at report time only -- ingest stores arrays as
/// `Type(Array)` with positional stats on the side.
#[derive(Clone, Debug)]
pub enum ShapeKind {
    Type(ValueType),
    Variant { arms: Vec<ShapeNode> },
    Tuple { positions: Vec<ShapeNode> },
    Absent,
}
