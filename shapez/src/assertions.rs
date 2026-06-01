use meta_types::value::ValueType;

use crate::node::ShapeNode;
use crate::path::PathPattern;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AssertionRef(pub String);

#[derive(Clone, Debug)]
pub enum AssertionTarget {
    Map,
    Record,
    Tuple,
    List,
    OrderSignificant,
    Bag,
    Type(ValueType),
    VariantArm(Box<ShapeNode>),
}

#[derive(Clone, Debug)]
pub struct Assertion {
    pub id: AssertionRef,
    pub path: PathPattern,
    pub target: AssertionTarget,
    /// Violating docs tolerated before an error is emitted.
    pub tolerance: u32,
}

#[derive(Clone, Debug, Default)]
pub struct AssertionSet {
    pub assertions: Vec<Assertion>,
}
