use meta_types::value::Value;

use crate::assertions::{AssertionRef, AssertionTarget};
use crate::node::ShapeNode;
use crate::path::{Path, PathPattern};

#[derive(Clone, Debug)]
pub struct ShapeException {
    pub doc_ordinal: u64,
    pub full_path: Path,
    pub low_entropy_prefix: PathPattern,
    pub observed_value: Value,
    pub observed_shape: ShapeNode,
    pub violations: Vec<AssertionViolation>,
}

#[derive(Clone, Debug)]
pub struct AssertionViolation {
    pub assertion_ref: AssertionRef,
    pub mode: ViolationMode,
    pub expected: AssertionTarget,
}

#[derive(Clone, Debug)]
pub enum ViolationMode {
    PerDoc,
    Aggregate { tipped_at_count: u64 },
}

pub trait ExceptionSink {
    fn emit(&mut self, ex: ShapeException);
}
