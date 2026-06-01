use meta_types::value::Value;

use crate::node::ShapeNode;

pub trait Analyzer {
    fn observe(&mut self, doc_ordinal: u64, value: &Value);
    fn finish(self) -> ShapeNode;
}
