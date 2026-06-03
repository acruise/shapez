//! Streaming JSON shape analyzer.
//!
//! Walks `JsonEventSink` events into a flat arena of per-path
//! accumulator nodes; produces a `ShapeNode` tree at `finish()` with
//! record-vs-map and tuple-vs-bag decisions applied.
//!
//! Phase 1 scope: dual-view accumulators (record_view + map_value,
//! positional_view + bag_value), Option A lazy variant emergence at
//! leaves, simple cardinality-based record-vs-map and arity-based
//! tuple-vs-bag heuristics, plus per-array Space-Saving subtree
//! clustering so polymorphic arrays surface as `Array{element:
//! Variant{...}}`. No HLLs yet; cluster sketches are the primary
//! variant signal. Assertions / exception sessions land in later passes.

use std::collections::{BTreeMap, HashMap};
use std::hash::Hash;

use meta_types::value::{StructField, ValueType};

use crate::ingest::{Analyzer, JsonEventSink};
use crate::node::{ShapeField, ShapeKind, ShapeNode};
use crate::stats::Stats;

type NodeId = usize;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Ord, PartialOrd)]
enum ScalarKind {
    Null,
    Bool,
    I64,
    U64,
    F64,
    String,
}

impl ScalarKind {
    fn to_value_type(self) -> ValueType {
        match self {
            ScalarKind::Null => ValueType::Null,
            ScalarKind::Bool => ValueType::Bool,
            ScalarKind::I64 => ValueType::I64,
            ScalarKind::U64 => ValueType::U64,
            ScalarKind::F64 => ValueType::F64,
            ScalarKind::String => ValueType::String,
        }
    }
}

/// Canonicalized structural signature of a single observed value. Used
/// as the key for per-array subtree clustering. Order-canonical: Record
/// fields are sorted by name; Variant arms are sorted by their own
/// derived order.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Ord, PartialOrd)]
enum Sig {
    Scalar(ScalarKind),
    Array(Box<Sig>),
    Record(Vec<(String, Sig)>),
    /// Heterogeneous element/value within a single container. Arms are
    /// deduped and sorted.
    Variant(Vec<Sig>),
    /// Container with no children observed (empty array/object).
    Empty,
}

fn homogenize(sigs: Vec<Sig>) -> Sig {
    if sigs.is_empty() {
        return Sig::Empty;
    }
    let mut uniq: Vec<Sig> = Vec::new();
    for s in sigs {
        if !uniq.contains(&s) {
            uniq.push(s);
        }
    }
    uniq.sort();
    if uniq.len() == 1 {
        uniq.into_iter().next().unwrap()
    } else {
        Sig::Variant(uniq)
    }
}

// ---------------------------------------------------------------------------
// Space-Saving sketch (Metwally-Agrawal-Abbadi). Bounded counters; on
// overflow, evict the lowest counter and replace its key, inheriting the
// evicted counter as the new entry's starting count. Good enough for
// top-K identification when the head of the distribution is heavy.
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct SpaceSaving<K: Eq + Hash + Clone> {
    counters: HashMap<K, u64>,
    cap: usize,
    evictions: u64,
}

impl<K: Eq + Hash + Clone> SpaceSaving<K> {
    fn new(cap: usize) -> Self {
        Self { counters: HashMap::new(), cap, evictions: 0 }
    }

    fn observe(&mut self, key: K) {
        if let Some(c) = self.counters.get_mut(&key) {
            *c += 1;
            return;
        }
        if self.counters.len() < self.cap {
            self.counters.insert(key, 1);
            return;
        }
        let (min_k, min_c) = self
            .counters
            .iter()
            .min_by_key(|(_, v)| *v)
            .map(|(k, v)| (k.clone(), *v))
            .unwrap();
        self.counters.remove(&min_k);
        self.counters.insert(key, min_c + 1);
        self.evictions += 1;
    }

    fn entries(&self) -> Vec<(K, u64)> {
        let mut v: Vec<_> = self.counters.iter().map(|(k, c)| (k.clone(), *c)).collect();
        v.sort_by_key(|(_, c)| std::cmp::Reverse(*c));
        v
    }
}

#[derive(Debug, Default)]
struct Node {
    obs: u64,
    first_doc: u64,
    last_doc: u64,
    scalar_arms: BTreeMap<ScalarKind, u64>,
    object: Option<ObjectAcc>,
    array: Option<ArrayAcc>,
}

#[derive(Debug)]
struct ObjectAcc {
    obs: u64,
    fields: BTreeMap<String, NodeId>,
    field_order: Vec<String>,
    record_alive: bool,
    map_value: NodeId,
    key_count_sum: u64,
}

#[derive(Debug)]
struct ArrayAcc {
    obs: u64,
    positional: Vec<NodeId>,
    positional_alive: bool,
    bag_value: NodeId,
    length_sum: u64,
    min_length: u32,
    max_length: u32,
    length_histogram: BTreeMap<u32, u64>,
    /// Per-element subtree-signature clustering. Cross-document.
    element_cluster: SpaceSaving<Sig>,
}

enum Frame {
    /// Object container. As keys arrive and values complete, child sigs
    /// accumulate here; on object_end we compute this object's overall
    /// Sig::Record.
    Object {
        node: NodeId,
        pending_key: Option<String>,
        children: Vec<(String, Sig)>,
    },
    /// Array container. As elements complete, their sigs feed the array
    /// node's cluster sketch AND accumulate locally so this array's
    /// overall Sig::Array(homogenized) can be reported to its parent.
    Array {
        node: NodeId,
        position: u32,
        children: Vec<Sig>,
    },
}

pub struct StreamingAnalyzer {
    arena: Vec<Node>,
    root: NodeId,
    doc_count: u64,
    current_doc: u64,
    pending: Option<NodeId>,
    stack: Vec<Frame>,
    record_view_cap: usize,
    positional_view_cap: usize,
    cluster_cap: usize,
}

impl StreamingAnalyzer {
    pub fn new() -> Self {
        let mut arena = Vec::new();
        let root = alloc_node(&mut arena);
        Self {
            arena,
            root,
            doc_count: 0,
            current_doc: 0,
            pending: None,
            stack: Vec::new(),
            record_view_cap: 64,
            positional_view_cap: 32,
            cluster_cap: 16,
        }
    }

    pub fn with_caps(record_view_cap: usize, positional_view_cap: usize) -> Self {
        let mut a = Self::new();
        a.record_view_cap = record_view_cap;
        a.positional_view_cap = positional_view_cap;
        a
    }

    pub fn doc_count(&self) -> u64 {
        self.doc_count
    }

    fn alloc(&mut self) -> NodeId {
        alloc_node(&mut self.arena)
    }

    fn obs_at(&mut self, id: NodeId) {
        let n = &mut self.arena[id];
        if n.obs == 0 {
            n.first_doc = self.current_doc;
        }
        n.obs += 1;
        n.last_doc = self.current_doc;
    }

    fn consume_target(&mut self) -> NodeId {
        if let Some(t) = self.pending.take() {
            return t;
        }
        let (arr_node, pos) = {
            let frame = self.stack.last_mut().expect("event outside document");
            match frame {
                Frame::Array { node, position, .. } => {
                    let arr_node = *node;
                    let pos = *position;
                    *position += 1;
                    (arr_node, pos)
                }
                Frame::Object { .. } => {
                    panic!("event inside object without preceding object_key")
                }
            }
        };
        self.array_child(arr_node, pos)
    }

    fn array_child(&mut self, arr_node: NodeId, position: u32) -> NodeId {
        let (bag_value, alive, positional_len, cap) = {
            let arr = self.arena[arr_node]
                .array
                .as_ref()
                .expect("array_child on non-array");
            (
                arr.bag_value,
                arr.positional_alive,
                arr.positional.len(),
                self.positional_view_cap,
            )
        };
        if !alive {
            return bag_value;
        }
        if (position as usize) < positional_len {
            return self.arena[arr_node].array.as_ref().unwrap().positional[position as usize];
        }
        if (position as usize) >= cap {
            self.arena[arr_node].array.as_mut().unwrap().positional_alive = false;
            return bag_value;
        }
        let new_id = self.alloc();
        self.arena[arr_node].array.as_mut().unwrap().positional.push(new_id);
        new_id
    }

    fn ensure_object(&mut self, id: NodeId) {
        if self.arena[id].object.is_none() {
            let map_value = self.alloc();
            self.arena[id].object = Some(ObjectAcc {
                obs: 0,
                fields: BTreeMap::new(),
                field_order: Vec::new(),
                record_alive: true,
                map_value,
                key_count_sum: 0,
            });
        }
    }

    fn ensure_array(&mut self, id: NodeId) {
        if self.arena[id].array.is_none() {
            let bag_value = self.alloc();
            let cluster_cap = self.cluster_cap;
            self.arena[id].array = Some(ArrayAcc {
                obs: 0,
                positional: Vec::new(),
                positional_alive: true,
                bag_value,
                length_sum: 0,
                min_length: u32::MAX,
                max_length: 0,
                length_histogram: BTreeMap::new(),
                element_cluster: SpaceSaving::new(cluster_cap),
            });
        }
    }

    fn object_child_for_key(&mut self, obj_node: NodeId, key: &str) -> NodeId {
        let cap = self.record_view_cap;
        if let Some(&child) = self.arena[obj_node].object.as_ref().unwrap().fields.get(key) {
            return child;
        }
        let (map_value, record_alive, fields_len) = {
            let obj = self.arena[obj_node].object.as_ref().unwrap();
            (obj.map_value, obj.record_alive, obj.fields.len())
        };
        if record_alive && fields_len < cap {
            let new_id = self.alloc();
            let obj = self.arena[obj_node].object.as_mut().unwrap();
            obj.fields.insert(key.to_string(), new_id);
            obj.field_order.push(key.to_string());
            return new_id;
        }
        if record_alive {
            self.arena[obj_node].object.as_mut().unwrap().record_alive = false;
        }
        map_value
    }

    fn observe_scalar(&mut self, kind: ScalarKind) {
        let t = self.consume_target();
        self.obs_at(t);
        *self.arena[t].scalar_arms.entry(kind).or_insert(0) += 1;
        self.emit_child_sig(Sig::Scalar(kind));
    }

    /// A child value has just completed. Inform the parent frame (the
    /// container we're inside): for arrays, push into the element
    /// cluster sketch and accumulate; for objects, attach to the
    /// pending key. If there's no parent frame, the value was a root
    /// document — nothing to report.
    fn emit_child_sig(&mut self, sig: Sig) {
        let frame = match self.stack.last_mut() {
            Some(f) => f,
            None => return,
        };
        match frame {
            Frame::Array { node, children, .. } => {
                let arr_node = *node;
                children.push(sig.clone());
                let arr = self.arena[arr_node].array.as_mut().unwrap();
                arr.element_cluster.observe(sig);
            }
            Frame::Object { pending_key, children, .. } => {
                let key = pending_key.take().expect("object emitted child without preceding key");
                children.push((key, sig));
            }
        }
    }
}

fn alloc_node(arena: &mut Vec<Node>) -> NodeId {
    let id = arena.len();
    arena.push(Node::default());
    id
}

impl Default for StreamingAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonEventSink for StreamingAnalyzer {
    fn document_begin(&mut self, doc_ordinal: u64) {
        self.current_doc = doc_ordinal;
        self.doc_count += 1;
        debug_assert!(self.stack.is_empty());
        debug_assert!(self.pending.is_none());
        self.pending = Some(self.root);
    }

    fn document_end(&mut self) {
        debug_assert!(self.stack.is_empty(), "document ended with open containers");
        self.pending = None;
    }

    fn null(&mut self) { self.observe_scalar(ScalarKind::Null); }
    fn bool(&mut self, _v: bool) { self.observe_scalar(ScalarKind::Bool); }
    fn i64(&mut self, _v: i64) { self.observe_scalar(ScalarKind::I64); }
    fn u64(&mut self, _v: u64) { self.observe_scalar(ScalarKind::U64); }
    fn f64(&mut self, _v: f64) { self.observe_scalar(ScalarKind::F64); }
    fn string(&mut self, _s: &str) { self.observe_scalar(ScalarKind::String); }

    fn array_begin(&mut self) {
        let t = self.consume_target();
        self.obs_at(t);
        self.ensure_array(t);
        self.arena[t].array.as_mut().unwrap().obs += 1;
        self.stack.push(Frame::Array { node: t, position: 0, children: Vec::new() });
    }

    fn array_end(&mut self) {
        let (node, length, children) = match self.stack.pop().expect("array_end without array_begin") {
            Frame::Array { node, position, children } => (node, position, children),
            Frame::Object { .. } => panic!("array_end in object frame"),
        };
        let arr = self.arena[node].array.as_mut().unwrap();
        arr.length_sum += length as u64;
        if length < arr.min_length {
            arr.min_length = length;
        }
        if length > arr.max_length {
            arr.max_length = length;
        }
        *arr.length_histogram.entry(length).or_insert(0) += 1;
        let sig = Sig::Array(Box::new(homogenize(children)));
        self.emit_child_sig(sig);
    }

    fn object_begin(&mut self) {
        let t = self.consume_target();
        self.obs_at(t);
        self.ensure_object(t);
        self.arena[t].object.as_mut().unwrap().obs += 1;
        self.stack.push(Frame::Object { node: t, pending_key: None, children: Vec::new() });
    }

    fn object_key(&mut self, key: &str) {
        let frame = self.stack.last_mut().expect("object_key without object_begin");
        let obj_node = match frame {
            Frame::Object { node, pending_key, .. } => {
                debug_assert!(pending_key.is_none(), "object_key before previous value");
                *pending_key = Some(key.to_string());
                *node
            }
            Frame::Array { .. } => panic!("object_key in array frame"),
        };
        let child = self.object_child_for_key(obj_node, key);
        self.arena[obj_node].object.as_mut().unwrap().key_count_sum += 1;
        self.pending = Some(child);
    }

    fn object_end(&mut self) {
        let (_, mut children) = match self.stack.pop().expect("object_end without object_begin") {
            Frame::Object { node, children, .. } => (node, children),
            Frame::Array { .. } => panic!("object_end in array frame"),
        };
        children.sort_by(|a, b| a.0.cmp(&b.0));
        let sig = Sig::Record(children);
        self.emit_child_sig(sig);
    }
}

impl Analyzer for StreamingAnalyzer {
    fn finish(self) -> ShapeNode {
        let StreamingAnalyzer { arena, root, .. } = self;
        Finalizer { arena }.build(root)
    }
}

// ---------------------------------------------------------------------------
// Finalization
// ---------------------------------------------------------------------------

struct Finalizer {
    arena: Vec<Node>,
}

impl Finalizer {
    fn build(&self, id: NodeId) -> ShapeNode {
        let n = &self.arena[id];
        let mut arms: Vec<ShapeNode> = Vec::new();

        for (kind, count) in &n.scalar_arms {
            if *count == 0 {
                continue;
            }
            arms.push(ShapeNode {
                kind: ShapeKind::Type(kind.to_value_type()),
                stats: stats_with_obs(*count, n),
            });
        }
        if let Some(obj) = &n.object {
            arms.push(self.build_object(obj, n));
        }
        if let Some(arr) = &n.array {
            arms.push(self.build_array(arr, n));
        }

        match arms.len() {
            0 => ShapeNode {
                kind: ShapeKind::Absent,
                stats: stats_from(n),
            },
            1 => arms.into_iter().next().unwrap(),
            _ => ShapeNode {
                kind: ShapeKind::Variant { arms },
                stats: stats_from(n),
            },
        }
    }

    fn build_object(&self, obj: &ObjectAcc, n: &Node) -> ShapeNode {
        let total = obj.obs.max(1);
        let unique = obj.field_order.len();
        let mean_keys = obj.key_count_sum as f64 / total as f64;

        let force_map = !obj.record_alive;
        let map_by_cardinality = obj.record_alive
            && unique >= 16
            && (unique as f64) > mean_keys.max(1.0) * 4.0;

        if force_map || map_by_cardinality {
            let mut value_shape = self.build(obj.map_value);
            if matches!(value_shape.kind, ShapeKind::Absent) {
                let ids: Vec<NodeId> = obj
                    .field_order
                    .iter()
                    .filter_map(|f| obj.fields.get(f).copied())
                    .collect();
                value_shape = self.dominant_child_shape(&ids);
            }
            let values_nullable = contains_null(&value_shape);
            // Prefer the rich Map form when the value carries shape
            // structure that ValueType can't express.
            if shape_is_value_type_only(&value_shape) {
                return ShapeNode {
                    kind: ShapeKind::Type(ValueType::Map {
                        key_type: Box::new(ValueType::String),
                        value_type: Box::new(shape_to_value_type(&value_shape)),
                        values_nullable,
                    }),
                    stats: stats_from(n),
                };
            }
            return ShapeNode {
                kind: ShapeKind::Map {
                    key: Box::new(ShapeNode {
                        kind: ShapeKind::Type(ValueType::String),
                        stats: Stats::default(),
                    }),
                    value: Box::new(value_shape),
                    values_nullable,
                },
                stats: stats_from(n),
            };
        }

        let fields_rich: Vec<ShapeField> = obj
            .field_order
            .iter()
            .map(|name| {
                let cid = obj.fields[name];
                let shape = self.build(cid);
                let child_obs = self.arena[cid].obs;
                let nullable = child_obs < obj.obs || contains_null(&shape);
                ShapeField { name: name.clone(), shape, nullable }
            })
            .collect();

        if fields_rich.iter().all(|f| shape_is_value_type_only(&f.shape)) {
            let fields: Vec<StructField> = fields_rich
                .into_iter()
                .map(|f| StructField {
                    name: f.name,
                    human_name: String::new(),
                    value_type: shape_to_value_type(&f.shape),
                    nullable: f.nullable,
                })
                .collect();
            return ShapeNode {
                kind: ShapeKind::Type(ValueType::Struct { fields }),
                stats: stats_from(n),
            };
        }
        ShapeNode {
            kind: ShapeKind::Record { fields: fields_rich },
            stats: stats_from(n),
        }
    }

    fn build_array(&self, arr: &ArrayAcc, n: &Node) -> ShapeNode {
        let total = arr.obs.max(1);
        let (mode_len, mode_count) = arr
            .length_histogram
            .iter()
            .max_by_key(|(_, c)| **c)
            .map(|(l, c)| (*l, *c))
            .unwrap_or((0, 0));
        let mode_share = mode_count as f64 / total as f64;
        let positional_useful = arr.positional_alive && !arr.positional.is_empty();
        let small_mode = mode_len > 0 && mode_len <= 16;
        let tight = mode_share >= 0.9;

        if positional_useful && small_mode && tight {
            let take = (mode_len as usize).min(arr.positional.len());
            let positions: Vec<ShapeNode> = arr
                .positional
                .iter()
                .take(take)
                .map(|cid| self.build(*cid))
                .collect();
            return ShapeNode {
                kind: ShapeKind::Tuple { positions },
                stats: stats_from(n),
            };
        }

        // Bag path. Build element shape from the cluster sketch when
        // available — the cluster carries per-element variant structure
        // that per-position children lose.
        let element_shape = self.build_array_element_shape(arr);
        let elements_nullable = contains_null(&element_shape);

        if shape_is_value_type_only(&element_shape) {
            return ShapeNode {
                kind: ShapeKind::Type(ValueType::Array {
                    element_type: Box::new(shape_to_value_type(&element_shape)),
                    elements_nullable,
                }),
                stats: stats_from(n),
            };
        }
        ShapeNode {
            kind: ShapeKind::Array { element: Box::new(element_shape), elements_nullable },
            stats: stats_from(n),
        }
    }

    fn build_array_element_shape(&self, arr: &ArrayAcc) -> ShapeNode {
        // Prefer the cluster sketch's view if it has signal. A single
        // dominant signature => one element shape. Multiple => Variant.
        let entries = arr.element_cluster.entries();
        // Discard the empty marker if it ever shows up.
        let entries: Vec<(Sig, u64)> = entries
            .into_iter()
            .filter(|(s, _)| !matches!(s, Sig::Empty))
            .collect();

        if !entries.is_empty() {
            let arms: Vec<ShapeNode> = entries
                .iter()
                .map(|(sig, count)| sig_to_shape(sig, *count))
                .collect();
            if arms.len() == 1 {
                return arms.into_iter().next().unwrap();
            }
            let total: u64 = entries.iter().map(|(_, c)| *c).sum();
            return ShapeNode {
                kind: ShapeKind::Variant { arms },
                stats: Stats {
                    observation_count: total,
                    first_doc_ordinal: 0,
                    last_doc_ordinal: 0,
                    exemplars: Vec::new(),
                },
            };
        }

        // Fallback: bag_value if present, else dominant positional child.
        let bag = self.build(arr.bag_value);
        if !matches!(bag.kind, ShapeKind::Absent) {
            return bag;
        }
        self.dominant_child_shape(&arr.positional)
    }

    fn dominant_child_shape(&self, ids: &[NodeId]) -> ShapeNode {
        if ids.is_empty() {
            return ShapeNode {
                kind: ShapeKind::Absent,
                stats: Stats::default(),
            };
        }
        let mut best = ids[0];
        let mut best_obs = self.arena[best].obs;
        for &id in &ids[1..] {
            if self.arena[id].obs > best_obs {
                best = id;
                best_obs = self.arena[id].obs;
            }
        }
        self.build(best)
    }
}

fn stats_from(n: &Node) -> Stats {
    Stats {
        observation_count: n.obs,
        first_doc_ordinal: n.first_doc,
        last_doc_ordinal: n.last_doc,
        exemplars: Vec::new(),
    }
}

fn stats_with_obs(count: u64, n: &Node) -> Stats {
    Stats {
        observation_count: count,
        first_doc_ordinal: n.first_doc,
        last_doc_ordinal: n.last_doc,
        exemplars: Vec::new(),
    }
}

fn stats_count(count: u64) -> Stats {
    Stats {
        observation_count: count,
        first_doc_ordinal: 0,
        last_doc_ordinal: 0,
        exemplars: Vec::new(),
    }
}

/// True if a ShapeNode can be losslessly represented as a ValueType.
/// Variants, Tuples, Absents, and any rich compound subtree forbid the
/// flat ValueType form.
fn shape_is_value_type_only(s: &ShapeNode) -> bool {
    match &s.kind {
        ShapeKind::Type(_) => true,
        ShapeKind::Variant { .. } | ShapeKind::Tuple { .. } | ShapeKind::Absent => false,
        ShapeKind::Array { element, .. } => shape_is_value_type_only(element),
        ShapeKind::Record { fields } => fields.iter().all(|f| shape_is_value_type_only(&f.shape)),
        ShapeKind::Map { key, value, .. } => {
            shape_is_value_type_only(key) && shape_is_value_type_only(value)
        }
    }
}

fn shape_to_value_type(shape: &ShapeNode) -> ValueType {
    match &shape.kind {
        ShapeKind::Type(vt) => vt.clone(),
        ShapeKind::Variant { arms } => arms
            .iter()
            .filter(|a| !matches!(a.kind, ShapeKind::Type(ValueType::Null) | ShapeKind::Absent))
            .max_by_key(|a| a.stats.observation_count)
            .map(shape_to_value_type)
            .unwrap_or(ValueType::Null),
        ShapeKind::Tuple { positions } => {
            let elem = positions
                .first()
                .map(shape_to_value_type)
                .unwrap_or(ValueType::Null);
            ValueType::Array { element_type: Box::new(elem), elements_nullable: false }
        }
        ShapeKind::Absent => ValueType::Null,
        ShapeKind::Array { element, elements_nullable } => ValueType::Array {
            element_type: Box::new(shape_to_value_type(element)),
            elements_nullable: *elements_nullable,
        },
        ShapeKind::Record { fields } => ValueType::Struct {
            fields: fields
                .iter()
                .map(|f| StructField {
                    name: f.name.clone(),
                    human_name: String::new(),
                    value_type: shape_to_value_type(&f.shape),
                    nullable: f.nullable,
                })
                .collect(),
        },
        ShapeKind::Map { key, value, values_nullable } => ValueType::Map {
            key_type: Box::new(shape_to_value_type(key)),
            value_type: Box::new(shape_to_value_type(value)),
            values_nullable: *values_nullable,
        },
    }
}

fn contains_null(shape: &ShapeNode) -> bool {
    match &shape.kind {
        ShapeKind::Type(ValueType::Null) => true,
        ShapeKind::Variant { arms } => arms.iter().any(contains_null),
        _ => false,
    }
}

/// Build a ShapeNode from a single subtree signature observed by the
/// cluster sketch. Stats carry the observed count for that cluster.
fn sig_to_shape(sig: &Sig, count: u64) -> ShapeNode {
    match sig {
        Sig::Empty => ShapeNode { kind: ShapeKind::Absent, stats: stats_count(count) },
        Sig::Scalar(k) => ShapeNode {
            kind: ShapeKind::Type(k.to_value_type()),
            stats: stats_count(count),
        },
        Sig::Array(elem) => {
            let element = sig_to_shape(elem, count);
            if shape_is_value_type_only(&element) {
                ShapeNode {
                    kind: ShapeKind::Type(ValueType::Array {
                        element_type: Box::new(shape_to_value_type(&element)),
                        elements_nullable: false,
                    }),
                    stats: stats_count(count),
                }
            } else {
                ShapeNode {
                    kind: ShapeKind::Array { element: Box::new(element), elements_nullable: false },
                    stats: stats_count(count),
                }
            }
        }
        Sig::Record(fields) => {
            let f_rich: Vec<ShapeField> = fields
                .iter()
                .map(|(name, s)| ShapeField {
                    name: name.clone(),
                    shape: sig_to_shape(s, count),
                    nullable: false,
                })
                .collect();
            if f_rich.iter().all(|f| shape_is_value_type_only(&f.shape)) {
                ShapeNode {
                    kind: ShapeKind::Type(ValueType::Struct {
                        fields: f_rich
                            .into_iter()
                            .map(|f| StructField {
                                name: f.name,
                                human_name: String::new(),
                                value_type: shape_to_value_type(&f.shape),
                                nullable: f.nullable,
                            })
                            .collect(),
                    }),
                    stats: stats_count(count),
                }
            } else {
                ShapeNode { kind: ShapeKind::Record { fields: f_rich }, stats: stats_count(count) }
            }
        }
        Sig::Variant(arms) => {
            let arm_shapes: Vec<ShapeNode> = arms.iter().map(|s| sig_to_shape(s, count)).collect();
            ShapeNode { kind: ShapeKind::Variant { arms: arm_shapes }, stats: stats_count(count) }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn analyze<F: FnOnce(&mut StreamingAnalyzer)>(f: F) -> ShapeNode {
        let mut a = StreamingAnalyzer::new();
        f(&mut a);
        a.finish()
    }

    #[test]
    fn scalar_root_i64() {
        let s = analyze(|a| {
            for i in 0..10 {
                a.document_begin(i);
                a.i64(i as i64);
                a.document_end();
            }
        });
        match s.kind {
            ShapeKind::Type(ValueType::I64) => (),
            other => panic!("expected I64, got {other:?}"),
        }
        assert_eq!(s.stats.observation_count, 10);
    }

    #[test]
    fn record_two_fields_all_required() {
        let s = analyze(|a| {
            for i in 0..5 {
                a.document_begin(i);
                a.object_begin();
                a.object_key("id");
                a.string("u");
                a.object_key("age");
                a.i64(1);
                a.object_end();
                a.document_end();
            }
        });
        match s.kind {
            ShapeKind::Type(ValueType::Struct { fields }) => {
                assert_eq!(fields.len(), 2);
                let id = fields.iter().find(|f| f.name == "id").unwrap();
                assert_eq!(id.value_type, ValueType::String);
                assert!(!id.nullable);
                let age = fields.iter().find(|f| f.name == "age").unwrap();
                assert_eq!(age.value_type, ValueType::I64);
                assert!(!age.nullable);
            }
            other => panic!("expected Struct, got {other:?}"),
        }
    }

    #[test]
    fn record_optional_field_marked_nullable() {
        let s = analyze(|a| {
            for i in 0..10 {
                a.document_begin(i);
                a.object_begin();
                a.object_key("id");
                a.string("u");
                if i % 2 == 0 {
                    a.object_key("nick");
                    a.string("n");
                }
                a.object_end();
                a.document_end();
            }
        });
        match s.kind {
            ShapeKind::Type(ValueType::Struct { fields }) => {
                let nick = fields.iter().find(|f| f.name == "nick").unwrap();
                assert!(nick.nullable);
                let id = fields.iter().find(|f| f.name == "id").unwrap();
                assert!(!id.nullable);
            }
            other => panic!("expected Struct, got {other:?}"),
        }
    }

    #[test]
    fn map_keys_overflow_to_map() {
        let s = analyze(|a| {
            let mut next = 0;
            for d in 0..50 {
                a.document_begin(d);
                a.object_begin();
                for _ in 0..4 {
                    let key = format!("k{}", next);
                    next += 1;
                    a.object_key(&key);
                    a.bool(true);
                }
                a.object_end();
                a.document_end();
            }
        });
        match s.kind {
            ShapeKind::Type(ValueType::Map { value_type, .. }) => {
                assert_eq!(*value_type, ValueType::Bool);
            }
            other => panic!("expected Map, got {other:?}"),
        }
    }

    #[test]
    fn tuple_detected_when_length_tight() {
        let s = analyze(|a| {
            for i in 0..10 {
                a.document_begin(i);
                a.array_begin();
                a.f64(1.0);
                a.f64(2.0);
                a.f64(3.0);
                a.array_end();
                a.document_end();
            }
        });
        match s.kind {
            ShapeKind::Tuple { positions } => {
                assert_eq!(positions.len(), 3);
                for p in positions {
                    assert!(matches!(p.kind, ShapeKind::Type(ValueType::F64)));
                }
            }
            other => panic!("expected Tuple, got {other:?}"),
        }
    }

    #[test]
    fn array_with_varied_length_is_bag() {
        let s = analyze(|a| {
            for i in 0..20 {
                a.document_begin(i);
                a.array_begin();
                for _ in 0..(2 + i % 4) {
                    a.i64(1);
                }
                a.array_end();
                a.document_end();
            }
        });
        match s.kind {
            ShapeKind::Type(ValueType::Array { element_type, .. }) => {
                assert_eq!(*element_type, ValueType::I64);
            }
            other => panic!("expected Array, got {other:?}"),
        }
    }

    #[test]
    fn variant_at_leaf_for_mixed_scalar_types() {
        let s = analyze(|a| {
            for i in 0..10 {
                a.document_begin(i);
                if i % 2 == 0 {
                    a.i64(1);
                } else {
                    a.string("x");
                }
                a.document_end();
            }
        });
        match s.kind {
            ShapeKind::Variant { arms } => {
                assert_eq!(arms.len(), 2);
                let has_i64 = arms.iter().any(|a| matches!(a.kind, ShapeKind::Type(ValueType::I64)));
                let has_str = arms.iter().any(|a| matches!(a.kind, ShapeKind::Type(ValueType::String)));
                assert!(has_i64 && has_str);
            }
            other => panic!("expected Variant, got {other:?}"),
        }
    }

    #[test]
    fn polymorphic_array_emits_variant_element() {
        let s = analyze(|a| {
            for i in 0..30 {
                a.document_begin(i);
                a.array_begin();
                // Varied length so the tuple heuristic doesn't latch on.
                let n = 2 + (i as u64 % 4);
                for j in 0..n {
                    a.object_begin();
                    a.object_key("type");
                    match (i + j) % 3 {
                        0 => {
                            a.string("post");
                            a.object_key("post_id");
                            a.string("uuid");
                        }
                        1 => {
                            a.string("like");
                            a.object_key("like_id");
                            a.string("uuid");
                        }
                        _ => {
                            a.string("follow");
                            a.object_key("follow_id");
                            a.string("uuid");
                        }
                    }
                    a.object_end();
                }
                a.array_end();
                a.document_end();
            }
        });
        let element = match &s.kind {
            ShapeKind::Array { element, .. } => element.as_ref(),
            other => panic!("expected rich Array, got {other:?}"),
        };
        let arms = match &element.kind {
            ShapeKind::Variant { arms } => arms,
            other => panic!("expected Variant at element, got {other:?}"),
        };
        assert_eq!(arms.len(), 3, "three variant arms expected");
    }
}
