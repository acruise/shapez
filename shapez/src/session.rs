use meta_types::value::Value;

use crate::assertions::AssertionRef;
use crate::exceptions::ShapeException;
use crate::node::ShapeNode;
use crate::path::{Path, PathPattern};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SessionId(pub u64);

/// Equivalence key under which recurring exceptions cluster into one session.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClusterKey {
    pub assertion_refs: Vec<AssertionRef>,
    pub low_entropy_prefix: PathPattern,
}

#[derive(Clone, Debug)]
pub struct ExceptionSession {
    pub session_id: SessionId,
    pub cluster_key: ClusterKey,
    pub observed_shape: ShapeNode,
    pub first_doc_ordinal: u64,
    pub last_doc_ordinal: u64,
    pub occurrence_count: u64,
}

/// Wire-format events emitted by a session-aware sink.
#[derive(Clone, Debug)]
pub enum ExceptionEvent {
    SessionOpen {
        session_id: SessionId,
        full: ShapeException,
        cluster_key: ClusterKey,
    },
    SessionDelta {
        session_id: SessionId,
        doc_ordinal: u64,
        full_path: Path,
        delta: ShapeDelta,
    },
    SessionClose {
        session_id: SessionId,
        reason: CloseReason,
        summary: SessionSummary,
    },
}

#[derive(Clone, Debug)]
pub enum ShapeDelta {
    /// New occurrence fit the session's current shape; carries differing leaves.
    Fit { leaves: Value },
    /// New occurrence grew the session's shape; carries the extension and leaves.
    Grow { extension: ShapeNode, leaves: Value },
}

#[derive(Clone, Debug)]
pub enum CloseReason {
    Evicted,
    StreamEnd,
    ConfigChanged,
}

#[derive(Clone, Debug)]
pub struct SessionSummary {
    pub occurrence_count: u64,
    pub first_doc_ordinal: u64,
    pub last_doc_ordinal: u64,
}
