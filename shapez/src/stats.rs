use crate::path::Path;

#[derive(Clone, Debug, Default)]
pub struct Stats {
    pub observation_count: u64,
    pub first_doc_ordinal: u64,
    pub last_doc_ordinal: u64,
    pub exemplars: Vec<Exemplar>,
}

/// A back-pointer into the input stream. Reservoir-sampled, biased toward
/// transitions (first observation of an arm, threshold crossings, etc.).
#[derive(Clone, Debug)]
pub struct Exemplar {
    pub doc_ordinal: u64,
    pub full_path: Path,
}
