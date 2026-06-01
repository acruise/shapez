//! shapez-gen: curated JSON Schema corpus and value-stream generator.
//!
//! Loads a corpus of JSON Schemas covering analyzer decision dimensions,
//! then emits an infinite stream of conforming `serde_json::Value`s for
//! use as test feedstock against the shapez analyzer.

pub mod corpus;
pub mod generator;
pub mod instantiate;
pub mod manifest;
pub mod schema;
pub mod shape;

pub use corpus::{Corpus, CorpusError};
pub use generator::Generator;
pub use manifest::{Manifest, SchemaEntry};
pub use shape::Shape;
