use std::fmt;
use std::fs;
use std::path::Path;

use crate::manifest::Manifest;
use crate::schema::{parse, SchemaError};
use crate::shape::Shape;

#[derive(Debug)]
pub enum CorpusError {
    Io(std::io::Error),
    Json(serde_json::Error),
    Schema(SchemaError),
}

impl fmt::Display for CorpusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CorpusError::Io(e) => write!(f, "io: {}", e),
            CorpusError::Json(e) => write!(f, "json: {}", e),
            CorpusError::Schema(e) => write!(f, "schema: {}", e),
        }
    }
}

impl std::error::Error for CorpusError {}

impl From<std::io::Error> for CorpusError {
    fn from(e: std::io::Error) -> Self { CorpusError::Io(e) }
}
impl From<serde_json::Error> for CorpusError {
    fn from(e: serde_json::Error) -> Self { CorpusError::Json(e) }
}
impl From<SchemaError> for CorpusError {
    fn from(e: SchemaError) -> Self { CorpusError::Schema(e) }
}

pub struct Corpus {
    manifest: Manifest,
    shapes: Vec<Shape>,
}

impl Corpus {
    pub fn load(dir: impl AsRef<Path>) -> Result<Self, CorpusError> {
        let dir = dir.as_ref();
        let manifest_str = fs::read_to_string(dir.join("manifest.json"))?;
        let manifest: Manifest = serde_json::from_str(&manifest_str)?;

        let mut shapes = Vec::with_capacity(manifest.schemas.len());
        for entry in &manifest.schemas {
            let schema_str = fs::read_to_string(dir.join(format!("{}.json", entry.name)))?;
            let schema_json: serde_json::Value = serde_json::from_str(&schema_str)?;
            shapes.push(parse(&schema_json)?);
        }

        Ok(Self { manifest, shapes })
    }

    pub fn shapes(&self) -> &[Shape] {
        &self.shapes
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }
}
