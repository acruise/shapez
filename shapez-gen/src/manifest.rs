use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    pub schemas: Vec<SchemaEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaEntry {
    pub name: String,
    /// One- or two-sentence natural-language description of what this
    /// schema is exercising. Surfaced in the eyeball runner and report
    /// files so each scenario carries its own narration.
    #[serde(default)]
    pub shows: String,
    pub dimensions: Vec<String>,
}
