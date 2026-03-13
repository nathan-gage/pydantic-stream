use std::collections::HashMap;
use std::sync::Arc;

/// Specification for a single field in a projected JSON object.
#[derive(Debug, Clone)]
pub struct FieldSpec {
    /// The canonical output key.
    pub output_key: Box<str>,
    /// Nested object spec, if this field is a projected sub-object.
    pub nested: Option<Arc<ObjectSpec>>,
}

/// Specification for which fields to keep when projecting a JSON object.
#[derive(Debug, Clone)]
pub struct ObjectSpec {
    /// Lookup from any accepted input key → field spec.
    pub fields: HashMap<Box<str>, FieldSpec>,
}
