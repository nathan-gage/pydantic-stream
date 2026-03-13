use std::sync::Arc;

use ahash::AHashMap;

const HEX: [u8; 16] = *b"0123456789abcdef";

/// Pre-encode `output_key` as a JSON key+colon sequence, e.g. `"name":`.
///
/// Stored once in `FieldSpec` so the hot projection path can use a single
/// `extend_from_slice` instead of calling `write_json_string` per field.
fn encode_json_key(output_key: &str) -> Box<[u8]> {
    let mut bytes = Vec::with_capacity(output_key.len() + 3);
    bytes.push(b'"');
    for byte in output_key.bytes() {
        match byte {
            b'"' => {
                bytes.push(b'\\');
                bytes.push(b'"');
            }
            b'\\' => {
                bytes.push(b'\\');
                bytes.push(b'\\');
            }
            b'\n' => {
                bytes.push(b'\\');
                bytes.push(b'n');
            }
            b'\r' => {
                bytes.push(b'\\');
                bytes.push(b'r');
            }
            b'\t' => {
                bytes.push(b'\\');
                bytes.push(b't');
            }
            b if b < 0x20 => {
                bytes.extend_from_slice(b"\\u00");
                bytes.push(HEX[(b >> 4) as usize]);
                bytes.push(HEX[(b & 0xf) as usize]);
            }
            _ => bytes.push(byte),
        }
    }
    bytes.push(b'"');
    bytes.push(b':');
    bytes.into_boxed_slice()
}

/// Specification for a single field in a projected JSON object.
#[derive(Debug, Clone)]
pub struct FieldSpec {
    /// The canonical output key.
    pub output_key: Box<str>,
    /// Pre-encoded `"output_key":` bytes for fast output writing.
    pub encoded_key: Box<[u8]>,
    /// Nested object spec, if this field is a projected sub-object.
    pub nested: Option<Arc<ObjectSpec>>,
}

impl FieldSpec {
    /// Construct a new `FieldSpec`, pre-computing the encoded key.
    pub fn new(output_key: impl Into<Box<str>>, nested: Option<Arc<ObjectSpec>>) -> Self {
        let output_key: Box<str> = output_key.into();
        let encoded_key = encode_json_key(&output_key);
        Self {
            output_key,
            encoded_key,
            nested,
        }
    }
}

/// Specification for which fields to keep when projecting a JSON object.
#[derive(Debug, Clone)]
pub struct ObjectSpec {
    /// Lookup from any accepted input key → field spec.
    /// Uses [`AHashMap`] for faster lookups compared to [`std::collections::HashMap`].
    pub fields: AHashMap<Box<str>, FieldSpec>,
}

impl ObjectSpec {
    /// Construct from an iterator of `(input_key, FieldSpec)` pairs.
    pub fn from_fields<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = (Box<str>, FieldSpec)>,
    {
        Self {
            fields: iter.into_iter().collect(),
        }
    }
}
