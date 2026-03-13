use jiter::{Jiter, JiterError, JiterErrorType, JsonErrorType, Peek};

use crate::error::StreamError;

/// A byte range within an input buffer representing a complete JSON value.
#[derive(Debug, Clone, Copy)]
pub struct ByteSpan {
    pub start: usize,
    pub end: usize,
}

/// Result of extracting complete items from a partial JSON array buffer.
#[derive(Debug)]
pub struct PartialArrayResult {
    /// Byte spans of complete items found in this chunk.
    pub items: Vec<ByteSpan>,
    /// Number of bytes consumed from the input.
    pub consumed: usize,
    /// Whether the closing `]` was reached.
    pub finished: bool,
}

/// Check whether a `JiterError` indicates truncated (partial) input
/// rather than genuinely malformed JSON.
pub(crate) const fn is_partial_error(e: &JiterError) -> bool {
    matches!(
        &e.error_type,
        JiterErrorType::JsonError(
            JsonErrorType::EofWhileParsingList
                | JsonErrorType::EofWhileParsingObject
                | JsonErrorType::EofWhileParsingString
                | JsonErrorType::EofWhileParsingValue
                | JsonErrorType::ExpectedListCommaOrEnd
                | JsonErrorType::ExpectedObjectCommaOrEnd
        )
    )
}

/// Skip ASCII whitespace bytes starting at `*pos`.
#[inline]
fn skip_ws(input: &[u8], pos: &mut usize) {
    while *pos < input.len() && input[*pos].is_ascii_whitespace() {
        *pos += 1;
    }
}

/// Extract complete JSON objects from a (possibly incomplete) JSON array buffer.
///
/// This is the core streaming primitive — it finds complete JSON values
/// without parsing or transforming them. Each returned `ByteSpan` points
/// into the original `input` buffer.
///
/// * `is_start` — `true` for the first chunk (expects leading `[`),
///   `false` for continuation chunks.
pub fn extract_array_items(
    input: &[u8],
    is_start: bool,
) -> Result<PartialArrayResult, StreamError> {
    let mut items = Vec::new();
    let mut pos: usize = 0;
    let len = input.len();

    skip_ws(input, &mut pos);

    if is_start {
        if pos >= len {
            return Ok(PartialArrayResult {
                items,
                consumed: 0,
                finished: false,
            });
        }
        if input[pos] != b'[' {
            return Err(StreamError::new(format!(
                "Expected '[', got {:?}",
                input[pos] as char
            )));
        }
        pos += 1;
    }

    loop {
        skip_ws(input, &mut pos);
        if pos >= len {
            return Ok(PartialArrayResult {
                items,
                consumed: pos,
                finished: false,
            });
        }

        match input[pos] {
            b']' => {
                return Ok(PartialArrayResult {
                    items,
                    consumed: pos + 1,
                    finished: true,
                });
            }
            b',' => {
                pos += 1;
            }
            _ => {
                // Any JSON value — use jiter to check if it's complete
                let slice = &input[pos..];
                let mut scanner = Jiter::new(slice);
                match scanner.next_skip() {
                    Ok(()) => {
                        let val_end = pos + scanner.current_index();
                        items.push(ByteSpan {
                            start: pos,
                            end: val_end,
                        });
                        pos = val_end;
                    }
                    Err(e) if is_partial_error(&e) => {
                        // Value is incomplete — need more data
                        return Ok(PartialArrayResult {
                            items,
                            consumed: pos,
                            finished: false,
                        });
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        }
    }
}

/// Navigate into a nested JSON structure by following key segments.
///
/// For each segment, expects the current position to be at a JSON object,
/// finds the matching key, and leaves the parser positioned at its value.
/// Empty `segments` slice is a no-op.
pub fn navigate_to_prefix(jiter: &mut Jiter<'_>, segments: &[&str]) -> Result<(), StreamError> {
    for &segment in segments {
        let peek = jiter.peek()?;
        if peek != Peek::Object {
            return Err(StreamError::new(format!(
                "Expected a JSON object at prefix segment {segment:?}, got {peek:?}"
            )));
        }

        let first_key = jiter.known_object()?;
        let mut found = false;

        if let Some(key) = first_key {
            let key_owned = key.to_string();
            if key_owned == segment {
                found = true;
            } else {
                jiter.next_skip()?;

                while let Some(key) = jiter.next_key()? {
                    let key_owned = key.to_string();
                    if key_owned == segment {
                        found = true;
                        break;
                    }
                    jiter.next_skip()?;
                }
            }
        }

        if !found {
            return Err(StreamError::new(format!(
                "Prefix key {segment:?} not found in JSON object"
            )));
        }
    }
    Ok(())
}

/// Trim ASCII whitespace from both ends of a byte slice.
#[must_use]
pub fn trim_ascii(s: &[u8]) -> &[u8] {
    let start = s
        .iter()
        .position(|&b| !b.is_ascii_whitespace())
        .unwrap_or(s.len());
    let end = s
        .iter()
        .rposition(|&b| !b.is_ascii_whitespace())
        .map_or(start, |p| p + 1);
    &s[start..end]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn extract_empty_array() {
        let result = extract_array_items(b"[]", true).unwrap();
        assert!(result.items.is_empty());
        assert!(result.finished);
        assert_eq!(result.consumed, 2);
    }

    #[test]
    fn extract_single_object() {
        let input = b"[{\"x\":1}]";
        let result = extract_array_items(input, true).unwrap();
        assert_eq!(result.items.len(), 1);
        assert!(result.finished);
        let span = &result.items[0];
        assert_eq!(&input[span.start..span.end], b"{\"x\":1}");
    }

    #[test]
    fn extract_multiple_objects() {
        let input = b"[{\"x\":1},{\"x\":2},{\"x\":3}]";
        let result = extract_array_items(input, true).unwrap();
        assert_eq!(result.items.len(), 3);
        assert!(result.finished);
    }

    #[test]
    fn extract_partial_object() {
        let input = b"[{\"x\":1},{\"x\":";
        let result = extract_array_items(input, true).unwrap();
        assert_eq!(result.items.len(), 1);
        assert!(!result.finished);
        // consumed should be at the start of the incomplete object
        let span = &result.items[0];
        assert_eq!(&input[span.start..span.end], b"{\"x\":1}");
    }

    #[test]
    fn extract_continuation_chunk() {
        // Simulate a continuation (no leading '[')
        let input = b"{\"x\":2},{\"x\":3}]";
        let result = extract_array_items(input, false).unwrap();
        assert_eq!(result.items.len(), 2);
        assert!(result.finished);
    }

    #[test]
    fn extract_empty_buffer() {
        let result = extract_array_items(b"", true).unwrap();
        assert!(result.items.is_empty());
        assert!(!result.finished);
        assert_eq!(result.consumed, 0);
    }

    #[test]
    fn extract_non_object_items() {
        // streaming.rs is type-agnostic — it works with any JSON value
        let input = b"[1,\"hello\",true,null]";
        let result = extract_array_items(input, true).unwrap();
        assert_eq!(result.items.len(), 4);
        assert!(result.finished);
        assert_eq!(&input[result.items[0].start..result.items[0].end], b"1");
        assert_eq!(&input[result.items[1].start..result.items[1].end], b"\"hello\"");
        assert_eq!(&input[result.items[2].start..result.items[2].end], b"true");
        assert_eq!(&input[result.items[3].start..result.items[3].end], b"null");
    }

    #[test]
    fn extract_not_array_error() {
        let err = extract_array_items(b"{\"x\":1}", true).unwrap_err();
        assert!(err.message.contains("Expected '['"));
    }
}
