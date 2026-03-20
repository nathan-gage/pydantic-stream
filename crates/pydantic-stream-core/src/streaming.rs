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

/// Locate the byte offset of the JSON array to stream.
///
/// When `prefix` is empty this expects a top-level array. Otherwise it
/// incrementally navigates through nested object keys and returns the byte
/// offset of the `[` for the target array value.
///
/// Returns `Ok(None)` when more input is needed to reach or confirm the array
/// start. This is the key primitive used by prefix-aware streaming iterators.
pub fn locate_array_start(input: &[u8], prefix: &[&str]) -> Result<Option<usize>, StreamError> {
    let mut jiter = Jiter::new(input);

    if prefix.is_empty() {
        let peek = match jiter.peek() {
            Ok(peek) => peek,
            Err(e) if is_partial_error(&e) => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        if peek != Peek::Array {
            return Err(StreamError::new(format!(
                "Expected a JSON array, got {peek:?}"
            )));
        }

        return Ok(Some(jiter.current_index()));
    }

    for &segment in prefix {
        let peek = match jiter.peek() {
            Ok(peek) => peek,
            Err(e) if is_partial_error(&e) => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        if peek != Peek::Object {
            return Err(StreamError::new(format!(
                "Expected a JSON object at prefix segment {segment:?}, got {peek:?}"
            )));
        }

        let first_key = match jiter.known_object() {
            Ok(first_key) => first_key,
            Err(e) if is_partial_error(&e) => return Ok(None),
            Err(e) => return Err(e.into()),
        };

        let mut found = false;

        if let Some(key) = first_key {
            let key_owned = key.to_string();
            if key_owned == segment {
                found = true;
            } else {
                match jiter.next_skip() {
                    Ok(()) => {}
                    Err(e) if is_partial_error(&e) => return Ok(None),
                    Err(e) => return Err(e.into()),
                }

                loop {
                    match jiter.next_key() {
                        Ok(Some(key)) => {
                            let key_owned = key.to_string();
                            if key_owned == segment {
                                found = true;
                                break;
                            }
                            match jiter.next_skip() {
                                Ok(()) => {}
                                Err(e) if is_partial_error(&e) => return Ok(None),
                                Err(e) => return Err(e.into()),
                            }
                        }
                        Ok(None) => break,
                        Err(e) if is_partial_error(&e) => return Ok(None),
                        Err(e) => return Err(e.into()),
                    }
                }
            }
        }

        if !found {
            return Err(StreamError::new(format!(
                "Prefix key {segment:?} not found in JSON object"
            )));
        }
    }

    let array_start = jiter.current_index();
    let peek = match jiter.peek() {
        Ok(peek) => peek,
        Err(e) if is_partial_error(&e) => return Ok(None),
        Err(e) => return Err(e.into()),
    };

    if peek != Peek::Array {
        return Err(StreamError::new(format!(
            "Expected a JSON array, got {peek:?}"
        )));
    }

    Ok(Some(array_start))
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

    fn span_ranges(items: &[ByteSpan]) -> Vec<(usize, usize)> {
        items.iter().map(|span| (span.start, span.end)).collect()
    }

    fn span_bytes<'a>(input: &'a [u8], items: &[ByteSpan]) -> Vec<&'a [u8]> {
        items
            .iter()
            .map(|span| &input[span.start..span.end])
            .collect()
    }

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
        assert_eq!(span_ranges(&result.items), vec![(1, 8)]);
        assert!(result.finished);
        assert_eq!(result.consumed, input.len());
        assert_eq!(
            span_bytes(input, &result.items),
            vec![b"{\"x\":1}".as_slice()]
        );
    }

    #[test]
    fn extract_multiple_objects() {
        let input = b"[{\"x\":1},{\"x\":2},{\"x\":3}]";
        let result = extract_array_items(input, true).unwrap();
        assert_eq!(span_ranges(&result.items), vec![(1, 8), (9, 16), (17, 24)]);
        assert!(result.finished);
        assert_eq!(result.consumed, input.len());
        assert_eq!(
            span_bytes(input, &result.items),
            vec![
                b"{\"x\":1}".as_slice(),
                b"{\"x\":2}".as_slice(),
                b"{\"x\":3}".as_slice(),
            ]
        );
    }

    #[test]
    fn extract_partial_object() {
        let input = b"[{\"x\":1},{\"x\":";
        let result = extract_array_items(input, true).unwrap();
        assert_eq!(span_ranges(&result.items), vec![(1, 8)]);
        assert!(!result.finished);
        assert_eq!(result.consumed, 9);
        assert_eq!(
            span_bytes(input, &result.items),
            vec![b"{\"x\":1}".as_slice()]
        );
    }

    #[test]
    fn extract_continuation_chunk() {
        // Simulate a continuation (no leading '[')
        let input = b"{\"x\":2},{\"x\":3}]";
        let result = extract_array_items(input, false).unwrap();
        assert_eq!(span_ranges(&result.items), vec![(0, 7), (8, 15)]);
        assert!(result.finished);
        assert_eq!(result.consumed, input.len());
        assert_eq!(
            span_bytes(input, &result.items),
            vec![b"{\"x\":2}".as_slice(), b"{\"x\":3}".as_slice()]
        );
    }

    #[test]
    fn extract_two_chunk_handoff_uses_consumed_offset() {
        let first_chunk = b"[{\"x\":1},{\"x\":2";
        let first = extract_array_items(first_chunk, true).unwrap();
        assert_eq!(span_ranges(&first.items), vec![(1, 8)]);
        assert_eq!(first.consumed, 9);
        assert!(!first.finished);

        let mut continuation = first_chunk[first.consumed..].to_vec();
        continuation.extend_from_slice(br#"},{"x":3}]"#);

        let second = extract_array_items(&continuation, false).unwrap();
        assert_eq!(span_ranges(&second.items), vec![(0, 7), (8, 15)]);
        assert_eq!(second.consumed, continuation.len());
        assert!(second.finished);
        assert_eq!(
            span_bytes(&continuation, &second.items),
            vec![b"{\"x\":2}".as_slice(), b"{\"x\":3}".as_slice()]
        );
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
        assert_eq!(
            span_ranges(&result.items),
            vec![(1, 2), (3, 10), (11, 15), (16, 20)]
        );
        assert!(result.finished);
        assert_eq!(
            span_bytes(input, &result.items),
            vec![
                b"1".as_slice(),
                b"\"hello\"".as_slice(),
                b"true".as_slice(),
                b"null".as_slice(),
            ]
        );
        assert_eq!(result.consumed, input.len());
    }

    #[test]
    fn extract_not_array_error() {
        let err = extract_array_items(b"{\"x\":1}", true).unwrap_err();
        assert!(err.message.contains("Expected '['"));
    }

    #[test]
    fn navigate_to_prefix_positions_parser_at_nested_value() {
        let input = br#"{"meta":0,"outer":{"items":[{"x":1}]}}"#;
        let mut jiter = Jiter::new(input);

        navigate_to_prefix(&mut jiter, &["outer", "items"]).unwrap();

        assert_eq!(jiter.peek().unwrap(), Peek::Array);
        assert_eq!(jiter.known_array().unwrap(), Some(Peek::Object));
    }

    #[test]
    fn navigate_to_prefix_reports_missing_key() {
        let input = br#"{"outer":{"present":[]}}"#;
        let mut jiter = Jiter::new(input);

        let err = navigate_to_prefix(&mut jiter, &["outer", "missing"]).unwrap_err();

        assert!(err.message.contains("Prefix key"));
        assert!(err.message.contains("missing"));
    }

    #[test]
    fn navigate_to_prefix_reports_non_object_segment() {
        let input = br#"{"outer":[1,2,3]}"#;
        let mut jiter = Jiter::new(input);

        let err = navigate_to_prefix(&mut jiter, &["outer", "items"]).unwrap_err();

        assert!(err
            .message
            .contains("Expected a JSON object at prefix segment"));
        assert!(err.message.contains("items"));
    }

    #[test]
    fn locate_array_start_for_top_level_array() {
        let input = br#"  [{"x":1}]"#;
        let start = locate_array_start(input, &[]).unwrap();
        assert_eq!(start, Some(2));
    }

    #[test]
    fn locate_array_start_for_nested_prefix() {
        let input = br#"{"meta":0,"outer":{"items":[{"x":1}]}}"#;
        let start = locate_array_start(input, &["outer", "items"]).unwrap();
        assert_eq!(start, Some(27));
        assert_eq!(input[start.unwrap()], b'[');
    }

    #[test]
    fn locate_array_start_returns_none_for_partial_prefix() {
        let input = br#"{"outer":{"items""#;
        let start = locate_array_start(input, &["outer", "items"]).unwrap();
        assert_eq!(start, None);
    }

    #[test]
    fn locate_array_start_returns_none_for_partial_array_value() {
        let input = br#"{"outer":{"items": "#;
        let start = locate_array_start(input, &["outer", "items"]).unwrap();
        assert_eq!(start, None);
    }

    #[test]
    fn locate_array_start_reports_missing_key() {
        let input = br#"{"outer":{"present":[]}}"#;
        let err = locate_array_start(input, &["outer", "missing"]).unwrap_err();
        assert!(err.message.contains("Prefix key"));
        assert!(err.message.contains("missing"));
    }

    #[test]
    fn locate_array_start_reports_non_array_target() {
        let input = br#"{"outer":{"items":{}}}"#;
        let err = locate_array_start(input, &["outer", "items"]).unwrap_err();
        assert!(err.message.contains("Expected a JSON array"));
    }

    #[test]
    fn trim_ascii_trims_edges_and_all_whitespace() {
        assert_eq!(trim_ascii(b" \n\t{\"x\":1}\r "), b"{\"x\":1}");
        assert_eq!(trim_ascii(b" \n\t\r "), b"");
    }
}
