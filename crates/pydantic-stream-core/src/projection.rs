use jiter::{Jiter, JiterError, JiterErrorType, JsonErrorType, Peek};
use memchr::memchr2;

use crate::error::StreamError;
use crate::spec::ObjectSpec;
use crate::streaming::{navigate_to_prefix, trim_ascii};

/// Project a single JSON object, returning projected JSON bytes.
pub fn project_object(input: &[u8], spec: &ObjectSpec) -> Result<Vec<u8>, StreamError> {
    let mut jiter = Jiter::new(input);
    let peek = jiter.peek()?;

    if peek != Peek::Object {
        return Err(StreamError::new(format!(
            "Expected a top-level JSON object, got {peek:?}"
        )));
    }

    let output = project_object_from_current::<false, false>(&mut jiter, input, spec)?;
    jiter.finish()?;
    Ok(output)
}

/// Project a JSON array of objects, returning projected JSON bytes.
///
/// Input must be a JSON array `[{...}, {...}, ...]`.
/// Returns a JSON array with only the fields specified by `spec`.
pub fn project_array(input: &[u8], spec: &ObjectSpec) -> Result<Vec<u8>, StreamError> {
    let mut jiter = Jiter::new(input);
    let peek = jiter.peek()?;

    if peek != Peek::Array {
        return Err(StreamError::new(format!(
            "Expected a top-level JSON array, got {peek:?}"
        )));
    }

    let first = jiter.known_array()?;

    let mut output = Vec::with_capacity((input.len() / 10).min(65_536));
    output.push(b'[');

    if let Some(peek) = first {
        if peek != Peek::Object {
            return Err(StreamError::new(format!(
                "Expected array items to be JSON objects, got {peek:?}"
            )));
        }
        project_object_inner::<true, true>(&mut jiter, input, spec, &mut output)?;

        while let Some(peek) = jiter.array_step()? {
            if peek != Peek::Object {
                return Err(StreamError::new(format!(
                    "Expected array items to be JSON objects, got {peek:?}"
                )));
            }
            output.push(b',');
            project_object_inner::<true, true>(&mut jiter, input, spec, &mut output)?;
        }
    }

    output.push(b']');
    jiter.finish()?;
    Ok(output)
}

/// Project a JSON array of objects, returning one `Vec<u8>` per item.
pub fn project_array_items(input: &[u8], spec: &ObjectSpec) -> Result<Vec<Vec<u8>>, StreamError> {
    let mut jiter = Jiter::new(input);
    let peek = jiter.peek()?;

    if peek != Peek::Array {
        return Err(StreamError::new(format!(
            "Expected a top-level JSON array, got {peek:?}"
        )));
    }

    let first = jiter.known_array()?;
    let mut results = Vec::new();

    if let Some(peek) = first {
        if peek != Peek::Object {
            return Err(StreamError::new(format!(
                "Expected array items to be JSON objects, got {peek:?}"
            )));
        }
        let mut output = Vec::with_capacity(256);
        project_object_inner::<true, true>(&mut jiter, input, spec, &mut output)?;
        results.push(output);

        while let Some(peek) = jiter.array_step()? {
            if peek != Peek::Object {
                return Err(StreamError::new(format!(
                    "Expected array items to be JSON objects, got {peek:?}"
                )));
            }
            let mut output = Vec::with_capacity(256);
            project_object_inner::<true, true>(&mut jiter, input, spec, &mut output)?;
            results.push(output);
        }
    }

    jiter.finish()?;
    Ok(results)
}

/// Project JSONL (newline-delimited JSON) input.
/// Returns a Vec of projected JSON byte strings, one per line.
pub fn project_jsonl(input: &[u8], spec: &ObjectSpec) -> Result<Vec<Vec<u8>>, StreamError> {
    let mut results = Vec::new();

    for (line_number, line) in input.split(|&b| b == b'\n').enumerate() {
        let line = if line.last() == Some(&b'\r') {
            &line[..line.len() - 1]
        } else {
            line
        };

        let trimmed = trim_ascii(line);
        if trimmed.is_empty() {
            continue;
        }

        match project_object(trimmed, spec) {
            Ok(projected) => results.push(projected),
            Err(e) => {
                let line_num = line_number + 1;
                let msg = &e.message;
                return Err(StreamError::new(format!(
                    "Invalid JSONL record on line {line_num}: {msg}"
                )));
            }
        }
    }

    Ok(results)
}

/// Project a JSON array with prefix navigation and slice-based indexing.
///
/// Returns one `Vec<u8>` per matching item. Items are selected by
/// `start`, `stop`, and `step` parameters (like Python slice semantics).
#[allow(clippy::similar_names)]
pub fn project_array_items_sliced(
    input: &[u8],
    spec: &ObjectSpec,
    prefix: &[&str],
    start: usize,
    stop: Option<usize>,
    step: usize,
) -> Result<Vec<Vec<u8>>, StreamError> {
    let mut jiter = Jiter::new(input);

    if !prefix.is_empty() {
        navigate_to_prefix(&mut jiter, prefix)?;
    }

    let peek = jiter.peek()?;
    if peek != Peek::Array {
        return Err(StreamError::new(format!(
            "Expected a JSON array, got {peek:?}"
        )));
    }

    let first = jiter.known_array()?;
    let mut results = Vec::new();
    let mut index: usize = 0;

    if let Some(mut peek) = first {
        loop {
            if let Some(s) = stop {
                if index >= s {
                    break;
                }
            }

            if index >= start && (index - start) % step == 0 {
                if peek != Peek::Object {
                    return Err(StreamError::new(format!(
                        "Expected array items to be JSON objects, got {peek:?}"
                    )));
                }
                let mut output = Vec::with_capacity(256);
                project_object_inner::<true, true>(&mut jiter, input, spec, &mut output)?;
                results.push(output);
            } else {
                jiter.known_skip(peek)?;
            }

            match jiter.array_step()? {
                Some(next_peek) => {
                    peek = next_peek;
                    index += 1;
                }
                None => break,
            }
        }
    }

    Ok(results)
}

/// Project a JSON array with prefix navigation, returning a single
/// concatenated JSON array as bytes.
pub fn project_array_nav(
    input: &[u8],
    spec: &ObjectSpec,
    prefix: &[&str],
) -> Result<Vec<u8>, StreamError> {
    let mut jiter = Jiter::new(input);

    let has_prefix = !prefix.is_empty();

    if has_prefix {
        navigate_to_prefix(&mut jiter, prefix)?;
    }

    let peek = jiter.peek()?;
    if peek != Peek::Array {
        return Err(StreamError::new(format!(
            "Expected a JSON array, got {peek:?}"
        )));
    }

    let first = jiter.known_array()?;

    let mut output = Vec::with_capacity((input.len() / 10).min(65_536));
    output.push(b'[');

    if let Some(peek) = first {
        if peek != Peek::Object {
            return Err(StreamError::new(format!(
                "Expected array items to be JSON objects, got {peek:?}"
            )));
        }
        project_object_inner::<true, true>(&mut jiter, input, spec, &mut output)?;

        while let Some(peek) = jiter.array_step()? {
            if peek != Peek::Object {
                return Err(StreamError::new(format!(
                    "Expected array items to be JSON objects, got {peek:?}"
                )));
            }
            output.push(b',');
            project_object_inner::<true, true>(&mut jiter, input, spec, &mut output)?;
        }
    }

    output.push(b']');

    if !has_prefix {
        jiter.finish()?;
    }

    Ok(output)
}

/// Result of projecting complete items from a partial JSON array buffer.
#[derive(Debug)]
pub struct PartialProjectionResult {
    /// Projected items found in this chunk.
    pub items: Vec<Vec<u8>>,
    /// Number of bytes consumed from the input.
    pub consumed: usize,
    /// Whether the closing `]` was reached.
    pub finished: bool,
}

/// Process a (possibly incomplete) chunk of a top-level JSON array,
/// projecting each complete object item.
///
/// This combines streaming (chunk boundary detection) with projection
/// (field filtering) in a single pass for maximum efficiency.
///
/// * `is_start` — `true` for the first chunk (expects leading `[`),
///   `false` for continuation chunks.
pub fn project_array_items_partial(
    input: &[u8],
    spec: &ObjectSpec,
    is_start: bool,
) -> Result<PartialProjectionResult, StreamError> {
    use crate::streaming::is_partial_error;

    let mut items = Vec::new();
    let mut pos: usize = 0;
    let len = input.len();

    skip_ws(input, &mut pos);

    if is_start {
        if pos >= len {
            return Ok(PartialProjectionResult {
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
            return Ok(PartialProjectionResult {
                items,
                consumed: pos,
                finished: false,
            });
        }

        match input[pos] {
            b']' => {
                return Ok(PartialProjectionResult {
                    items,
                    consumed: pos + 1,
                    finished: true,
                });
            }
            b',' => {
                pos += 1;
            }
            b'{' => {
                let slice = &input[pos..];
                let mut jiter = Jiter::new(slice);
                match project_object_from_current::<true, false>(&mut jiter, slice, spec) {
                    Ok(projected) => {
                        let obj_end = pos + jiter.current_index();
                        items.push(projected);
                        pos = obj_end;
                    }
                    Err(e) if is_partial_error(&e) => {
                        return Ok(PartialProjectionResult {
                            items,
                            consumed: pos,
                            finished: false,
                        });
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            other => {
                return Err(StreamError::new(format!(
                    "Expected object in array, got {:?}",
                    other as char
                )));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Project the object at the current parser position into a new buffer.
///
/// * `EARLY_EXIT` — skip hash lookups once all known fields have been seen
///   (safe for array projections; `false` for single-object projection to
///   preserve "last-wins" semantics for duplicate keys).
/// * `FAST_SKIP`  — after `EARLY_EXIT` fires, replace jiter with a custom
///   byte-scanner to skip the remaining object content in O(bytes) rather
///   than O(elements).  Only safe when the caller does NOT read
///   `jiter.current_index()` after this call (i.e. array-bulk paths).
fn project_object_from_current<'a, const EARLY_EXIT: bool, const FAST_SKIP: bool>(
    jiter: &mut Jiter<'a>,
    input: &'a [u8],
    spec: &ObjectSpec,
) -> Result<Vec<u8>, JiterError> {
    let mut output = Vec::with_capacity(256);
    project_object_inner::<EARLY_EXIT, FAST_SKIP>(jiter, input, spec, &mut output)?;
    Ok(output)
}

/// Project a single object from the current jiter position.
/// Assumes jiter has already peeked `Peek::Object`.
fn project_object_inner<'a, const EARLY_EXIT: bool, const FAST_SKIP: bool>(
    jiter: &mut Jiter<'a>,
    input: &'a [u8],
    spec: &ObjectSpec,
    output: &mut Vec<u8>,
) -> Result<(), JiterError> {
    let first_key = jiter.known_object()?;

    output.push(b'{');
    let mut first_field = true;

    if let Some(key) = first_key {
        // `spec.fields.get(key)` is the last use of `key`; NLL releases the
        // jiter borrow before `process_field` mutably borrows it again.
        let field = spec.fields.get(key);

        if EARLY_EXIT {
            let mut remaining = spec.fields.len();
            if field.is_some() && remaining > 0 {
                remaining -= 1;
            }
            process_field::<EARLY_EXIT, FAST_SKIP>(jiter, input, output, field, &mut first_field)?;

            loop {
                match jiter.next_key()? {
                    None => break,
                    Some(key) => {
                        if remaining > 0 {
                            let field = spec.fields.get(key);
                            if field.is_some() {
                                remaining -= 1;
                            }
                            process_field::<EARLY_EXIT, FAST_SKIP>(
                                jiter, input, output, field, &mut first_field,
                            )?;
                        } else if FAST_SKIP {
                            // All known fields found.  Use the fast byte-scanner to
                            // skip the current value and all remaining key-value pairs
                            // in one pass, then replace jiter so the caller can
                            // continue from the correct position.
                            // NLL: key not used here — jiter borrow released.
                            let abs_pos = jiter_abs_pos(jiter, input);
                            let abs_end =
                                fast_skip_to_object_end(input, abs_pos)?;
                            *jiter = Jiter::new(&input[abs_end..]);
                            break;
                        } else {
                            // All known fields found — skip without hash lookup.
                            // NLL: key not used here, jiter borrow released.
                            jiter.next_skip()?;
                        }
                    }
                }
            }
        } else {
            process_field::<EARLY_EXIT, FAST_SKIP>(jiter, input, output, field, &mut first_field)?;
            loop {
                match jiter.next_key()? {
                    None => break,
                    Some(key) => {
                        let field = spec.fields.get(key);
                        process_field::<EARLY_EXIT, FAST_SKIP>(
                            jiter, input, output, field, &mut first_field,
                        )?;
                    }
                }
            }
        }
    }

    output.push(b'}');
    Ok(())
}

/// Process a single looked-up field during object projection.
///
/// Receives the already-resolved `Option<&FieldSpec>` so that the caller
/// can drop the `key: &str` borrow on jiter before this function borrows
/// jiter mutably again.
#[inline]
fn process_field<'a, const EARLY_EXIT: bool, const FAST_SKIP: bool>(
    jiter: &mut Jiter<'a>,
    input: &'a [u8],
    output: &mut Vec<u8>,
    field: Option<&crate::spec::FieldSpec>,
    first_field: &mut bool,
) -> Result<(), JiterError> {
    match field {
        None => {
            jiter.next_skip()?;
        }
        Some(field_spec) => {
            if let Some(ref nested_spec) = field_spec.nested {
                let peek = jiter.peek()?;
                if peek == Peek::Object {
                    if !*first_field {
                        output.push(b',');
                    }
                    output.extend_from_slice(&field_spec.encoded_key);
                    project_object_inner::<EARLY_EXIT, FAST_SKIP>(
                        jiter, input, nested_spec, output,
                    )?;
                    *first_field = false;
                } else {
                    copy_raw_value(jiter, output, field_spec, first_field)?;
                }
            } else {
                copy_raw_value(jiter, output, field_spec, first_field)?;
            }
        }
    }
    Ok(())
}

/// Copy a raw JSON value from jiter's backing data to the output buffer.
///
/// Uses `Jiter::slice_to_current` so the copy is correct even when jiter was
/// previously replaced with a sub-slice (which happens in the `FAST_SKIP` path).
#[inline]
fn copy_raw_value(
    jiter: &mut Jiter<'_>,
    output: &mut Vec<u8>,
    field_spec: &crate::spec::FieldSpec,
    first_field: &mut bool,
) -> Result<(), JiterError> {
    let start = jiter.current_index();
    jiter.next_skip()?;
    // slice_to_current uses jiter.data (which may be a sub-slice after FAST_SKIP
    // replaces jiter) so the copy is always from the correct bytes.
    let value_bytes = jiter.slice_to_current(start);

    if !*first_field {
        output.push(b',');
    }
    output.extend_from_slice(&field_spec.encoded_key);
    output.extend_from_slice(value_bytes);

    *first_field = false;
    Ok(())
}

// ---------------------------------------------------------------------------
// Fast-skip helpers (used by the FAST_SKIP=true path)
// ---------------------------------------------------------------------------

/// Return the absolute byte offset of jiter's current position within `input`.
///
/// Works even after jiter has been replaced with a sub-slice via
/// `*jiter = Jiter::new(&input[n..])`, because `slice_to_current(0).as_ptr()`
/// always points to the start of jiter's backing data regardless of
/// `current_index()`.
#[inline]
fn jiter_abs_pos(jiter: &Jiter<'_>, input: &[u8]) -> usize {
    // jiter.data is always &input[base..] for some base (either 0 initially,
    // or abs_end after a replacement).  Recover base via pointer arithmetic.
    let jiter_data_ptr = jiter.slice_to_current(0).as_ptr() as usize;
    let input_ptr = input.as_ptr() as usize;
    // Safe: jiter.data is a sub-slice of input (guaranteed by our construction).
    (jiter_data_ptr - input_ptr) + jiter.current_index()
}

/// Skip a JSON string that has already had its opening `"` consumed.
/// Returns the index immediately after the closing `"`.
fn fast_skip_string(data: &[u8], mut pos: usize) -> Result<usize, JiterError> {
    loop {
        // SIMD-accelerated search for `"` or `\`
        match memchr2(b'"', b'\\', &data[pos..]) {
            None => {
                return Err(JiterError {
                    error_type: JiterErrorType::JsonError(JsonErrorType::EofWhileParsingString),
                    index: pos,
                })
            }
            Some(offset) => {
                pos += offset;
                match data[pos] {
                    b'"' => return Ok(pos + 1),
                    // Backslash escape: skip `\` and the next character.
                    _ => pos += 2,
                }
            }
        }
    }
}

/// Characters that require a branch in a JSON container (object/array) scan.
/// Used by `fast_skip_container` to find structural bytes quickly.
const CONTAINER_STRUCTURAL: [u8; 256] = {
    let mut t = [0u8; 256];
    t[b'"' as usize] = 1;
    t[b'{' as usize] = 1;
    t[b'}' as usize] = 1;
    t[b'[' as usize] = 1;
    t[b']' as usize] = 1;
    t
};

/// Skip a JSON container (`{...}` or `[...]`) whose opening bracket has
/// already been consumed.  `close` is the matching closing bracket.
/// Returns the index immediately after the closing bracket.
fn fast_skip_container(data: &[u8], mut pos: usize, close: u8) -> Result<usize, JiterError> {
    let mut depth: u32 = 1;
    loop {
        // Find the next structural character via a lookup-table scan.
        // LLVM auto-vectorises this with NEON on AArch64.
        let offset = data[pos..]
            .iter()
            .position(|&b| CONTAINER_STRUCTURAL[b as usize] != 0)
            .ok_or(JiterError {
                error_type: JiterErrorType::JsonError(JsonErrorType::EofWhileParsingObject),
                index: pos,
            })?;
        pos += offset;
        match data[pos] {
            b'"' => {
                pos = fast_skip_string(data, pos + 1)?;
            }
            b'{' | b'[' => {
                depth += 1;
                pos += 1;
            }
            _ => {
                // `}` or `]`
                pos += 1;
                if depth == 1 && data[pos - 1] == close {
                    return Ok(pos);
                }
                depth -= 1;
            }
        }
    }
}

/// Skip a single JSON value starting at `pos`.  Returns the index immediately
/// after the value (after any leading whitespace has been consumed).
fn fast_skip_value(data: &[u8], mut pos: usize) -> Result<usize, JiterError> {
    while pos < data.len() && data[pos].is_ascii_whitespace() {
        pos += 1;
    }
    match data.get(pos) {
        None => Err(JiterError {
            error_type: JiterErrorType::JsonError(JsonErrorType::EofWhileParsingValue),
            index: pos,
        }),
        Some(&b'"') => fast_skip_string(data, pos + 1),
        Some(&b'{') => fast_skip_container(data, pos + 1, b'}'),
        Some(&b'[') => fast_skip_container(data, pos + 1, b']'),
        Some(b't' | b'n') => Ok(pos + 4), // true / null (both 4 bytes)
        Some(&b'f') => Ok(pos + 5),               // false
        _ => {
            // Number: advance until a delimiter is found.
            let end = pos
                + data[pos..]
                    .iter()
                    .position(|&b| {
                        matches!(b, b',' | b'}' | b']' | b' ' | b'\t' | b'\n' | b'\r')
                    })
                    .unwrap_or(data.len() - pos);
            Ok(end)
        }
    }
}

/// Skip from the start of an unknown value (at absolute byte `pos` inside
/// `data`) to just after the closing `}` of the *containing* JSON object.
///
/// Assumes the object's known fields have already been projected and jiter is
/// positioned at the start of the first unknown value.
fn fast_skip_to_object_end(data: &[u8], mut pos: usize) -> Result<usize, JiterError> {
    // 1. Skip the current (unknown) value.
    pos = fast_skip_value(data, pos)?;

    // 2. Skip remaining key:value pairs until the object's closing `}`.
    loop {
        while pos < data.len() && data[pos].is_ascii_whitespace() {
            pos += 1;
        }
        match data.get(pos) {
            Some(&b'}') => return Ok(pos + 1),
            Some(&b',') => {
                pos += 1;
                // Skip whitespace before key.
                while pos < data.len() && data[pos].is_ascii_whitespace() {
                    pos += 1;
                }
                // Skip the key string.
                if data.get(pos) != Some(&b'"') {
                    return Err(JiterError {
                        error_type: JiterErrorType::JsonError(
                            JsonErrorType::EofWhileParsingObject,
                        ),
                        index: pos,
                    });
                }
                pos = fast_skip_string(data, pos + 1)?;
                // Skip whitespace + colon.
                while pos < data.len() && data[pos].is_ascii_whitespace() {
                    pos += 1;
                }
                if data.get(pos) != Some(&b':') {
                    return Err(JiterError {
                        error_type: JiterErrorType::JsonError(
                            JsonErrorType::ExpectedObjectCommaOrEnd,
                        ),
                        index: pos,
                    });
                }
                pos += 1;
                // Skip the value.
                pos = fast_skip_value(data, pos)?;
            }
            _ => {
                return Err(JiterError {
                    error_type: JiterErrorType::JsonError(
                        JsonErrorType::ExpectedObjectCommaOrEnd,
                    ),
                    index: pos,
                })
            }
        }
    }
}

/// Skip ASCII whitespace bytes starting at `*pos`.
#[inline]
fn skip_ws(input: &[u8], pos: &mut usize) {
    while *pos < input.len() && input[*pos].is_ascii_whitespace() {
        *pos += 1;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use crate::spec::{FieldSpec, ObjectSpec};

    use super::*;

    // Helper constructors
    fn field(output_key: &str) -> FieldSpec {
        FieldSpec::new(output_key, None)
    }

    fn field_nested(output_key: &str, nested: ObjectSpec) -> FieldSpec {
        FieldSpec::new(output_key, Some(Arc::new(nested)))
    }

    fn mk_spec(pairs: &[(&str, FieldSpec)]) -> ObjectSpec {
        ObjectSpec::from_fields(
            pairs
                .iter()
                .map(|(k, v)| (Box::from(*k), v.clone())),
        )
    }

    fn parse(bytes: &[u8]) -> serde_json::Value {
        serde_json::from_slice(bytes).unwrap()
    }

    fn parse_items(items: &[Vec<u8>]) -> Vec<serde_json::Value> {
        items.iter().map(|item| parse(item)).collect()
    }

    // -- project_object tests --

    #[test]
    fn empty_object() {
        let s = mk_spec(&[]);
        let out = project_object(b"{}", &s).unwrap();
        assert_eq!(parse(&out), serde_json::json!({}));
    }

    #[test]
    fn single_known_field_integer() {
        let s = mk_spec(&[("x", field("x"))]);
        let out = project_object(b"{\"x\":1}", &s).unwrap();
        assert_eq!(parse(&out), serde_json::json!({"x": 1}));
    }

    #[test]
    fn unknown_fields_stripped() {
        let s = mk_spec(&[("x", field("x"))]);
        let out = project_object(b"{\"x\":1,\"y\":2}", &s).unwrap();
        assert_eq!(parse(&out), serde_json::json!({"x": 1}));
    }

    #[test]
    fn nested_object_projection() {
        let inner_spec = mk_spec(&[("a", field("a"))]);
        let s = mk_spec(&[("inner", field_nested("inner", inner_spec))]);
        let out = project_object(b"{\"inner\":{\"a\":1,\"b\":2}}", &s).unwrap();
        assert_eq!(parse(&out), serde_json::json!({"inner": {"a": 1}}));
    }

    #[test]
    fn project_object_rejects_non_object_input() {
        let s = mk_spec(&[("x", field("x"))]);
        let err = project_object(b"[{\"x\":1}]", &s).unwrap_err();
        assert!(err.message.contains("Expected a top-level JSON object"));
    }

    // -- project_array tests --

    #[test]
    fn array_multiple_objects_stripped() {
        let s = mk_spec(&[("x", field("x"))]);
        let out =
            project_array(b"[{\"x\":1,\"noise\":\"a\"},{\"x\":2,\"noise\":\"b\"}]", &s).unwrap();
        assert_eq!(parse(&out), serde_json::json!([{"x":1},{"x":2}]));
    }

    #[test]
    fn project_array_rejects_non_object_items() {
        let s = mk_spec(&[("x", field("x"))]);
        let err = project_array(b"[{\"x\":1},2]", &s).unwrap_err();
        assert!(err
            .message
            .contains("Expected array items to be JSON objects"));
    }

    // -- project_array_items tests --

    #[test]
    fn project_array_items_returns_projected_objects() {
        let s = mk_spec(&[("x", field("x"))]);
        let out = project_array_items(b"[{\"x\":1,\"skip\":0},{\"x\":2,\"skip\":0}]", &s).unwrap();

        assert_eq!(
            parse_items(&out),
            vec![serde_json::json!({"x": 1}), serde_json::json!({"x": 2})]
        );
    }

    #[test]
    fn project_array_items_rejects_non_array_input() {
        let s = mk_spec(&[("x", field("x"))]);
        let err = project_array_items(b"{\"x\":1}", &s).unwrap_err();
        assert!(err.message.contains("Expected a top-level JSON array"));
    }

    // -- project_jsonl tests --

    #[test]
    fn project_jsonl_skips_blank_lines_and_crlf() {
        let s = mk_spec(&[("x", field("x"))]);
        let input = b" \r\n{\"x\":1,\"skip\":0}\r\n\t{\"x\":2}\n";
        let out = project_jsonl(input, &s).unwrap();

        assert_eq!(
            parse_items(&out),
            vec![serde_json::json!({"x": 1}), serde_json::json!({"x": 2})]
        );
    }

    #[test]
    fn project_jsonl_reports_line_number_for_malformed_record() {
        let s = mk_spec(&[("x", field("x"))]);
        let err = project_jsonl(b"{\"x\":1}\n{\"x\":\n", &s).unwrap_err();

        assert!(err.message.contains("Invalid JSONL record on line 2"));
    }

    #[test]
    fn project_jsonl_rejects_non_object_record() {
        let s = mk_spec(&[("x", field("x"))]);
        let err = project_jsonl(b"{\"x\":1}\n[1]\n", &s).unwrap_err();

        assert!(err.message.contains("Invalid JSONL record on line 2"));
        assert!(err.message.contains("Expected a top-level JSON object"));
    }

    // -- project_array_items_sliced tests --

    #[test]
    fn sliced_with_prefix() {
        let s = mk_spec(&[("x", field("x"))]);
        let input = b"{\"items\":[{\"x\":1},{\"x\":2}]}";
        let out = project_array_items_sliced(input, &s, &["items"], 0, None, 1).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(parse(&out[0]), serde_json::json!({"x": 1}));
    }

    #[test]
    fn sliced_respects_start_stop_and_step() {
        let s = mk_spec(&[("x", field("x"))]);
        let input =
            b"{\"items\":[{\"x\":0},{\"x\":1},{\"x\":2},{\"x\":3},{\"x\":4}],\"tail\":true}";
        let out = project_array_items_sliced(input, &s, &["items"], 1, Some(5), 2).unwrap();

        assert_eq!(
            parse_items(&out),
            vec![serde_json::json!({"x": 1}), serde_json::json!({"x": 3})]
        );
    }

    #[test]
    fn sliced_rejects_non_object_selected_item() {
        let s = mk_spec(&[("x", field("x"))]);
        let err = project_array_items_sliced(b"[{\"x\":1},2]", &s, &[], 0, None, 1).unwrap_err();

        assert!(err
            .message
            .contains("Expected array items to be JSON objects"));
    }

    // -- project_array_nav tests --

    #[test]
    fn project_array_nav_projects_prefixed_array() {
        let s = mk_spec(&[("x", field("x"))]);
        let input = b"{\"data\":{\"items\":[{\"x\":1,\"skip\":0},{\"x\":2,\"skip\":0}],\"after\":true},\"meta\":0}";
        let out = project_array_nav(input, &s, &["data", "items"]).unwrap();

        assert_eq!(parse(&out), serde_json::json!([{"x": 1}, {"x": 2}]));
    }

    #[test]
    fn project_array_nav_reports_missing_prefix_key() {
        let s = mk_spec(&[("x", field("x"))]);
        let err = project_array_nav(b"{\"data\":{}}", &s, &["data", "items"]).unwrap_err();

        assert!(err.message.contains("Prefix key"));
        assert!(err.message.contains("items"));
    }

    #[test]
    fn project_array_nav_rejects_non_array_target() {
        let s = mk_spec(&[("x", field("x"))]);
        let err = project_array_nav(b"{\"items\":{\"x\":1}}", &s, &["items"]).unwrap_err();

        assert!(err.message.contains("Expected a JSON array"));
    }

    // -- project_array_items_partial tests --

    #[test]
    fn partial_complete_array() {
        let s = mk_spec(&[("x", field("x"))]);
        let input = b"[{\"x\":1},{\"x\":2}]";
        let result = project_array_items_partial(input, &s, true).unwrap();
        assert!(result.finished);
        assert_eq!(result.consumed, input.len());
        assert_eq!(
            parse_items(&result.items),
            vec![serde_json::json!({"x": 1}), serde_json::json!({"x": 2})]
        );
    }

    #[test]
    fn partial_incomplete_array() {
        let s = mk_spec(&[("x", field("x"))]);
        let input = b"[{\"x\":1},{\"x\":";
        let result = project_array_items_partial(input, &s, true).unwrap();
        assert!(!result.finished);
        assert_eq!(result.consumed, 9);
        assert_eq!(
            parse_items(&result.items),
            vec![serde_json::json!({"x": 1})]
        );
    }

    #[test]
    fn partial_incomplete_unknown_field_returns_partial() {
        let s = mk_spec(&[("x", field("x"))]);
        let input = br#"[{"x":1,"skip":{"nested":1"#;
        let result = project_array_items_partial(input, &s, true).unwrap();

        assert!(!result.finished);
        assert_eq!(result.consumed, 1);
        assert!(result.items.is_empty());
    }

    #[test]
    fn partial_two_chunk_handoff_uses_consumed_offset() {
        let s = mk_spec(&[("x", field("x"))]);
        let first_chunk = b"[{\"x\":1},{\"x\":2";
        let first = project_array_items_partial(first_chunk, &s, true).unwrap();

        assert_eq!(first.consumed, 9);
        assert!(!first.finished);
        assert_eq!(parse_items(&first.items), vec![serde_json::json!({"x": 1})]);

        let mut continuation = first_chunk[first.consumed..].to_vec();
        continuation.extend_from_slice(br#"},{"x":3}]"#);

        let second = project_array_items_partial(&continuation, &s, false).unwrap();
        assert_eq!(second.consumed, continuation.len());
        assert!(second.finished);
        assert_eq!(
            parse_items(&second.items),
            vec![serde_json::json!({"x": 2}), serde_json::json!({"x": 3})]
        );
    }

    #[test]
    fn partial_rejects_non_object_items() {
        let s = mk_spec(&[("x", field("x"))]);
        let err = project_array_items_partial(b"[1]", &s, true).unwrap_err();

        assert!(err.message.contains("Expected object in array"));
    }
}
