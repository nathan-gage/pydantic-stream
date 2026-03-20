mod spec;

use std::borrow::Cow;

use pyo3::create_exception;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyList};

use pydantic_stream_core::streaming::ByteSpan;

use spec::{PyFieldSpec, PyObjectSpec};

create_exception!(_native, StreamingProjectionError, PyRuntimeError);

// ---------------------------------------------------------------------------
// Streaming functions (no projection / no spec required)
// ---------------------------------------------------------------------------

/// Extract complete JSON items from a (possibly incomplete) JSON array buffer.
/// Returns (`item_bytes_list`, `consumed_bytes`, `finished`).
#[pyfunction]
#[pyo3(signature = (data, is_start=true))]
fn extract_array_items(
    py: Python<'_>,
    data: Cow<'_, [u8]>,
    is_start: bool,
) -> PyResult<(Vec<Py<PyAny>>, usize, bool)> {
    let result = pydantic_stream_core::streaming::extract_array_items(&data, is_start);
    match result {
        Ok(r) => {
            let py_items: Vec<Py<PyAny>> = r
                .items
                .iter()
                .map(|span: &ByteSpan| PyBytes::new(py, &data[span.start..span.end]).into())
                .collect();
            Ok((py_items, r.consumed, r.finished))
        }
        Err(e) => Err(StreamingProjectionError::new_err(e.message)),
    }
}

// ---------------------------------------------------------------------------
// Projection functions (require ObjectSpec)
// ---------------------------------------------------------------------------

/// Project a single JSON object, keeping only fields in the spec.
#[pyfunction]
fn project_object(py: Python<'_>, data: Cow<'_, [u8]>, spec: &PyObjectSpec) -> PyResult<Py<PyAny>> {
    let result = pydantic_stream_core::projection::project_object(&data, &spec.inner);
    match result {
        Ok(bytes) => Ok(PyBytes::new(py, &bytes).into()),
        Err(e) => Err(StreamingProjectionError::new_err(e.message)),
    }
}

/// Project a JSON array of objects, keeping only fields in the spec.
#[pyfunction]
fn project_array(py: Python<'_>, data: Cow<'_, [u8]>, spec: &PyObjectSpec) -> PyResult<Py<PyAny>> {
    let result = pydantic_stream_core::projection::project_array(&data, &spec.inner);
    match result {
        Ok(bytes) => Ok(PyBytes::new(py, &bytes).into()),
        Err(e) => Err(StreamingProjectionError::new_err(e.message)),
    }
}

/// Project a JSON array of objects, returning one `bytes` per item.
#[pyfunction]
fn project_array_items(
    py: Python<'_>,
    data: Cow<'_, [u8]>,
    spec: &PyObjectSpec,
) -> PyResult<Vec<Py<PyAny>>> {
    let result = pydantic_stream_core::projection::project_array_items(&data, &spec.inner);
    match result {
        Ok(items) => {
            let py_items: Vec<Py<PyAny>> = items
                .iter()
                .map(|item| PyBytes::new(py, item).into())
                .collect();
            Ok(py_items)
        }
        Err(e) => Err(StreamingProjectionError::new_err(e.message)),
    }
}

/// Project a JSON array and validate projected items in small batches with the
/// supplied validators, falling back to per-item validation inside a failing
/// batch so Python keeps "yield valid items until the first error" semantics.
#[pyfunction]
#[pyo3(signature = (data, spec, validator, list_validator, batch_size=16))]
fn validate_array_items_batched(
    py: Python<'_>,
    data: Cow<'_, [u8]>,
    spec: &PyObjectSpec,
    validator: &Bound<'_, PyAny>,
    list_validator: &Bound<'_, PyAny>,
    batch_size: usize,
) -> PyResult<(Vec<Py<PyAny>>, Option<Py<PyAny>>)> {
    let result = pydantic_stream_core::projection::project_array_items(&data, &spec.inner);
    match result {
        Ok(items) => {
            let mut validated = Vec::with_capacity(items.len());
            let batch_size = batch_size.max(1);
            for batch in items.chunks(batch_size) {
                let payload_len =
                    2 + batch.iter().map(Vec::len).sum::<usize>() + batch.len().saturating_sub(1);
                let mut blob = Vec::with_capacity(payload_len);
                blob.push(b'[');
                for (index, item) in batch.iter().enumerate() {
                    if index > 0 {
                        blob.push(b',');
                    }
                    blob.extend_from_slice(item);
                }
                blob.push(b']');

                match list_validator.call1((PyBytes::new(py, &blob),)) {
                    Ok(list_obj) => {
                        let list = list_obj.cast::<PyList>()?;
                        validated.extend(list.iter().map(Bound::unbind));
                    }
                    Err(_) if batch.len() > 1 => {
                        for item in batch {
                            match validator.call1((PyBytes::new(py, item),)) {
                                Ok(obj) => validated.push(obj.unbind()),
                                Err(err) => return Ok((validated, Some(err.into_value(py).into()))),
                            }
                        }
                    }
                    Err(err) => return Ok((validated, Some(err.into_value(py).into()))),
                }
            }
            Ok((validated, None))
        }
        Err(e) => Err(StreamingProjectionError::new_err(e.message)),
    }
}

/// Project a JSON array and validate projected items with the supplied
/// callable (typically ``TypeAdapter.validate_json``), stopping after the
/// first validation error so Python can preserve "yield valid items until the
/// first error" iteration semantics.
#[pyfunction]
fn validate_array_items(
    py: Python<'_>,
    data: Cow<'_, [u8]>,
    spec: &PyObjectSpec,
    validator: &Bound<'_, PyAny>,
) -> PyResult<(Vec<Py<PyAny>>, Option<Py<PyAny>>)> {
    let mut validated = Vec::new();
    let result = pydantic_stream_core::projection::visit_projected_array_items(
        &data,
        &spec.inner,
        |item| match validator.call1((PyBytes::new(py, &item),)) {
            Ok(obj) => {
                validated.push(obj.unbind());
                Ok(())
            }
            Err(err) => Err(err),
        },
    );

    match result {
        Ok(()) => Ok((validated, None)),
        Err(pydantic_stream_core::projection::VisitProjectedArrayItemsError::Stream(err)) => {
            Err(StreamingProjectionError::new_err(err.message))
        }
        Err(pydantic_stream_core::projection::VisitProjectedArrayItemsError::Visitor(err)) => {
            Ok((validated, Some(err.into_value(py).into())))
        }
    }
}

/// Project and validate one top-level array item at a time.
#[pyfunction]
#[pyo3(signature = (data, spec, validator, pos=0, started=false))]
fn validate_array_item_next(
    py: Python<'_>,
    data: Cow<'_, [u8]>,
    spec: &PyObjectSpec,
    validator: &Bound<'_, PyAny>,
    pos: usize,
    started: bool,
) -> PyResult<(Option<Py<PyAny>>, usize, bool)> {
    match pydantic_stream_core::projection::project_next_array_item(&data, &spec.inner, pos, started) {
        Ok(result) => match result.item {
            Some(bytes) => {
                let obj = validator.call1((PyBytes::new(py, &bytes),))?;
                Ok((Some(obj.unbind()), result.next_pos, result.finished))
            }
            None => Ok((None, result.next_pos, result.finished)),
        },
        Err(e) => Err(StreamingProjectionError::new_err(e.message)),
    }
}

/// Project and validate a small batch of top-level array items at a time.
#[pyfunction]
#[pyo3(signature = (data, spec, validator, pos=0, started=false, batch_size=8))]
fn validate_array_items_next_batch(
    py: Python<'_>,
    data: Cow<'_, [u8]>,
    spec: &PyObjectSpec,
    validator: &Bound<'_, PyAny>,
    pos: usize,
    started: bool,
    batch_size: usize,
) -> PyResult<(Vec<Py<PyAny>>, usize, bool, Option<Py<PyAny>>)> {
    match pydantic_stream_core::projection::project_next_array_items_batch(
        &data,
        &spec.inner,
        pos,
        started,
        batch_size,
    ) {
        Ok(result) => {
            let mut validated = Vec::with_capacity(result.items.len());
            for item in result.items {
                match validator.call1((PyBytes::new(py, &item),)) {
                    Ok(obj) => validated.push(obj.unbind()),
                    Err(err) => {
                        return Ok((
                            validated,
                            result.next_pos,
                            result.finished,
                            Some(err.into_value(py).into()),
                        ))
                    }
                }
            }
            Ok((validated, result.next_pos, result.finished, None))
        }
        Err(e) => Err(StreamingProjectionError::new_err(e.message)),
    }
}

/// Validate one raw top-level array item at a time.
#[pyfunction]
#[pyo3(signature = (data, validator, pos=0, started=false))]
fn validate_raw_array_item_next(
    py: Python<'_>,
    data: Cow<'_, [u8]>,
    validator: &Bound<'_, PyAny>,
    pos: usize,
    started: bool,
) -> PyResult<(Option<Py<PyAny>>, usize, bool)> {
    match pydantic_stream_core::streaming::next_array_item(&data, pos, started) {
        Ok(result) => match result.span {
            Some(span) => {
                let obj = validator.call1((PyBytes::new(py, &data[span.start..span.end]),))?;
                Ok((Some(obj.unbind()), result.next_pos, result.finished))
            }
            None => Ok((None, result.next_pos, result.finished)),
        },
        Err(e) => Err(StreamingProjectionError::new_err(e.message)),
    }
}

/// Validate raw JSON array items directly with the supplied callable.
#[pyfunction]
fn validate_raw_array_items(
    py: Python<'_>,
    data: Cow<'_, [u8]>,
    validator: &Bound<'_, PyAny>,
) -> PyResult<(Vec<Py<PyAny>>, Option<Py<PyAny>>)> {
    let mut validated = Vec::new();
    let result = pydantic_stream_core::streaming::visit_array_items(&data, true, |span| {
        match validator.call1((PyBytes::new(py, &data[span.start..span.end]),)) {
            Ok(obj) => {
                validated.push(obj.unbind());
                Ok(())
            }
            Err(err) => Err(err),
        }
    });

    match result {
        Ok(_) => Ok((validated, None)),
        Err(pydantic_stream_core::streaming::VisitArrayItemsError::Stream(err)) => {
            Err(StreamingProjectionError::new_err(err.message))
        }
        Err(pydantic_stream_core::streaming::VisitArrayItemsError::Visitor(err)) => {
            Ok((validated, Some(err.into_value(py).into())))
        }
    }
}

/// Project JSONL input, returning a list of projected JSON byte strings.
#[pyfunction]
fn project_jsonl(py: Python<'_>, data: Cow<'_, [u8]>, spec: &PyObjectSpec) -> PyResult<Vec<Py<PyAny>>> {
    let result = pydantic_stream_core::projection::project_jsonl(&data, &spec.inner);
    match result {
        Ok(lines) => {
            let py_lines: Vec<Py<PyAny>> = lines
                .iter()
                .map(|line| PyBytes::new(py, line).into())
                .collect();
            Ok(py_lines)
        }
        Err(e) => Err(StreamingProjectionError::new_err(e.message)),
    }
}

/// Project a JSON array with prefix navigation and slice-based indexing.
#[pyfunction]
#[pyo3(signature = (data, spec, prefix=None, start=0, stop=None, step=1))]
#[allow(clippy::similar_names)]
fn project_array_items_sliced(
    py: Python<'_>,
    data: Cow<'_, [u8]>,
    spec: &PyObjectSpec,
    prefix: Option<&str>,
    start: usize,
    stop: Option<usize>,
    step: usize,
) -> PyResult<Vec<Py<PyAny>>> {
    let segments: Vec<&str> = match prefix {
        Some(p) if !p.is_empty() => p.split('.').collect(),
        _ => Vec::new(),
    };

    let result = pydantic_stream_core::projection::project_array_items_sliced(
        &data,
        &spec.inner,
        &segments,
        start,
        stop,
        step,
    );

    match result {
        Ok(items) => {
            let py_items: Vec<Py<PyAny>> = items
                .iter()
                .map(|item| PyBytes::new(py, item).into())
                .collect();
            Ok(py_items)
        }
        Err(e) => Err(StreamingProjectionError::new_err(e.message)),
    }
}

/// Project one JSON array item with prefix navigation.
#[pyfunction]
#[pyo3(signature = (data, spec, prefix=None, index=0))]
fn project_array_item_at(
    py: Python<'_>,
    data: Cow<'_, [u8]>,
    spec: &PyObjectSpec,
    prefix: Option<&str>,
    index: usize,
) -> PyResult<Option<Py<PyAny>>> {
    let segments: Vec<&str> = match prefix {
        Some(p) if !p.is_empty() => p.split('.').collect(),
        _ => Vec::new(),
    };

    let result = pydantic_stream_core::projection::project_array_item_at(&data, &spec.inner, &segments, index);

    match result {
        Ok(Some(bytes)) => Ok(Some(PyBytes::new(py, &bytes).into())),
        Ok(None) => Ok(None),
        Err(e) => Err(StreamingProjectionError::new_err(e.message)),
    }
}

/// Project a JSON array with prefix navigation, returning concatenated JSON array.
#[pyfunction]
#[pyo3(signature = (data, spec, prefix=None))]
fn project_array_nav(
    py: Python<'_>,
    data: Cow<'_, [u8]>,
    spec: &PyObjectSpec,
    prefix: Option<&str>,
) -> PyResult<Py<PyAny>> {
    let segments: Vec<&str> = match prefix {
        Some(p) if !p.is_empty() => p.split('.').collect(),
        _ => Vec::new(),
    };

    let result = pydantic_stream_core::projection::project_array_nav(&data, &spec.inner, &segments);

    match result {
        Ok(bytes) => Ok(PyBytes::new(py, &bytes).into()),
        Err(e) => Err(StreamingProjectionError::new_err(e.message)),
    }
}

/// Project a JSON array with prefix navigation and validate the projected array.
#[pyfunction]
#[pyo3(signature = (data, spec, validator, prefix=None))]
fn validate_array_nav(
    py: Python<'_>,
    data: Cow<'_, [u8]>,
    spec: &PyObjectSpec,
    validator: &Bound<'_, PyAny>,
    prefix: Option<&str>,
) -> PyResult<Py<PyAny>> {
    let segments: Vec<&str> = match prefix {
        Some(p) if !p.is_empty() => p.split('.').collect(),
        _ => Vec::new(),
    };

    let result = pydantic_stream_core::projection::project_array_nav(&data, &spec.inner, &segments);

    match result {
        Ok(bytes) => validator.call1((PyBytes::new(py, &bytes),)).map(Bound::unbind),
        Err(e) => Err(StreamingProjectionError::new_err(e.message)),
    }
}

/// Project a partial chunk of a JSON array and validate the projected items.
#[pyfunction]
#[pyo3(signature = (data, spec, validator, is_start=true))]
fn validate_array_items_partial(
    py: Python<'_>,
    data: Cow<'_, [u8]>,
    spec: &PyObjectSpec,
    validator: &Bound<'_, PyAny>,
    is_start: bool,
) -> PyResult<(Vec<Py<PyAny>>, usize, bool, Option<Py<PyAny>>)> {
    let result =
        pydantic_stream_core::projection::project_array_items_partial(&data, &spec.inner, is_start);
    match result {
        Ok(r) => {
            let mut validated = Vec::with_capacity(r.items.len());
            for item in r.items {
                match validator.call1((PyBytes::new(py, &item),)) {
                    Ok(obj) => validated.push(obj.unbind()),
                    Err(err) => {
                        return Ok((validated, r.consumed, r.finished, Some(err.into_value(py).into())));
                    }
                }
            }
            Ok((validated, r.consumed, r.finished, None))
        }
        Err(e) => Err(StreamingProjectionError::new_err(e.message)),
    }
}

/// Project a partial chunk of a JSON array (streaming + projection combined).
#[pyfunction]
#[pyo3(signature = (data, spec, is_start=true))]
fn project_array_items_partial(
    py: Python<'_>,
    data: Cow<'_, [u8]>,
    spec: &PyObjectSpec,
    is_start: bool,
) -> PyResult<(Vec<Py<PyAny>>, usize, bool)> {
    let result =
        pydantic_stream_core::projection::project_array_items_partial(&data, &spec.inner, is_start);
    match result {
        Ok(r) => {
            let py_items: Vec<Py<PyAny>> = r
                .items
                .iter()
                .map(|item| PyBytes::new(py, item).into())
                .collect();
            Ok((py_items, r.consumed, r.finished))
        }
        Err(e) => Err(StreamingProjectionError::new_err(e.message)),
    }
}

/// `PyO3` module definition.
#[pymodule(gil_used = false)]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Types
    m.add_class::<PyFieldSpec>()?;
    m.add_class::<PyObjectSpec>()?;
    m.add(
        "StreamingProjectionError",
        m.py().get_type::<StreamingProjectionError>(),
    )?;

    // Streaming (no spec)
    m.add_function(wrap_pyfunction!(extract_array_items, m)?)?;

    // Projection (requires spec)
    m.add_function(wrap_pyfunction!(project_array, m)?)?;
    m.add_function(wrap_pyfunction!(project_array_items, m)?)?;
    m.add_function(wrap_pyfunction!(validate_array_items, m)?)?;
    m.add_function(wrap_pyfunction!(validate_array_item_next, m)?)?;
    m.add_function(wrap_pyfunction!(validate_array_items_next_batch, m)?)?;
    m.add_function(wrap_pyfunction!(validate_array_items_batched, m)?)?;
    m.add_function(wrap_pyfunction!(validate_raw_array_item_next, m)?)?;
    m.add_function(wrap_pyfunction!(validate_raw_array_items, m)?)?;
    m.add_function(wrap_pyfunction!(project_object, m)?)?;
    m.add_function(wrap_pyfunction!(project_jsonl, m)?)?;
    m.add_function(wrap_pyfunction!(project_array_items_sliced, m)?)?;
    m.add_function(wrap_pyfunction!(project_array_item_at, m)?)?;
    m.add_function(wrap_pyfunction!(project_array_nav, m)?)?;
    m.add_function(wrap_pyfunction!(validate_array_nav, m)?)?;
    m.add_function(wrap_pyfunction!(validate_array_items_partial, m)?)?;
    m.add_function(wrap_pyfunction!(project_array_items_partial, m)?)?;
    Ok(())
}
