mod spec;

use pyo3::create_exception;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

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
    data: &[u8],
    is_start: bool,
) -> PyResult<(Vec<Py<PyAny>>, usize, bool)> {
    let result = pydantic_stream_core::streaming::extract_array_items(data, is_start);
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
fn project_object(py: Python<'_>, data: &[u8], spec: &PyObjectSpec) -> PyResult<Py<PyAny>> {
    let result = pydantic_stream_core::projection::project_object(data, &spec.inner);
    match result {
        Ok(bytes) => Ok(PyBytes::new(py, &bytes).into()),
        Err(e) => Err(StreamingProjectionError::new_err(e.message)),
    }
}

/// Project a JSON array of objects, keeping only fields in the spec.
#[pyfunction]
fn project_array(py: Python<'_>, data: &[u8], spec: &PyObjectSpec) -> PyResult<Py<PyAny>> {
    let result = pydantic_stream_core::projection::project_array(data, &spec.inner);
    match result {
        Ok(bytes) => Ok(PyBytes::new(py, &bytes).into()),
        Err(e) => Err(StreamingProjectionError::new_err(e.message)),
    }
}

/// Project a JSON array of objects, returning one `bytes` per item.
#[pyfunction]
fn project_array_items(
    py: Python<'_>,
    data: &[u8],
    spec: &PyObjectSpec,
) -> PyResult<Vec<Py<PyAny>>> {
    let result = pydantic_stream_core::projection::project_array_items(data, &spec.inner);
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

/// Project JSONL input, returning a list of projected JSON byte strings.
#[pyfunction]
fn project_jsonl(py: Python<'_>, data: &[u8], spec: &PyObjectSpec) -> PyResult<Vec<Py<PyAny>>> {
    let result = pydantic_stream_core::projection::project_jsonl(data, &spec.inner);
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
    data: &[u8],
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
        data,
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

/// Project a JSON array with prefix navigation, returning concatenated JSON array.
#[pyfunction]
#[pyo3(signature = (data, spec, prefix=None))]
fn project_array_nav(
    py: Python<'_>,
    data: &[u8],
    spec: &PyObjectSpec,
    prefix: Option<&str>,
) -> PyResult<Py<PyAny>> {
    let segments: Vec<&str> = match prefix {
        Some(p) if !p.is_empty() => p.split('.').collect(),
        _ => Vec::new(),
    };

    let result = pydantic_stream_core::projection::project_array_nav(data, &spec.inner, &segments);

    match result {
        Ok(bytes) => Ok(PyBytes::new(py, &bytes).into()),
        Err(e) => Err(StreamingProjectionError::new_err(e.message)),
    }
}

/// Project a partial chunk of a JSON array (streaming + projection combined).
#[pyfunction]
#[pyo3(signature = (data, spec, is_start=true))]
fn project_array_items_partial(
    py: Python<'_>,
    data: &[u8],
    spec: &PyObjectSpec,
    is_start: bool,
) -> PyResult<(Vec<Py<PyAny>>, usize, bool)> {
    let result =
        pydantic_stream_core::projection::project_array_items_partial(data, &spec.inner, is_start);
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
    m.add_function(wrap_pyfunction!(project_object, m)?)?;
    m.add_function(wrap_pyfunction!(project_jsonl, m)?)?;
    m.add_function(wrap_pyfunction!(project_array_items_sliced, m)?)?;
    m.add_function(wrap_pyfunction!(project_array_nav, m)?)?;
    m.add_function(wrap_pyfunction!(project_array_items_partial, m)?)?;
    Ok(())
}
