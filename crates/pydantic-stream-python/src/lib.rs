mod spec;

use std::sync::Arc;

use pyo3::create_exception;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use pydantic_stream_core::streaming::{trim_ascii, ByteSpan};

use spec::{PyFieldSpec, PyObjectSpec};

create_exception!(_native, StreamingProjectionError, PyRuntimeError);

/// Incrementally project a top-level or prefixed JSON array.
///
/// This is a low-level helper for chunked streaming. ``push(...)`` returns a
/// projected JSON array blob whenever one or more complete items are ready.
#[pyclass(name = "ProjectedArrayBlobStreamer")]
struct PyProjectedArrayBlobStreamer {
    spec: Arc<pydantic_stream_core::spec::ObjectSpec>,
    prefix: Vec<String>,
    buffer: Vec<u8>,
    is_start: bool,
    array_started: bool,
    finished: bool,
}

#[pymethods]
impl PyProjectedArrayBlobStreamer {
    fn check_trailing(&self) -> bool {
        self.prefix.is_empty()
    }

    /// Create a streamer for a top-level array or a nested array selected by
    /// ``prefix``.
    #[new]
    #[pyo3(signature = (spec, prefix=None))]
    fn new(spec: &PyObjectSpec, prefix: Option<&str>) -> Self {
        let prefix_vec = match prefix {
            Some(p) if !p.is_empty() => p.split('.').map(ToOwned::to_owned).collect(),
            _ => Vec::new(),
        };
        let array_started = prefix_vec.is_empty();
        Self {
            spec: Arc::clone(&spec.inner),
            prefix: prefix_vec,
            buffer: Vec::new(),
            is_start: true,
            array_started,
            finished: false,
        }
    }

    /// Feed one chunk of input.
    ///
    /// Returns ``None`` if more input is needed, or a projected JSON array blob
    /// containing the complete items found so far.
    fn push(&mut self, py: Python<'_>, chunk: &[u8]) -> PyResult<Option<Py<PyAny>>> {
        if self.finished {
            if self.check_trailing() && !trim_ascii(chunk).is_empty() {
                return Err(StreamingProjectionError::new_err(
                    "Trailing content after JSON array",
                ));
            }
            return Ok(None);
        }

        if chunk.is_empty() {
            return Ok(None);
        }

        self.buffer.extend_from_slice(chunk);

        if !self.array_started {
            let segments: Vec<&str> = self.prefix.iter().map(String::as_str).collect();
            match pydantic_stream_core::streaming::locate_array_start(&self.buffer, &segments) {
                Ok(Some(start)) => {
                    self.buffer.drain(..start);
                    self.array_started = true;
                    self.is_start = true;
                }
                Ok(None) => return Ok(None),
                Err(e) => return Err(StreamingProjectionError::new_err(e.message)),
            }
        }

        let result = pydantic_stream_core::projection::project_array_blob_partial(
            &self.buffer,
            &self.spec,
            self.is_start,
        )
        .map_err(|e| StreamingProjectionError::new_err(e.message))?;

        self.buffer.drain(..result.consumed);
        self.is_start = false;

        if result.finished {
            self.finished = true;
            if self.check_trailing() && !trim_ascii(&self.buffer).is_empty() {
                return Err(StreamingProjectionError::new_err(
                    "Trailing content after JSON array",
                ));
            }
            self.buffer.clear();
        }

        if result.blob == b"[]" {
            Ok(None)
        } else {
            Ok(Some(PyBytes::new(py, &result.blob).into()))
        }
    }

    /// Finish the stream and return the last projected blob, if any.
    fn finish(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        if self.finished {
            return Ok(None);
        }

        if self.buffer.is_empty() {
            if !self.array_started || self.is_start {
                return Ok(None);
            }
            return Err(StreamingProjectionError::new_err(
                "Unexpected end of JSON array",
            ));
        }

        if !self.array_started {
            if trim_ascii(&self.buffer).is_empty() {
                return Ok(None);
            }
            let prefix = self.prefix.join(".");
            return Err(StreamingProjectionError::new_err(format!(
                "Unexpected end before JSON array at root_prefix {prefix:?}"
            )));
        }

        let result = pydantic_stream_core::projection::project_array_blob_partial(
            &self.buffer,
            &self.spec,
            self.is_start,
        )
        .map_err(|e| StreamingProjectionError::new_err(e.message))?;

        self.buffer.drain(..result.consumed);
        if !result.finished {
            return Err(StreamingProjectionError::new_err(
                "Unexpected end of JSON array",
            ));
        }
        if self.check_trailing() && !trim_ascii(&self.buffer).is_empty() {
            return Err(StreamingProjectionError::new_err(
                "Trailing content after JSON array",
            ));
        }

        self.finished = true;
        self.buffer.clear();

        if result.blob == b"[]" {
            Ok(None)
        } else {
            Ok(Some(PyBytes::new(py, &result.blob).into()))
        }
    }
}

// ---------------------------------------------------------------------------
// Streaming functions (no projection / no spec required)
// ---------------------------------------------------------------------------

/// Pull complete item byte strings out of a possibly partial JSON array.
///
/// Returns ``(items, consumed_bytes, finished)``.
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

/// Return the byte offset of the ``[`` for a top-level or prefixed array.
#[pyfunction]
#[pyo3(signature = (data, prefix=None))]
fn locate_array_start(data: &[u8], prefix: Option<&str>) -> PyResult<Option<usize>> {
    let segments: Vec<&str> = match prefix {
        Some(p) if !p.is_empty() => p.split('.').collect(),
        _ => Vec::new(),
    };

    let result = pydantic_stream_core::streaming::locate_array_start(data, &segments);
    match result {
        Ok(offset) => Ok(offset),
        Err(e) => Err(StreamingProjectionError::new_err(e.message)),
    }
}

// ---------------------------------------------------------------------------
// Projection functions (require ObjectSpec)
// ---------------------------------------------------------------------------

/// Return a compact JSON object containing only fields declared in ``spec``.
#[pyfunction]
fn project_object(py: Python<'_>, data: &[u8], spec: &PyObjectSpec) -> PyResult<Py<PyAny>> {
    let result = pydantic_stream_core::projection::project_object(data, &spec.inner);
    match result {
        Ok(bytes) => Ok(PyBytes::new(py, &bytes).into()),
        Err(e) => Err(StreamingProjectionError::new_err(e.message)),
    }
}

/// Return a compact JSON array containing only fields declared in ``spec``.
#[pyfunction]
fn project_array(py: Python<'_>, data: &[u8], spec: &PyObjectSpec) -> PyResult<Py<PyAny>> {
    let result = pydantic_stream_core::projection::project_array(data, &spec.inner);
    match result {
        Ok(bytes) => Ok(PyBytes::new(py, &bytes).into()),
        Err(e) => Err(StreamingProjectionError::new_err(e.message)),
    }
}

/// Return one projected JSON object per array item.
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

/// Project JSON Lines input and return one projected JSON byte string per line.
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

/// Project only the selected items from a top-level or prefixed JSON array.
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

/// Project a top-level or prefixed JSON array and return it as compact JSON.
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

/// Low-level streaming helper for projected arrays.
///
/// Returns ``(items, consumed_bytes, finished)`` for one possibly partial
/// chunk of input.
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

/// Low-level streaming helper that returns one projected JSON array blob.
///
/// Returns ``(blob, consumed_bytes, finished)`` for one possibly partial chunk
/// of input.
#[pyfunction]
#[pyo3(signature = (data, spec, is_start=true))]
fn project_array_blob_partial(
    py: Python<'_>,
    data: &[u8],
    spec: &PyObjectSpec,
    is_start: bool,
) -> PyResult<(Py<PyAny>, usize, bool)> {
    let result =
        pydantic_stream_core::projection::project_array_blob_partial(data, &spec.inner, is_start);
    match result {
        Ok(r) => Ok((PyBytes::new(py, &r.blob).into(), r.consumed, r.finished)),
        Err(e) => Err(StreamingProjectionError::new_err(e.message)),
    }
}

/// `PyO3` module definition.
#[pymodule(gil_used = false)]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Types
    m.add_class::<PyFieldSpec>()?;
    m.add_class::<PyObjectSpec>()?;
    m.add_class::<PyProjectedArrayBlobStreamer>()?;
    m.add(
        "StreamingProjectionError",
        m.py().get_type::<StreamingProjectionError>(),
    )?;

    // Streaming (no spec)
    m.add_function(wrap_pyfunction!(extract_array_items, m)?)?;
    m.add_function(wrap_pyfunction!(locate_array_start, m)?)?;

    // Projection (requires spec)
    m.add_function(wrap_pyfunction!(project_array, m)?)?;
    m.add_function(wrap_pyfunction!(project_array_items, m)?)?;
    m.add_function(wrap_pyfunction!(project_object, m)?)?;
    m.add_function(wrap_pyfunction!(project_jsonl, m)?)?;
    m.add_function(wrap_pyfunction!(project_array_items_sliced, m)?)?;
    m.add_function(wrap_pyfunction!(project_array_nav, m)?)?;
    m.add_function(wrap_pyfunction!(project_array_items_partial, m)?)?;
    m.add_function(wrap_pyfunction!(project_array_blob_partial, m)?)?;
    Ok(())
}
