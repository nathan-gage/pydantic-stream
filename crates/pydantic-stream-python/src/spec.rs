use pyo3::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

use pydantic_stream_core::spec;

/// Low-level projection rule for one JSON field.
///
/// Most users will not build ``FieldSpec`` directly; it is mainly useful when
/// constructing an ``ObjectSpec`` by hand.
#[pyclass(frozen, from_py_object, name = "FieldSpec")]
#[derive(Clone, Debug)]
pub struct PyFieldSpec {
    pub inner: spec::FieldSpec,
}

#[pymethods]
impl PyFieldSpec {
    #[new]
    #[pyo3(signature = (output_key, nested=None))]
    fn new(output_key: String, nested: Option<PyObjectSpec>) -> Self {
        Self {
            inner: spec::FieldSpec::new(
                output_key.into_boxed_str(),
                nested.map(|s| Arc::clone(&s.inner)),
            ),
        }
    }

    #[getter]
    fn output_key(&self) -> &str {
        &self.inner.output_key
    }

    fn __repr__(&self) -> String {
        format!(
            "FieldSpec(output_key={:?}, nested={})",
            self.inner.output_key,
            if self.inner.nested.is_some() {
                "ObjectSpec(...)"
            } else {
                "None"
            }
        )
    }
}

/// Low-level projection spec for one JSON object.
///
/// ``ObjectSpec`` tells the Rust projector which input keys to keep. Most
/// users should get one from ``compile_model_spec(...)`` or
/// ``compile_object_spec(...)`` instead of constructing it manually.
#[pyclass(frozen, from_py_object, name = "ObjectSpec")]
#[derive(Clone, Debug)]
pub struct PyObjectSpec {
    pub inner: Arc<spec::ObjectSpec>,
}

#[pymethods]
impl PyObjectSpec {
    /// Build from a `dict[str, FieldSpec]`.
    #[new]
    fn new(fields_by_input_key: HashMap<String, PyFieldSpec>) -> Self {
        let spec = spec::ObjectSpec::from_fields(
            fields_by_input_key
                .into_iter()
                .map(|(k, v)| (k.into_boxed_str(), v.inner)),
        );
        Self {
            inner: Arc::new(spec),
        }
    }

    fn __repr__(&self) -> String {
        let keys: Vec<&str> = self.inner.fields.keys().map(AsRef::as_ref).collect();
        format!("ObjectSpec(keys={keys:?})")
    }

    fn __len__(&self) -> usize {
        self.inner.fields.len()
    }

    fn __contains__(&self, key: &str) -> bool {
        self.inner.fields.contains_key(key)
    }
}
