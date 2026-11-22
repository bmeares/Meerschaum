//! `_core.ApiResponse`: a response in the shape plugin code uses (`requests.Response`'s common
//! surface).

use super::*;

/// A response in the shape plugin code uses (`requests.Response`'s common surface).
#[pyclass(frozen, name = "ApiResponse", module = "meerschaum._core")]
pub struct PyApiResponse {
    pub(super) inner: ApiResponse,
    pub(super) url: String,
}

#[pymethods]
impl PyApiResponse {
    #[getter]
    fn status_code(&self) -> u16 {
        self.inner.status
    }

    #[getter]
    fn ok(&self) -> bool {
        self.inner.ok()
    }

    #[getter]
    fn text(&self) -> String {
        self.inner.text()
    }

    #[getter]
    fn content<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.inner.bytes)
    }

    #[getter]
    fn url(&self) -> String {
        self.url.clone()
    }

    #[getter]
    fn headers<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let d = PyDict::new(py);
        d.set_item("Content-Type", &self.inner.content_type)?;
        Ok(d)
    }

    #[pyo3(signature = (**_kw))]
    fn json<'py>(
        &self,
        py: Python<'py>,
        _kw: Option<&Bound<'py, PyDict>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        py.import("json")?
            .call_method1("loads", (self.inner.text(),))
    }

    fn raise_for_status(&self) -> PyResult<()> {
        if self.inner.ok() {
            return Ok(());
        }
        let kind = if self.inner.status < 500 {
            "Client"
        } else {
            "Server"
        };
        Err(PyIOError::new_err(format!(
            "{} {kind} Error for url: {}",
            self.inner.status, self.url
        )))
    }

    fn __bool__(&self) -> bool {
        self.inner.ok()
    }

    fn __repr__(&self) -> String {
        format!("<Response [{}]>", self.inner.status)
    }
}
