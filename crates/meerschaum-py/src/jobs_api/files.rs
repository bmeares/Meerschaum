//! `_core.RotatingFile` and `_core.StdinFile`: a job's rotating log and its stdin file.

use super::*;

/// A rotating log (`meerschaum.utils.daemon.RotatingFile`).
#[pyclass(name = "RotatingFile", module = "meerschaum._core")]
pub struct PyRotatingFile {
    inner: Mutex<RotatingFile>,
}

impl PyRotatingFile {
    fn lock(&self) -> std::sync::MutexGuard<'_, RotatingFile> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[pymethods]
impl PyRotatingFile {
    #[new]
    #[pyo3(signature = (file_path, num_files_to_keep=5, max_file_size=100000, write_timestamps=false, timestamp_format="%Y-%m-%d %H:%M".to_string()))]
    fn new(
        file_path: &str,
        num_files_to_keep: i64,
        max_file_size: i64,
        write_timestamps: bool,
        timestamp_format: String,
    ) -> PyResult<Self> {
        let file = RotatingFile::new(
            file_path,
            RotatingFileOptions {
                num_files_to_keep,
                max_file_size,
                write_timestamps,
                timestamp_format,
            },
        )
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(PyRotatingFile {
            inner: Mutex::new(file),
        })
    }

    #[getter]
    fn file_path(&self) -> String {
        path_text(self.lock().file_path().to_path_buf())
    }

    fn read(&self) -> String {
        self.lock().read()
    }

    fn readlines(&self) -> Vec<String> {
        self.lock().readlines()
    }

    fn write(&self, data: &str) -> PyResult<usize> {
        self.lock()
            .write(data)
            .map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(data.len())
    }

    #[pyo3(signature = (unused_only=false))]
    fn delete(&self, unused_only: bool) {
        self.lock().delete(unused_only);
    }

    fn close(&self) {
        self.lock().close();
    }

    fn touch(&self) -> PyResult<()> {
        self.lock()
            .touch()
            .map_err(|e| PyException::new_err(e.to_string()))
    }

    fn seek(&self, position: u64) {
        self.lock().seek(position);
    }

    fn refresh_cursor(&self) {
        self.lock().refresh_cursor();
    }

    fn get_latest_subfile_index(&self) -> i64 {
        self.lock().get_latest_subfile_index()
    }

    fn get_latest_subfile_path(&self) -> String {
        path_text(self.lock().get_latest_subfile_path())
    }

    fn get_existing_subfile_indices(&self) -> Vec<i64> {
        self.lock().get_existing_subfile_indices()
    }

    fn get_existing_subfile_paths(&self) -> Vec<String> {
        self.lock()
            .get_existing_subfile_paths()
            .into_iter()
            .map(path_text)
            .collect()
    }

    fn get_subfile_path_from_index(&self, index: i64) -> String {
        path_text(self.lock().get_subfile_path_from_index(index))
    }

    fn get_subfile_name_from_index(&self, index: i64) -> String {
        self.lock().get_subfile_name_from_index(index)
    }

    fn get_index_from_subfile_name(&self, name: &str) -> i64 {
        self.lock().get_index_from_subfile_name(name)
    }

    fn get_timestamp_prefix_str(&self) -> String {
        self.lock().get_timestamp_prefix_str()
    }
}

/// A job's stdin FIFO (`meerschaum.utils.daemon.StdinFile`).
#[pyclass(name = "StdinFile", module = "meerschaum._core")]
pub struct PyStdinFile {
    inner: Mutex<StdinFile>,
}

impl PyStdinFile {
    fn lock(&self) -> std::sync::MutexGuard<'_, StdinFile> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[pymethods]
impl PyStdinFile {
    #[new]
    #[pyo3(signature = (file_path, lock_file_path=None))]
    fn new(file_path: &str, lock_file_path: Option<&str>) -> Self {
        PyStdinFile {
            inner: Mutex::new(StdinFile::new(file_path, lock_file_path.map(PathBuf::from))),
        }
    }

    #[getter]
    fn file_path(&self) -> String {
        path_text(self.lock().file_path.clone())
    }

    #[getter]
    fn blocking_file_path(&self) -> String {
        path_text(self.lock().blocking_file_path.clone())
    }

    fn write(&self, py: Python<'_>, data: &str) -> PyResult<usize> {
        py.detach(|| self.lock().write(data.as_bytes()))
            .map_err(|e| PyException::new_err(e.to_string()))?;
        Ok(data.len())
    }

    fn readline(&self, py: Python<'_>) -> PyResult<String> {
        py.detach(|| self.lock().readline())
            .map_err(|e| PyException::new_err(e.to_string()))
    }

    fn close(&self) {
        self.lock().close();
    }

    fn is_open(&self) -> bool {
        self.lock().is_open()
    }
}
