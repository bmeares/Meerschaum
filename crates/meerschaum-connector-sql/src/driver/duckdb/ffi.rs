//! Runtime binding to DuckDB's C API (`libduckdb`): only the functions the driver uses.
//!
//! The library is DuckDB's own release build, loaded with `dlopen` / `LoadLibrary` by
//! [`crate::duckdb_lib`] (never linked). Every function is resolved when the library is loaded, so
//! a library that lacks one is refused up front instead of crashing on first use. This module
//! owns all of the driver's `unsafe` code.
#![allow(unsafe_code)]

use std::ffi::{CStr, CString, c_char, c_int, c_void};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

pub type Ptr = *mut c_void;
pub type Idx = u64;

#[repr(C)]
pub struct RawResult {
    deprecated_column_count: Idx,
    deprecated_row_count: Idx,
    deprecated_rows_changed: Idx,
    deprecated_columns: *mut c_void,
    deprecated_error_message: *mut c_char,
    internal_data: *mut c_void,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct DuckTimestamp {
    pub micros: i64,
}
#[repr(C)]
#[derive(Clone, Copy)]
pub struct DuckDate {
    pub days: i32,
}

macro_rules! api {
    ($( $name:ident : fn($($arg:ty),*) $(-> $ret:ty)? ),* $(,)?) => {
        #[allow(non_snake_case)]
        pub struct Api {
            _lib: libloading::Library,
            /// Where the library was loaded from (`libduckdb.so` for the system loader path).
            pub path: PathBuf,
            /// `duckdb_library_version()`, e.g. `v1.5.6`.
            pub version: String,
            $( pub $name: unsafe extern "C" fn($($arg),*) $(-> $ret)?, )*
        }
        impl Api {
            fn bind(lib: libloading::Library, path: PathBuf) -> Result<Api, String> {
                // SAFETY: each symbol is declared with the signature of the C API function
                // of the same name (duckdb.h); the library is kept alive in `_lib` for the
                // whole process (the `Api` lives in a `static`).
                unsafe {
                    $(
                        let $name = *lib
                            .get::<unsafe extern "C" fn($($arg),*) $(-> $ret)?>(
                                concat!(stringify!($name), "\0").as_bytes(),
                            )
                            .map_err(|e| format!("The DuckDB library lacks `{}` ({e}).", stringify!($name)))?;
                    )*
                    Ok(Api { _lib: lib, path, version: String::new(), $( $name, )* })
                }
            }
        }
    };
}

api! {
    duckdb_library_version: fn() -> *const c_char,
    duckdb_create_config: fn(*mut Ptr) -> c_int,
    duckdb_set_config: fn(Ptr, *const c_char, *const c_char) -> c_int,
    duckdb_destroy_config: fn(*mut Ptr),
    duckdb_open_ext: fn(*const c_char, *mut Ptr, Ptr, *mut *mut c_char) -> c_int,
    duckdb_close: fn(*mut Ptr),
    duckdb_connect: fn(Ptr, *mut Ptr) -> c_int,
    duckdb_disconnect: fn(*mut Ptr),
    duckdb_prepare: fn(Ptr, *const c_char, *mut Ptr) -> c_int,
    duckdb_prepare_error: fn(Ptr) -> *const c_char,
    duckdb_destroy_prepare: fn(*mut Ptr),
    duckdb_bind_value: fn(Ptr, Idx, Ptr) -> c_int,
    duckdb_execute_prepared: fn(Ptr, *mut RawResult) -> c_int,
    duckdb_query: fn(Ptr, *const c_char, *mut RawResult) -> c_int,
    duckdb_destroy_result: fn(*mut RawResult),
    duckdb_result_error: fn(*mut RawResult) -> *const c_char,
    duckdb_rows_changed: fn(*mut RawResult) -> Idx,
    duckdb_column_count: fn(*mut RawResult) -> Idx,
    duckdb_column_name: fn(*mut RawResult, Idx) -> *const c_char,
    duckdb_column_logical_type: fn(*mut RawResult, Idx) -> Ptr,
    duckdb_fetch_chunk: fn(RawResult) -> Ptr,
    duckdb_destroy_data_chunk: fn(*mut Ptr),
    duckdb_data_chunk_get_size: fn(Ptr) -> Idx,
    duckdb_data_chunk_get_vector: fn(Ptr, Idx) -> Ptr,
    duckdb_vector_get_data: fn(Ptr) -> *mut c_void,
    duckdb_vector_get_validity: fn(Ptr) -> *mut u64,
    duckdb_list_vector_get_child: fn(Ptr) -> Ptr,
    duckdb_struct_vector_get_child: fn(Ptr, Idx) -> Ptr,
    duckdb_array_vector_get_child: fn(Ptr) -> Ptr,
    duckdb_get_type_id: fn(Ptr) -> c_int,
    duckdb_vector_get_column_type: fn(Ptr) -> Ptr,
    duckdb_destroy_logical_type: fn(*mut Ptr),
    duckdb_decimal_width: fn(Ptr) -> u8,
    duckdb_decimal_scale: fn(Ptr) -> u8,
    duckdb_decimal_internal_type: fn(Ptr) -> c_int,
    duckdb_enum_internal_type: fn(Ptr) -> c_int,
    duckdb_enum_dictionary_value: fn(Ptr, Idx) -> *mut c_char,
    duckdb_array_type_array_size: fn(Ptr) -> Idx,
    duckdb_struct_type_child_count: fn(Ptr) -> Idx,
    duckdb_struct_type_child_name: fn(Ptr, Idx) -> *mut c_char,
    duckdb_free: fn(*mut c_void),
    duckdb_create_null_value: fn() -> Ptr,
    duckdb_create_bool: fn(bool) -> Ptr,
    duckdb_create_int64: fn(i64) -> Ptr,
    duckdb_create_double: fn(f64) -> Ptr,
    duckdb_create_varchar_length: fn(*const c_char, Idx) -> Ptr,
    duckdb_create_blob: fn(*const u8, Idx) -> Ptr,
    duckdb_create_timestamp: fn(DuckTimestamp) -> Ptr,
    duckdb_create_timestamp_tz: fn(DuckTimestamp) -> Ptr,
    duckdb_create_date: fn(DuckDate) -> Ptr,
    duckdb_destroy_value: fn(*mut Ptr),
}

/// The oldest DuckDB the driver supports (the C API it binds is stable since 1.4).
pub const MIN_VERSION: (u32, u32) = (1, 4);

static API: OnceLock<Api> = OnceLock::new();

pub fn parse_version(v: &str) -> Option<(u32, u32, u32)> {
    let mut it = v.trim().trim_start_matches('v').split(['.', '-', ' ']);
    Some((
        it.next()?.parse().ok()?,
        it.next()?.parse().ok()?,
        it.next().and_then(|p| p.parse().ok()).unwrap_or(0),
    ))
}

/// The loaded library, if [`load`] succeeded.
pub fn loaded() -> Option<&'static Api> {
    API.get()
}

/// Open the library. On Windows only the application, system and user directories (and the DLL's
/// own directory for an absolute path) are searched, never the current directory, so a planted DLL
/// is not picked up; macOS callers only pass absolute paths (see `duckdb_lib`), and a bare name
/// on Linux goes through the loader path (never the working directory).
///
/// # Safety
/// Runs the initializers of the library at `path`.
unsafe fn open_library(path: &Path) -> Result<libloading::Library, libloading::Error> {
    #[cfg(windows)]
    {
        use libloading::os::windows::{
            LOAD_LIBRARY_SEARCH_DEFAULT_DIRS, LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR, Library,
        };
        let mut flags = LOAD_LIBRARY_SEARCH_DEFAULT_DIRS;
        if path.is_absolute() {
            flags |= LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR;
        }
        // SAFETY: forwarded to the caller.
        unsafe { Library::load_with_flags(path, flags) }.map(Into::into)
    }
    #[cfg(not(windows))]
    {
        // SAFETY: forwarded to the caller.
        unsafe { libloading::Library::new(path) }
    }
}

/// Load the library once per process. `path` is a file path or, for the system loader path, a
/// bare file name such as `libduckdb.so`.
pub fn load(path: &Path) -> Result<&'static Api, String> {
    if let Some(api) = API.get() {
        return Ok(api);
    }
    // SAFETY: loading a shared library runs its initializers. The path is the user's own
    // `MRSM_DUCKDB_LIB`, the cache file we wrote after a SHA-256 check, or the system loader path
    // (the same trust as any dynamically linked dependency). `libloading` opens it with
    // `RTLD_LOCAL`, so its symbols never interpose on other libraries in the process (the Python
    // `duckdb` package, for instance).
    let lib = unsafe { open_library(path) }.map_err(|e| {
        let mut message = format!(
            "Could not load the DuckDB library '{}': {e}",
            path.display()
        );
        let text = e.to_string();
        if text.contains("libstdc++") || text.contains("libgcc_s") || text.contains("GLIBCXX") {
            // A minimal host or container without the C++ runtime.
            message.push_str(" (DuckDB's library needs the C++ runtime: install libstdc++)");
        }
        message
    })?;
    let mut api = Api::bind(lib, path.to_path_buf())?;
    // SAFETY: returns a static NUL-terminated string.
    let ver = unsafe { CStr::from_ptr((api.duckdb_library_version)()) }
        .to_string_lossy()
        .into_owned();
    match parse_version(&ver) {
        Some((a, b, _)) if (a, b) >= MIN_VERSION => {}
        _ => {
            return Err(format!(
                "The DuckDB library '{}' is {ver}; Meerschaum needs v{}.{} or newer.",
                path.display(),
                MIN_VERSION.0,
                MIN_VERSION.1
            ));
        }
    }
    api.version = ver;
    Ok(API.get_or_init(|| api))
}

pub fn api() -> &'static Api {
    API.get()
        .expect("the DuckDB library is loaded before any handle exists")
}

fn cstr(p: *const c_char) -> String {
    if p.is_null() {
        String::new()
    } else {
        // SAFETY: DuckDB returns NUL-terminated strings.
        unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
    }
}

pub struct Database(Ptr, u32);
// SAFETY: a `duckdb_database` is DuckDB's thread-safe instance object: connections to it may be
// opened from any thread, and the handle is only closed once, in `Drop`, after every
// `Connection` (each holds an `Arc<Database>`) is gone.
unsafe impl Send for Database {}
// SAFETY: the only operations on a shared `&Database` are `connect` (documented thread-safe by
// DuckDB) and reading the pointer; nothing mutates the handle.
unsafe impl Sync for Database {}

impl Database {
    fn check_pid(&self) -> Result<(), String> {
        if self.1 != std::process::id() {
            return Err("DuckDB handles belong to the parent process; start a fresh interpreter (spawn or exec).".into());
        }
        Ok(())
    }
    /// Open (or create) the database at `path` (`None`: in memory). With `no_auto_install`,
    /// DuckDB's own extension auto-installer is off (`MRSM_NO_AUTO_INSTALL` forbids every
    /// implicit install, including `INSTALL` of a known extension from DuckDB's repository);
    /// extensions already installed still auto-load.
    pub fn open(path: Option<&str>, no_auto_install: bool) -> Result<Database, String> {
        let a = api();
        let c_path = path
            .map(|p| CString::new(p).map_err(|e| e.to_string()))
            .transpose()?;
        let mut cfg: Ptr = std::ptr::null_mut();
        let mut db: Ptr = std::ptr::null_mut();
        let mut err: *mut c_char = std::ptr::null_mut();
        // SAFETY: out-pointers are valid locals; the config is destroyed on every path.
        unsafe {
            let have_cfg = (a.duckdb_create_config)(&mut cfg) == 0;
            if have_cfg && no_auto_install {
                // Best effort: an option an older library does not know is not an error.
                (a.duckdb_set_config)(
                    cfg,
                    c"autoinstall_known_extensions".as_ptr(),
                    c"false".as_ptr(),
                );
            }
            let state = (a.duckdb_open_ext)(
                c_path.as_ref().map_or(std::ptr::null(), |c| c.as_ptr()),
                &mut db,
                if have_cfg { cfg } else { std::ptr::null_mut() },
                &mut err,
            );
            if have_cfg {
                (a.duckdb_destroy_config)(&mut cfg);
            }
            if state != 0 {
                let msg = cstr(err);
                if !err.is_null() {
                    (a.duckdb_free)(err.cast());
                }
                return Err(msg);
            }
        }
        Ok(Database(db, std::process::id()))
    }

    pub fn connect(self: &std::sync::Arc<Self>) -> Result<Connection, String> {
        self.check_pid()?;
        let mut c: Ptr = std::ptr::null_mut();
        // SAFETY: `self.0` is an open database; `c` is a valid out-pointer.
        let state = unsafe { (api().duckdb_connect)(self.0, &mut c) };
        if state != 0 {
            return Err("DuckDB could not open a connection.".into());
        }
        Ok(Connection {
            ptr: c,
            _db: self.clone(),
        })
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        if self.1 != std::process::id() {
            return;
        }
        // SAFETY: closes the handle once; every `Connection` holds an `Arc` so none outlive it.
        unsafe { (api().duckdb_close)(&mut self.0) }
    }
}

pub struct Connection {
    ptr: Ptr,
    _db: std::sync::Arc<Database>,
}
// SAFETY: a connection is used by one thread at a time (callers hold a mutex).
unsafe impl Send for Connection {}

impl Drop for Connection {
    fn drop(&mut self) {
        if self._db.check_pid().is_err() {
            return;
        }
        // SAFETY: disconnects once.
        unsafe { (api().duckdb_disconnect)(&mut self.ptr) }
    }
}

pub struct Statement(Ptr);

impl Connection {
    pub fn prepare(&self, sql: &str) -> Result<Statement, String> {
        self._db.check_pid()?;
        let c = CString::new(sql).map_err(|_| "The SQL contains a NUL byte.".to_string())?;
        let mut s: Ptr = std::ptr::null_mut();
        // SAFETY: valid connection, NUL-terminated SQL, out-pointer.
        unsafe {
            let state = (api().duckdb_prepare)(self.ptr, c.as_ptr(), &mut s);
            if state != 0 {
                let msg = cstr((api().duckdb_prepare_error)(s));
                (api().duckdb_destroy_prepare)(&mut s);
                return Err(msg);
            }
        }
        Ok(Statement(s))
    }

    /// Run SQL directly (several statements allowed).
    pub fn batch(&self, sql: &str) -> Result<QueryResult, String> {
        self._db.check_pid()?;
        let c = CString::new(sql).map_err(|_| "The SQL contains a NUL byte.".to_string())?;
        let mut r = QueryResult::empty();
        // SAFETY: valid connection and SQL; `r` is destroyed by its Drop even on error.
        let state = unsafe { (api().duckdb_query)(self.ptr, c.as_ptr(), &mut r.raw) };
        r.check(state)?;
        Ok(r)
    }
}

pub enum Param {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    Bytes(Vec<u8>),
    Timestamp(i64),
    TimestampTz(i64),
    Date(i32),
}

impl Statement {
    pub fn execute(&self, params: &[Param]) -> Result<QueryResult, String> {
        let a = api();
        for (i, p) in params.iter().enumerate() {
            // SAFETY: the created value is destroyed right after binding (bind copies it).
            unsafe {
                let mut v = match p {
                    Param::Null => (a.duckdb_create_null_value)(),
                    Param::Bool(b) => (a.duckdb_create_bool)(*b),
                    Param::Int(n) => (a.duckdb_create_int64)(*n),
                    Param::Float(f) => (a.duckdb_create_double)(*f),
                    Param::Text(s) => {
                        (a.duckdb_create_varchar_length)(s.as_ptr().cast(), s.len() as Idx)
                    }
                    Param::Bytes(b) => (a.duckdb_create_blob)(b.as_ptr(), b.len() as Idx),
                    Param::Timestamp(m) => {
                        (a.duckdb_create_timestamp)(DuckTimestamp { micros: *m })
                    }
                    Param::TimestampTz(m) => {
                        (a.duckdb_create_timestamp_tz)(DuckTimestamp { micros: *m })
                    }
                    Param::Date(d) => (a.duckdb_create_date)(DuckDate { days: *d }),
                };
                let state = (a.duckdb_bind_value)(self.0, (i + 1) as Idx, v);
                (a.duckdb_destroy_value)(&mut v);
                if state != 0 {
                    return Err(format!("DuckDB could not bind parameter ${}.", i + 1));
                }
            }
        }
        let mut r = QueryResult::empty();
        // SAFETY: valid statement; `r` is destroyed by its Drop.
        let state = unsafe { (a.duckdb_execute_prepared)(self.0, &mut r.raw) };
        r.check(state)?;
        Ok(r)
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        // SAFETY: destroys once.
        unsafe { (api().duckdb_destroy_prepare)(&mut self.0) }
    }
}

pub struct QueryResult {
    raw: RawResult,
}

impl QueryResult {
    fn empty() -> QueryResult {
        QueryResult {
            raw: RawResult {
                deprecated_column_count: 0,
                deprecated_row_count: 0,
                deprecated_rows_changed: 0,
                deprecated_columns: std::ptr::null_mut(),
                deprecated_error_message: std::ptr::null_mut(),
                internal_data: std::ptr::null_mut(),
            },
        }
    }

    fn check(&mut self, state: c_int) -> Result<(), String> {
        if state == 0 {
            return Ok(());
        }
        // SAFETY: a failed result carries an error string.
        Err(cstr(unsafe { (api().duckdb_result_error)(&mut self.raw) }))
    }

    pub fn rows_changed(&mut self) -> u64 {
        // SAFETY: valid result.
        unsafe { (api().duckdb_rows_changed)(&mut self.raw) }
    }

    pub fn column_count(&mut self) -> usize {
        // SAFETY: valid result.
        unsafe { (api().duckdb_column_count)(&mut self.raw) as usize }
    }

    pub fn column_name(&mut self, i: usize) -> String {
        // SAFETY: valid result; the name is owned by the result.
        cstr(unsafe { (api().duckdb_column_name)(&mut self.raw, i as Idx) })
    }

    pub fn column_type(&mut self, i: usize) -> LogicalType {
        // SAFETY: the returned logical type is owned by the caller (destroyed in Drop).
        LogicalType(unsafe { (api().duckdb_column_logical_type)(&mut self.raw, i as Idx) })
    }

    /// The next data chunk, or `None` at the end.
    pub fn next_chunk(&mut self) -> Option<Chunk> {
        // SAFETY: `duckdb_fetch_chunk` takes the result by value (a bitwise copy that shares
        // the internal state, as the C API intends) and returns an owned chunk or NULL.
        let p = unsafe {
            (api().duckdb_fetch_chunk)(RawResult {
                deprecated_column_count: self.raw.deprecated_column_count,
                deprecated_row_count: self.raw.deprecated_row_count,
                deprecated_rows_changed: self.raw.deprecated_rows_changed,
                deprecated_columns: self.raw.deprecated_columns,
                deprecated_error_message: self.raw.deprecated_error_message,
                internal_data: self.raw.internal_data,
            })
        };
        (!p.is_null()).then_some(Chunk(p))
    }
}

impl Drop for QueryResult {
    fn drop(&mut self) {
        // SAFETY: destroys once (also valid for a failed or empty result).
        unsafe { (api().duckdb_destroy_result)(&mut self.raw) }
    }
}

pub struct LogicalType(pub Ptr);
impl LogicalType {
    pub fn id(&self) -> i32 {
        // SAFETY: valid type handle.
        unsafe { (api().duckdb_get_type_id)(self.0) }
    }
}
impl Drop for LogicalType {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: destroys once.
            unsafe { (api().duckdb_destroy_logical_type)(&mut self.0) }
        }
    }
}

pub struct Chunk(Ptr);
impl Chunk {
    pub fn len(&self) -> usize {
        // SAFETY: valid chunk.
        unsafe { (api().duckdb_data_chunk_get_size)(self.0) as usize }
    }
    pub fn vector(&self, col: usize) -> Ptr {
        // SAFETY: valid chunk; the vector is owned by the chunk.
        unsafe { (api().duckdb_data_chunk_get_vector)(self.0, col as Idx) }
    }
}
impl Drop for Chunk {
    fn drop(&mut self) {
        // SAFETY: destroys once.
        unsafe { (api().duckdb_destroy_data_chunk)(&mut self.0) }
    }
}
