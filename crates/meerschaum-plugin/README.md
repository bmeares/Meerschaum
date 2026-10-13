# Native Rust plugins

`meerschaum-plugin` implements the native plugin boundary from design §9. It depends on
core, kernel and connector; it does not depend on the engine, CLI, worker or an instance
backend. `examples/native_demo.rs` is a complete source connector and in-memory instance,
with an action, arguments and sync hooks. No Python process is needed.

## Authoring and installation

Implement the safe `Plugin` trait and call `meerschaum_plugin::export_plugin!(MyPlugin)` once
in a `cdylib` crate. `fetch` returns `Option<Frame>`; `sync` accepts optional data and returns
JSON (normally a serialized `SuccessTuple`). Override `call` for instance methods, actions,
register, setup and hooks. `Request` contains the method, connector type, label, config and
context; every hook kwarg is forwarded unchanged inside `context.kwargs`. Constructors and
calls are enclosed in `catch_unwind`, including serialization and frame conversion. Build
with `panic = "unwind"`; an aborting panic, process exit or native memory fault cannot be
recovered by the host.

The SDK constructs `Default` for each call, so plugin structs are stateless handlers. Keep
shared state in a synchronized store keyed by connector label/config. The example's single
static frame is intentionally only a small demonstration, not a general instance backend.
The SDK imposes no async runtime. Native calls are synchronous; async applications should
schedule potentially blocking calls on their blocking executor.

Build the example with `cargo build -p meerschaum-plugin --example native_demo`. Install the
result in a configured plugin directory:

```text
plugins/native_demo/plugin.toml
plugins/native_demo/libnative_demo.so
```

`plugin.toml` contains top-level `name` and `version` strings (see `examples/plugin.toml`).
On macOS use `libnative_demo.dylib`; on Windows install as `libnative_demo.dll` (rename Cargo's
`native_demo.dll`). `discover` accepts ordered configured directories; `environment_directories`
parses the OS path-list in `MRSM_PLUGINS_DIR`, without an implicit user-home fallback. Earlier
directories win; entries inside each directory are sorted. A directory containing both
`plugin.toml` and Python files is native. The caller must exclude discovered native names
from Python discovery. Missing libraries and invalid descriptors return errors.

## ABI v1 and ownership

The only exported symbol is `mrsm_plugin_v1`, returning a static `#[repr(C)] MrsmPluginV1`.
The permanent prefix contains two `u32`s: ABI version and structure size. The host reads only
this prefix before rejecting incompatible or truncated tables. Breaking changes require a
new symbol (`mrsm_plugin_v2`) and version; extending the JSON method namespace is additive.
The host should keep the v1 loader for at least one major release after v2 is introduced.

We use a hand-written C ABI instead of `abi_stable`: a small set of C primitives, explicit
allocator callbacks and the standardized Arrow interface suffices, without coupling plugins
to Rust trait-object layouts, compiler versions, async runtimes or an additional ABI type
system. Host and plugin must still target the same OS and architecture. The vtable uses
`describe`, `invoke` and `free_bytes`. Metadata (name, version and ABI version) and the manifest
come from `describe`. `invoke` multiplexes method names instead of adding a function pointer
for every connector method; this deliberately refines the illustrative per-method vtable in
§9.1 while retaining versioned C calls and the same operations.

* Borrowed `Bytes` remain owned by the caller until the call returns. JSON is UTF-8.
* `CallResult` has status 0 (success JSON), 1 (error UTF-8), 2 (caught panic UTF-8),
  or 3 (unsupported method UTF-8, mapped to `ConnectorError::NotImplemented`).
  Every payload is an exporting-library allocation, released exactly once using `free_bytes`.
* The input and output `FFI_ArrowArrayStream` pointers are valid, exclusive and non-null.
  Output initially has no release callback. The receiver moves input, leaving an empty stream.
  On every outcome the caller releases any remaining input/output stream.
* Data never uses JSON: stream callbacks exchange `FFI_ArrowArray` and `FFI_ArrowSchema`, with
  their standard producer-owned release callbacks. SDK output is an eager, one-batch stream;
  arbitrary conforming C plugins may return multiple batches. SDK callbacks contain only
  Arrow-owned schema/array operations over already materialized frames, not author code.
* The host consumes streams and materializes kernel frames while the library is alive.
  No borrowed callback or imported Arrow allocation escapes the call. The loader retains a
  library handle in every adapter and serializes calls through its mutex. Plugins must not
  leave background threads or callbacks running after their last owner drops.
* `LoadedPlugin::load` and `cached_manifest` are unsafe because executing arbitrary native
  code requires trust and ABI conformance. All SDK author methods and loaded-plugin calls
  are safe. This is an in-process ABI, not a security sandbox.

`LoadedPlugin::source` returns the standard `plugin:<name>` source adapter, without requiring
a custom connector declaration. `LoadedPlugin::connector` returns a `NativeConnector` implementing both connector traits;
check `is_instance()` before registering it as an instance. Required and optional pipe,
maintenance and lease operations are dispatched over the ABI. Generic user/plugin/token
operations remain the connector trait's implementations over internal pipes. `call` exposes
additional operations; `LoadedPlugin::action` and `hook` offer typed `SuccessTuple` results.

The control context for pipe operations is `{ "pipe": { "keys": ..., "parameters": ...,
"temporary": ... } }`. Reads add `query` with every `DataQuery` field. Sync adds `options`
with every `SyncOptions` field. Bounds are integers, ISO timestamp strings or null. Other
method arguments retain their Rust/Python names as top-level context fields; lease TTL is
`ttl_seconds`. Action/hook context is `{ "name": ..., "kwargs": ... }`. Results use ordinary
JSON, including `[bool, string]` success tuples, null missing values, list/dict pipe keys,
and integer/string IDs. Data results use only the output stream.

## Manifest and cache

The bundled schema is an exact copy of `meerschaum-worker/schema/manifest.schema.json`,
checked against that source by an integration test. Loading validates the full schema and
metadata identity. Actions include docstrings; arguments retain argparse's manifest fields;
hooks and connector types share the Python manifest's shape. Native manifests use empty
`python_version` and null `python_executable`.

`cached_manifest(directories, cache_path)` avoids loading code when fingerprints match.
Use a separate native cache file outside plugin trees; merge it into the worker manifest in
the caller. Fingerprints use the worker host's SHA-256 byte grammar: plugin name, sorted
relative file paths, sizes and nanosecond mtimes, excluding bytecode and hidden directories.
The aggregate uses schema version, names and per-plugin fingerprints in discovery order.
The cache is written through a unique sibling temporary file and rename. Native trees reject
symlinks (cycles and external files are not tracked). This is a native-only restriction;
Python plugin discovery is unchanged. Install by replacement when no library is loaded;
this API does not promise live reload of an already-open library.

## Validation and remaining integration

Run with fresh temporary `MRSM_ROOT_DIR` and empty `MRSM_PLUGINS_DIR`:

```sh
cargo fmt -p meerschaum-plugin
cargo clippy -p meerschaum-plugin --all-targets -- -D warnings
cargo test -p meerschaum-plugin
```

Integration tests build real example cdylibs, load them, fetch and sync nullable Arrow data
into the in-memory instance, exercise actions/hooks and panic recovery, reject a deliberately
short incompatible ABI table, validate the worker JSON schema and test discovery/cache
invalidation. Test subprocesses receive their own temporary roots. No database or Python
reference is used: v4 had no native ABI; the reused connector return shapes and manifest
schema are the compatibility surfaces.

The CLI/engine/PyO3 owners still need to merge native discovery/manifests, register adapters,
route actions/hooks and prefer native entries over Python. Repository artifact upload,
target-triple selection and installation belong to the installer. Server routes/pages are
not exposed by this SDK yet. Continuous `subscribe` and lazy streaming need a reader/sink SDK
beyond the current eager frame API. The future wasm contract is sketched in
`wit/plugin.wit` (not compiled); it replaces native Arrow pointers with Arrow IPC bytes.

## Hosting (wave 6)

`NativePlugins::open(dirs, cache)` is the process-level entry for hosts: it discovers native
plugins, reads or rebuilds the cached manifest without loading code on a fingerprint hit, and
loads each library lazily (`plugin(name)`, `connector(type, label, config)`, `source(name)`,
`hooks(post)`). The CLI merges its manifest into the Python manifest (a native plugin shadows
a Python plugin of the same name), dispatches native actions in-process, resolves native
connectors and `sync()` for the engine, and runs native sync hooks.

Server extensions use additive JSON methods through `LoadedPlugin::call_json`:

- `routes` (no context) returns `[{"method", "path", "public"?}]`; the manifest entry must list
  at least one name in `api_plugins` so the host knows to ask. `route` receives
  `{method, path, query, body, user}` and returns `{status, content_type, body}`. Routes need
  the server's credentials unless `"public": true` or the server runs `--no-auth`; paths under
  `/dash`, `/webterm`, or a core route group are rejected.
- Console pages are the manifest's `web_pages` entries; `page` receives
  `{path, function, query, username}` and returns `{html, status?}` (a trusted fragment).

Request and response bodies of routes are UTF-8 text.
