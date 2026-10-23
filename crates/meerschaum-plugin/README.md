# Native Rust plugins

> The native plugin SDK of [Meerschaum](https://meerschaum.io). Its C ABI (`mrsm_plugin_v1`) is versioned
> and stable independently of the Rust types; also re-exported by the [`meerschaum`](https://crates.io/crates/meerschaum)
> crate behind the `plugin` feature.

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

`Request` is the low-level dispatch envelope. Its `pipe()` method provides a typed Pipe
view; the optional host-call ABI extension supports `get_sync_time()` and `sync(data)`
for the current Pipe during source fetch and sync calls. Host writes use the normal
sync engine with supplied input, avoiding recursive plugin dispatch. Arbitrary connector
access, other Pipe construction and Python helper imports are not exposed.

Build the example with `cargo build -p meerschaum-plugin --example native_demo`. Install the
result in a configured plugin directory:

```text
plugins/native_demo/plugin.toml
plugins/native_demo/libnative_demo.so
```

`plugin.toml` contains top-level `name` and `version` strings (see `examples/plugin.toml`). Instead
of installing the library you can install the crate and let Meerschaum build it (see "Source plugins").
On macOS use `libnative_demo.dylib`; on Windows install as `libnative_demo.dll` (rename Cargo's
`native_demo.dll`). `discover` accepts ordered configured directories; `environment_directories`
parses the OS path-list in `MRSM_PLUGINS_DIR`, without an implicit user-home fallback. Earlier
directories win; entries inside each directory are sorted. A directory containing both
`plugin.toml` and Python files is native. The caller must exclude discovered native names
from Python discovery. An invalid plugin (bad descriptor, missing or unloadable library, metadata mismatch) is skipped with a
`Warning: Skipping invalid native plugin at '<dir>': <reason>` on stderr and the rest keep loading
(PARITY: v4 warns about a broken Python plugin and loads the others); `discover_report` and
`cached_manifest_report` return the skipped plugins as data.

## Source plugins

A plugin directory that is a Cargo crate (`plugin.toml`, `Cargo.toml`, `src/`) and has no
up-to-date `lib<name>.<ext>` next to `plugin.toml` is built by the host (`source.rs`):
`cargo build --release --manifest-path <dir>/Cargo.toml --target-dir <build>/<name>` with
`CARGO_INCREMENTAL=0`, where `<build>` is `<root>/cache/native_build`. The plugins directory is
written only by Cargo itself, and only `Cargo.lock` when it is missing or stale (the lock written
during a build is included in the stamped fingerprint, so it does not cause a rebuild). `cargo` is `$CARGO` or the first one on `PATH`. The crate's package name must equal
the `plugin.toml` name and `[lib] crate-type` must contain `cdylib`; otherwise the plugin is
skipped as invalid. Without `cargo` the plugin is skipped with `Warning: Skipping native plugin
at '<dir>': it is a source crate and no `cargo` was found on PATH (install a Rust toolchain or
ship lib<name>.<ext>)`; a failed build is skipped with the last 20 lines of cargo's stderr. The
other plugins still load.

* **Fingerprint.** For a source plugin `Candidate::fingerprint` is the content hash of
  every file in the plugin directory except `target/`, hidden entries other than `.cargo/`, and shared
  libraries (so `include_str!` assets and `build.rs` inputs count; not mtimes,
  so a checkout does not trigger a build). After a successful build the hash is written to
  `<build>/<name>/mrsm-source-fingerprint`; a build is current when that stamp matches and the
  library exists. The manifest cache keys on the same fingerprint, so a cache hit builds nothing.
* **Library path.** `Candidate::library` is the file Cargo produced
  (`<build>/<name>/release/lib<libname>.<ext>`; hyphens in the package name become underscores);
  `Candidate::source_crate` is true and the manifest entry gets a `library` field with the path.
* **Failed builds are remembered.** A failed build writes `<build>/<name>/mrsm-build-failed` with the
  source fingerprint; until the sources change or the build directory is deleted the plugin is skipped
  with `the last build failed; edit the sources or delete '<dir>' to retry` and cargo is not run.
* **A shipped library.** If `lib<name>.<ext>` exists next to the sources it is used unless a build
  input is newer than it; when it is stale and there is no `cargo`, it is still used, with a warning
  (`Using native plugin at '<dir>': its library is older than the sources and was not rebuilt`).
* **API.** `discover_report` / `discover_report_in(dirs, build_dir)` only scan: a source plugin that
  needs building comes back with `Candidate::build` set and no library yet.
  `Discovery::build_pending` runs the builds (a failure moves the plugin to `skipped` with
  `Skipped::source_crate`). `discover`, `cached_manifest(_report)` and `NativePlugins::open` build
  before they read the manifest, using `native_build` next to the cache file. Without an explicit
  build directory the root comes from `meerschaum-core`'s `Paths::from_env` (`MRSM_ROOT_DIR`).
  Path dependencies outside the plugin directory are not fingerprinted.

## ABI v1 and ownership

Native plugins and Python workers share manifest schema 5, separately from ABI v1 and
worker protocol v1. Return `MANIFEST_SCHEMA_VERSION` from the SDK in your manifest rather
than a version literal. A prerelease native library exporting schema 3 must be rebuilt
against the current SDK (and its SDK dependency updated); refreshing the manifest cache
cannot rewrite the library's exported `describe` metadata. Aggregate caches from an older
schema are regenerated automatically. Unsupported manifest versions are rejected.

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
* Full SDK frame data never uses JSON: stream callbacks exchange `FFI_ArrowArray` and `FFI_ArrowSchema`, with
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

## Validation and remaining work

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

Host integration is done (see "Hosting" below). The `mrsm` binary tests in
`crates/meerschaum-cli/tests/native_plugins.rs` and `native_e2e.rs` drive the examples end to
end (actions, hooks around a real sync, routes served by `start api`, a native plugin replacing a
Python plugin of the same name, rejection of corrupt or wrong-ABI libraries, the manifest cache),
and `tests/console/test_console.py` renders a native page in Chromium. Examples: `native_demo` (source,
instance, action, hooks, routes, page), `native_probe` (a plain fetch source whose hooks append to
`NATIVE_PROBE_LOG`, for asserting the built-in sync path) and `bad_abi`. `tests/source.rs` covers source plugins with a stand-in `cargo`;
`crates/meerschaum-cli/tests/native_source_plugin.rs` builds a real crate. Not yet available: repository
artifact upload, target-triple selection and installation (the installer's job), and continuous
`subscribe` / lazy streaming, which need a reader/sink SDK beyond the current eager frame API. The future wasm contract is sketched in
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
  `{path, function, query, username}` and returns `{html, status?}` (a trusted fragment). The Dash console of the Python package fetches the fragment
  from the server and frames it in its chrome; a standalone install shows no console, so the page is only reachable there.

Request and response bodies of routes are UTF-8 text.

### Single-file source plugins

Place `readings.rs` directly in `MRSM_PLUGINS_DIR`:

```rust
fn fetch(pipe: &Pipe, end: Option<Bound>) -> Result<Vec<Value>> {
    let last = pipe.get_sync_time()?;
    Ok(vec![json!({"id": 1, "previous": last, "end": end})])
}
```

The generated adapter accepts any subset of named `pipe: &Pipe`, `begin: Option<Bound>`
and `end: Option<Bound>` arguments, in any order, including no arguments. `sync` additionally
accepts `data: Option<Frame>`. Leading underscores are allowed. Unsupported names and async,
unsafe, conditional or generic entry points produce discovery errors. `Request` is not part
of the single-file author interface.

Fetch returns rows (`Vec<Value>`), `Frame` or `Option<Frame>`, optionally wrapped in `Result`.
Sync returns `SuccessTuple` or `(bool, String)` / `(bool, &str)`, optionally wrapped in
`Result`. Both functions may exist; sync overrides fetch in the engine. `Pipe`, `Bound`,
`SuccessTuple`, JSON `Value`, `json!`, `Frame`, `Result` and `Error` are implicitly imported.

`pipe.get_sync_time()` reads the newest axis value; `pipe.sync(rows)` writes supplied rows
through the host's sync engine and returns `Result<SuccessTuple>`. A zero-argument Pipe sync
is not provided, to avoid recursively invoking the same plugin. Callback state is scoped
to the current call; Pipe views cannot move to other threads. Recursive plugin calls fail
instead of blocking on the library's serialized call lock.

Meerschaum generates the crate, manifest and ABI adapter under the root's build cache.
Cargo is required to compile it; unchanged builds are reused. Use a full crate for extra
dependencies or capabilities beyond fetch and sync.

### Lightweight single-file builds

The generated single-file crate embeds a small runtime with only Serde and `serde_json` as
dependencies. It uses the optional `mrsm_plugin_invoke_rows_v1` C ABI extension: request,
result and row JSON bytes, plus call-scoped host callbacks for Pipe operations. Host
conversion owns the full data kernel. No Polars, Arrow, connector, loader or async runtime
dependency is compiled into these plugins. The original v1 Arrow ABI is unchanged.

Single-file `Frame` is a JSON row container (`from_rows`, `rows`, `into_rows`, `height`),
not the full SDK's typed frame. JSON loses frame schemas and converts special scalar
values to their JSON representations; choose a full Cargo plugin for typed Arrow
interchange and optimized data processing. Simple builds use `opt-level = 0` and
share a Cargo target cache across plugins; the installed libraries and stamps remain
separate. Saved compiler diagnostics survive failure caching.
