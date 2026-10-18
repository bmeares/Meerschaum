# 💼 Meerschaum Portable

!!! warning "Removed in 5.0"
    The standalone "portable" archive (`mrsm-full-*` / `mrsm-minimal-*`, built with `pyoxidizer`) is no longer produced. This page remains so old links keep working.

Use one of these instead:

- **`pip install meerschaum`** (or `pipx install meerschaum` / `uv tool install meerschaum`). The wheel contains the native core and has no required Python dependencies. During the 5.0 release candidates add `--pre`: `pip install --pre meerschaum`. See [Getting Started](/get-started).
- **A standalone `mrsm` binary**, with no Python needed for the core: `cargo install meerschaum --locked`, or the binaries attached to each GitHub release.
- **Docker**: the official `bmeares/meerschaum` image.

To keep an installation self-contained in one directory, set [`MRSM_ROOT_DIR`](/reference/environment/#mrsm_root_dir) (and optionally `MRSM_PLUGINS_DIR`) to a folder of your choice. See the [migration guide](/reference/migrating-to-v5/) for everything else that changed in v5.
