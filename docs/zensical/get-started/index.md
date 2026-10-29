# Get started

Your first Pipe needs Python and a few local rows. No database server or Docker setup is required.

## Install

Use Python **3.11 or newer** in your usual virtual environment:

```bash
python -m pip install --upgrade meerschaum
mrsm --version
```

The package includes the native core and the `mrsm` command. `python -m meerschaum` works too.

??? info "Using DataFrames in Python?"
    New installations return **Polars** DataFrames. Install the matching extra:

    ```bash
    python -m pip install 'meerschaum[polars]'
    ```

    For pandas, install `meerschaum[pandas]` and choose `dataframe: pandas` in `meerschaum.yaml`. Existing roots from v4 keep pandas. You can also read rows as plain dictionaries with `pipe.get_data(as_docs=True)`.

    See [Reading data](../reference/pipes/reading.md#dataframe-library).

## Make a workspace

Keep the tutorial's configuration and SQLite database together:

=== "Linux / macOS"

    ```bash
    mkdir mrsm-demo
    cd mrsm-demo
    mkdir .meerschaum
    export MRSM_ROOT_DIR="$PWD/.meerschaum"
    ```

=== "PowerShell"

    ```powershell
    mkdir mrsm-demo
    cd mrsm-demo
    mkdir .meerschaum
    $env:MRSM_ROOT_DIR = "$PWD/.meerschaum"
    ```

`sql:local` is a built-in SQLite connector. It stores your data inside this workspace:

```bash
mrsm show pipes -i sql:local
```

You should see **No pipes to show**. Keep this terminal open for the next steps.

[Build your first Pipe →](bootstrap.md){ .md-button .md-button--primary }

??? info "Other ways to run Meerschaum"
    **Your database:** add a [SQL connector](../reference/connectors/sql-connectors.md). The [stack](../reference/stack/index.md) supplies a ready-to-use TimescaleDB database through Docker.

    **Standalone command:** `cargo install meerschaum --locked` builds the stable release's native CLI. Prebuilt binaries are listed on [GitHub Releases](https://github.com/bmeares/Meerschaum/releases).

    **Docker:** see the [image variants](../reference/stack/index.md#docker-images).

    **macOS binaries:** downloaded binaries are unsigned. If Gatekeeper blocks one, remove its quarantine flag with `xattr -d com.apple.quarantine ./mrsm-<version>-<target>`, then run `chmod +x` on the file. The pip installation does not need this step.

    **Windows:** run commands in the foreground. Background jobs need Linux, macOS, Docker, or WSL.

Upgrading an existing installation? Read [Migrating to v5](../reference/migrating-to-v5.md) first.
