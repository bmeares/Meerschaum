<link rel="stylesheet" type="text/css" href="/assets/css/asciinema-player.css" />
<script src="/assets/js/asciinema-player.js"></script>

# 🛠️ Setup

## 🐍 Install from [PyPI](https://pypi.org/project/meerschaum/)

!!! note inline end ""

    Run Meerschaum commands with `mrsm` (or `python -m meerschaum` if `~/.local/bin/` isn't in your `PATH`).

```bash
python -m pip install --upgrade meerschaum
```

!!! note "Release candidates"
    While 5.0 is in release candidates, `pip install meerschaum` still installs the latest 4.x release. Install the candidate with `python -m pip install --pre meerschaum` (see the [migration guide](/reference/migrating-to-v5/) for what changed).

The wheel contains the native core and has no required Python dependencies. Wheels are published for Linux (x86_64 and arm64, for glibc distributions and for musl ones such as Alpine), macOS (arm64 and x86_64) and Windows (x86_64), for Python 3.11 or newer.

??? info "Standalone binary (`cargo install meerschaum`)"
    `pip install meerschaum` is the whole product: the `mrsm` command it installs runs the native command line from inside the package. If you would rather have a standalone binary (it starts in about 10 ms and needs no Python until you use a Python plugin or connector), build the same command line with Cargo:

    ```bash
    cargo install meerschaum --locked            # during the release candidates: --version 5.0.0-rc.8
    ```

    This needs Rust 1.95 or newer. `--locked` builds with the dependency versions Meerschaum was tested with (without it Cargo may pick newer ones, which can break the build on Windows). Prebuilt standalone binaries (Linux x86_64 and arm64, macOS arm64 and x86_64, Windows x86_64) are attached to each GitHub release, with a SHA-256 file for each.

    The `duckdb` and `oracle` flavors load native libraries at run time: see [SQL connectors](/reference/connectors/sql-connectors/#duckdb).

    !!! warning "macOS: the prebuilt binary is not signed"
        The release binaries for macOS are not signed or notarized (Meerschaum 5.0 has no Apple developer signing). A browser marks the downloaded file as quarantined and Gatekeeper then refuses to open it ("cannot be opened because the developer cannot be verified"). Remove the flag once, make the file executable, and run it:

        ```bash
        xattr -d com.apple.quarantine ./mrsm-<version>-aarch64-apple-darwin   # x86_64-apple-darwin on an Intel Mac
        chmod +x ./mrsm-<version>-aarch64-apple-darwin
        ./mrsm-<version>-aarch64-apple-darwin --version
        ```

        A file downloaded with `curl` or `gh release download` is not quarantined and needs only `chmod +x`. `pip install meerschaum` and `cargo install meerschaum` are not affected: pip and Cargo do not set the flag.

??? info "Docker"
    The official images are `bmeares/meerschaum` (the default image, with `meerschaum[full]`), `bmeares/meerschaum:api` and `bmeares/meerschaum:slim` (the base wheel only). They are published for `linux/amd64` and `linux/arm64`. See the [migration guide](/reference/migrating-to-v5/) for how they changed in v5.

??? warning "Windows"
    Everything runs on Windows except one thing. The Python API host (`@api_plugin` routes, the Dash web console and plugin pages) is reached over loopback TCP with a per-start token on Windows. Background jobs are refused on Windows, so `-d` and `start jobs` do not work (and neither do the console's Exec box and Manage menu, which run actions as jobs): run the command in the foreground, use Task Scheduler or a service wrapper, or run Meerschaum in Docker or WSL.


??? info "Installing dependencies"
    Meerschaum will auto-install packages as you use them (into a virtual environment, to preserve your base environment).

    You can manually install these packages in one command:

    ```bash
    python -m meerschaum upgrade packages api -y
    ```

    To install the packages into your environment (e.g. you're already using a virtual environment) by adding the name of the [dependency group](https://github.com/bmeares/Meerschaum/blob/main/meerschaum/utils/packages/_packages.py), e.g. `api`:

    ```bash
    python -m pip install --upgrade meerschaum[api]
    ```

## 🥞 Start the [Stack](/reference/stack/)

!!! note inline end ""
    You'll need [Docker](https://docs.docker.com/engine/install/) to start the stack.

```bash
mrsm stack up -d
```

??? example "📽️ Watch an example"
    <asciinema-player src="/assets/casts/stack.cast" size="small" preload="true" rows="10"></asciinema-player>

This will start the pre-configured TimescaleDB instance (`sql:main`).

??? note "Don't have Docker or want to use your own database?"
    You can use a built-in SQLite database with the keys `sql:local`, or you may use your own database by [creating a new connector](/reference/connectors/#creating-a-connector). Read more about the [`SQLConnector` here](/reference/connectors/sql-connectors/).

    ??? example "📽️ Watch an example"
        <asciinema-player src="/assets/casts/bootstrap-connector.cast" size="small" preload="true"></asciinema-player>

    ??? info "`MRSM_SQL_<LABEL>` environment variables"

        You can also define connectors in your environment. Set an environment variable `MRSM_SQL_<LABEL>` to your database URI:

        ```bash
        MRSM_SQL_FOO=sqlite:////tmp/foo.db \
          python -m meerschaum start connector sql:foo
        ```

## ⚡ Connect to Your Instance

![Meerschaum instance prompt](/assets/screenshots/prompt.png){ align=left }

Open the Meerschaum shell with `mrsm` or `python -m meerschaum`.

Your default instance is `sql:main` from the [pre-configured stack](/reference/stack/). Connect to a different instance with `instance sql:<label>`.

To test the connection, run the command `mrsm show pipes`. A successful connection should return the message:
<div style="background-color: black; padding: 15px;">
<pre style="color: #66ff00"><b>🎉 No pipes to show.</b></pre>
</div>
??? tip "Change your default instance"

    The `instance` command temporarily changes your connected instance. To permanently change your default instance:

    1. Open your configuration with `mrsm edit config`.
    2. Navigate to the key `instance:` at the bottom of the file.
    3. Edit the value to the keys of your new instance (`mrsm show connectors` to see registered connectors).

    ??? example "Watch an example"
        <asciinema-player src="/assets/casts/change-instance.cast" size="small" preload="true"></asciinema-player>

If you've successfully connected, try [building a pipe](bootstrap/) to get that data flowing!
