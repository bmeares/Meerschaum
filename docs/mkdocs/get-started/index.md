<link rel="stylesheet" type="text/css" href="/assets/css/asciinema-player.css" />
<script src="/assets/js/asciinema-player.js"></script>

# 🛠️ Setup

## 🐍 Install from [PyPI](https://pypi.org/project/meerschaum/)

```bash
python -m pip install --upgrade meerschaum
```

??? info "Optional dependencies"
    Meerschaum will auto-install packages as you use them (into a virtual environment, to preserve your base environment).

    You can manually install these packages in one command:

    ```bash
    python -m meerschaum upgrade packages -y
    ```

    You can also install the packages into your base environment by requesting the name of the dependency group, e.g. `api`:
    ```bash
    python -m pip install --upgrade meerschaum[api]
    ```

!!! note ""

    Run Meerschaum commands with `mrsm` or `python -m meerschaum` if `~/.local/bin/` isn't in your `PATH`.


## 🗄️ Choose a Database

Let's pick a database to store our data. Pick one of the three options below and take note of the keys (e.g. `sql:<label>`). We'll use this as our Meerschaum [instance connector](/reference/connectors/#instances-and-repositories).

1. **Pre-configured TimescaleDB (`sql:main`)**  

    The default [database connector](/reference/connectors/) `sql:main` points to the pre-configured [Meerschaum stack](/reference/stack/). If you have [Docker](https://www.docker.com/get-started), start the database service:

    ```bash
    mrsm stack up -d db
    ```

    ??? example "📽️ Watch an example"
        <asciinema-player src="/assets/casts/stack.cast" size="small" preload="true" rows="10"></asciinema-player>

2. **Built-in SQLite DB (`sql:local`)**  

    For your convenience, the keys `sql:local` point to a SQLite database in your Meerschaum root directory.

3. **Use your own DB (`sql:<label>`)**  

    You can [connect your own database](/reference/connectors/#creating-a-connector) with `mrsm bootstrap connector`. The keys for your connector will be `sql:<label>`, where `<label>` is the label you assign in the wizard.

    ??? example "📽️ Watch an example"
        <asciinema-player src="/assets/casts/bootstrap-connector.cast" size="small" preload="true"></asciinema-player>

    ??? info "`MRSM_SQL_<LABEL>` environment variables"

        You can also define connectors in your environment. Set an environment variable `MRSM_SQL_<LABEL>` to your database URI:

        ```bash
        MRSM_SQL_FOO=sqlite:////tmp/foo.db \
          python -m meerschaum start connector sql:foo
        ```

## ⚡ Connect to Your Instance

![Meerschaum instance prompt](/assets/screenshots/prompt.png){ align=left } Open the Meerschaum shell with `mrsm` or `python -m meerschaum`.

Your default instance is `sql:main` from the [pre-configured stack](/reference/stack/). Connect to a different instance with `instance sql:<label>`.

To test the connection, run the command `mrsm show pipes`. A successful connection should return the message:
<div style="background-color: black; padding: 15px;">
<pre style="color: red">💢 No pipes to show.</pre>
</div>
??? tip "Change your default instance"

    The `instance` command temporarily changes your connected instance. To permanently change your default instance:

    1. Open your configuration with `mrsm edit config`.
    2. Navigate to the key `instance:` at the bottom of the file.
    3. Edit the value to the keys of your new instance (`mrsm show connectors` to see registered connectors).

    ??? example "Watch an example"
        <asciinema-player src="/assets/casts/change-instance.cast" size="small" preload="true"></asciinema-player>

If you've successfully connected, try [building a pipe](bootstrap/) to get that data flowing!
