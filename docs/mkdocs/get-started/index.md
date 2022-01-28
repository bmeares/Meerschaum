<link rel="stylesheet" type="text/css" href="/assets/css/asciinema-player.css" />
<script src="/assets/js/asciinema-player.js"></script>

# 🛠️ Setup

## 🐍 Install from [PyPI](https://pypi.org/project/meerschaum/)

```bash
python -m pip install --upgrade meerschaum
```

??? info "Optional dependencies"
    Meerschaum will auto-install packages as you use them (into a virtual environment, to preserve your base environment).

    If you'd rather install all of its dependencies at installation, you can request the `full` version:
    ```bash
    python -m pip install --upgrade meerschaum[full]
    ```

## 🗄️ Choose a Database

1. **Pre-configured DB**  

    If you have [Docker](https://www.docker.com/get-started), you can run the pre-configured [Meerschaum stack](/reference/stack/).

    ```bash
    mrsm stack up -d db
    ```

    The keys for this pre-configured [connector](/reference/connectors/) are `sql:main`.

    ??? example "Watch an example"
        <asciinema-player src="/assets/casts/stack.cast" size="small" preload="true" rows="10"></asciinema-player>

2. **Built-in SQLite DB**  

    The keys for the built-in SQLite database are `sql:local`.

3. **Use your own DB**  

    You can [connect your own database](/reference/connectors/#creating-a-connector) with:

    ```bash
    mrsm bootstrap connector
    ```

    ??? example "Watch an example"
        <asciinema-player src="/assets/casts/bootstrap-connector.cast" size="small" preload="true"></asciinema-player>

## ⚡ Connect to Your Instance

![Meerschaum instance prompt](/assets/screenshots/prompt.png){ align=left } Open the Meerschaum shell with `mrsm` or `python -m meerschaum`.

By default, your prompt's instance is the database `sql:main` from the [pre-configured stack](/reference/stack/).

To test the connection, run the command `mrsm show pipes`. A successful connection should return the message:
<div style="background-color: black; padding: 15px;">
<pre style="color: red">💢 No pipes to show.</pre>
</div>
??? tip "Change your instance"

    Your default back-end [instance connector](/reference/connectors/#instances-and-repositories) is `sql:main`. You can see other configured connectors (e.g. `sql:local`) with `mrsm show connectors`.

    To temporarily change instances, open the `mrsm` shell and run the command `instance` followed by the keys.

    ```bash
    mrsm
    instance sql:local
    ```

    To permanently change your default instance:

    1. Open your configuration with `mrsm edit config`.
    2. Navigate to the key `instance:` at the bottom of the file.
    3. Edit the keys to the [`SQL` or `API` connector](/reference/connectors/#instances-and-repositories) of your new instance.

    ??? example "Watch an example"
        <asciinema-player src="/assets/casts/change-instance.cast" size="small" preload="true"></asciinema-player>

If you've successfully connected, try [building a pipe](bootstrap/) to get that data flowing!
