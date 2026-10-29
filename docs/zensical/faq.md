---
hide:
  - navigation
  - toc
---

# 🤔 Frequently Asked Questions

Below is a list of common questions and issues you may encounter. If you have suggestions, please join the [FAQ Suggestions discussion on the GitHub repository](https://github.com/bmeares/Meerschaum/discussions/46)!

??? faq "How do I upgrade to the latest version?"
    You can install the latest version of Meerschaum with `pip install --upgrade meerschaum` or the command `mrsm upgrade mrsm`, which upgrades the installation `mrsm` runs from (pip, uv, pipx, Cargo or a downloaded binary) and, when Docker is available, pulls the stack's images. Inside a Docker container, pull the new image and recreate the container instead (see the [migration guide](/reference/migrating-to-v5/#upgrading-an-installation)).

??? faq "`mrsm` doesn't work!"
    You can invoke `mrsm` directly with `python -m meerschaum`. Check that your `PATH` includes scripts installed by `pip`, such as `~/.local/bin`.

    ```
    export PATH=$PATH:~/.local/bin
    ```

??? faq "How do I turn off the emoji and/or colors?"
    The color and emoji configuration settings are contained in the `system` config file under the `formatting` section, which you can access with `edit config system`.

    Alternatively, you can install the `color` plugin which will toggle these values for you. The `color` command will toggle `unicode` and `ansi` settings by default, or you can specify `color unicode` or `color ansi` if you want to toggle specific values.
    ```
    install plugins color
    color
    ```
    Note that disabling ANSI may not completely elimate ANSI characters.

??? faq "Can I use Meerschaum in my scripts?"
    Yes, although some commands like `bootstrap` and `edit` are interactive and not safe for scripting, most commands are scriptable.

    When executing Meerschaum actions in a script, it's a good idea to add `--nopretty` and `--noask` flags, or `--yes` or `--force` to agree to confirmation dialogues.

    The flag `--noask` will choose the default values for questions (e.g. skipping deleting to prevent data loss) and `--yes` of `--force` will choose `yes` to agree to all dialogues.

    The `--nopretty` flag may not have 100% coverage, but in cases like `show pipes` or `show columns`, it will instead print JSON representations of the pipes and data (separated by newlines) rather than formatting them.

    If you are planning on integrating Meerschaum into your Python scripts, you can access actions directly via the `meerschaum.actions.actions` dictionary. Please consult [the Python package documentation](https://docs.meerschaum.io) for more information. Also, you might want to consider making your script into a [Meerschaum action by writing an action plugin](/reference/plugins/types-of-plugins/#action-plugins).

??? faq "Connectors don't work for `<database flavor>`!"
    Pipe operations run through Meerschaum's own Rust drivers, which support these flavors: `timescaledb`, `timescaledb-ha`, `postgresql`, `postgis`, `citus`, `cockroachdb`, `mssql`, `mysql`, `mariadb`, `oracle`, `sqlite`, `geopackage` and `duckdb` (see [Supported Flavors](/reference/connectors/sql-connectors/#supported-flavors)). If your database isn't listed, you might be able to use a supported flavor that speaks the same protocol (e.g. `postgresql` is a commonly shared flavor).

    Most flavors need nothing else installed. Microsoft SQL Server needs no ODBC driver. Two flavors need a client library:

    - **Oracle** needs [Oracle Instant Client](https://www.oracle.com/database/technologies/instant-client.html) (`libclntsh`) on the loader path; see [Oracle SQL](/reference/connectors/sql-connectors/#oracle-sql).
    - **DuckDB** uses the `meerschaum[sql-duckdb]` extra in Python, or downloads DuckDB's own library on first use; see [DuckDB](/reference/connectors/sql-connectors/#duckdb).

    Run `mrsm show drivers` to see whether these libraries were found, and `mrsm doctor` for a full health check.

    Raw SQL from Python (`conn.read()`, `conn.exec()`, `conn.engine`) still goes through SQLAlchemy and needs the `meerschaum[sql]` extra, plus `meerschaum[sql-mssql]` (`pyodbc`, which needs Microsoft's [ODBC driver](https://learn.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server)) or `meerschaum[sql-oracle]` for those flavors.

??? faq "How do I completely uninstall Meerschaum?"
    - Take down your stack and delete its data with `mrsm stack down -v`.
        - You might also want to delete `bmeares/meerschaum` and other images with `docker image rm`.
    - Uninstall via `pip`: `pip uninstall meerschaum`.
    - Delete the folder `~/.config/meerschaum/` (`%APPDATA%\Meerschaum\` on Windows).
