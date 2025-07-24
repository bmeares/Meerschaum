<link rel="stylesheet" type="text/css" href="/assets/css/asciinema-player.css" />
<script src="/assets/js/asciinema-player.js"></script>

# 🔌 Connectors

Meerschaum coordinates everything via connectors. Use cases for Connectors include fetching and inserting data when [syncing pipes](/reference/pipes/syncing).

A connector is identified by two keys: its **type** and **label**, separated by a colon (`:`). For example, the connector `sql:main` refers to a connector with the type `sql` and the label `main`.

The command `mrsm show connectors` will print out your defined connectors and their attributes.

## 🗃️ Instances and Repositories

The terms **instance** and **repository** connectors refer to specific interfaces for connectors. When connecting to a Meerschaum instance, you use a standard `sql` or `api` connector, which expects to be able to access internal Meerschaum methods, such as retrieving users' and pipes' metadata.

![Meerschaum Connectors Venn Diagram](connectors_venn_diagram.png){align=right}

!!! info
    Not all `sql` connections are instance connectors, but all `api` connectors are.

Repository connectors are a subset of instance connectors and may only be `api` connectors. Consider the Venn diagram to the right to vizualize the different classes of connectors.

## 🌳 Environment Connectors

One handy way to temporarily register a connector is by setting an environment variable `MRSM_<TYPE>_<LABEL>` to the connector's [URI](https://en.wikipedia.org/wiki/Uniform_Resource_Identifier). For example, the following environment variable would define the connector `sql:foo`:

```bash
MRSM_SQL_FOO=sqlite:////tmp/foo.db
```

!!! note ""
    Create your own custom connectors with the `@make_connector` decorator:

    ```python
    from meerschaum.connectors import make_connector, Connector

    @make_connector
    class DogConnector(Connector):
        IS_INSTANCE = False
        REQUIRED_ATTRIBUTES = ['username', 'password']

    ```

    In the connector's environment variable, define the attributes as JSON:

    ```bash
    export MRSM_DOG_SPOT='{"username": "foo", "password": "bar"}'
    ```

!!! tip "Did you know?"

    You can reference your Meerschaum configuration in environment connectors, like you can do with `MRSM_CONFIG`:

    ```bash
    MRSM_SQL_FOO=postgresql://user:MRSM{meerschaum:connectors:sql:main:password}@localhost:5432/db
    ```


## ✅ Creating a Connector
To create a new connector (or redefine an existing one), run the command `bootstrap connector` and answer the following prompts. The new connector will be added to your configuration file (which may be accessed with `edit config`).

??? example "🎦 Watch an example"
    <asciinema-player src="/assets/casts/bootstrap-connector.cast" size="small"></asciinema-player>

## ❌ Deleting a Connector
To delete a connector, run the command `delete connectors` with the `-c` connector keys flag:

```bash
mrsm delete connectors -c sql:myremoteconnector -y
```
