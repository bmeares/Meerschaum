<link rel="stylesheet" type="text/css" href="/assets/css/asciinema-player.css" />
<script src="/assets/js/asciinema-player.js"></script>

# 🔌 Connectors

Meerschaum coordinates everything via connectors. Under the hood, Connectors are collections of attributes (e.g. username, host, uri, etc.) that allow Meerschaum to send and retrieve data by implementing Meerschaum's interface to another protocol. Use cases for Connectors include fetching and inserting data when [syncing pipes](/reference/pipes/syncing).

A connector is identified by two keys: its **type** and **label**, separated by a colon (`:`). For example, the connector `sql:main` refers to a connector with the type `sql` and the label `main`.

The command `mrsm show connectors` will print out your defined connectors and their attributes.

## 🗃️ Instances and Repositories

The terms **instance** and **repository** connectors refer to subclasses of standard Meerschaum connectors. When connecting to a Meerschaum instance, you use a standard `sql` or `api` connector, which expects to be able to access internal Meerschaum methods, such as retrieving users' and pipes' metadata.

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
    Custom connectors may be set through environment variables if they are defined with the `@make_connector` decorator:

    ```python
    from meerschaum.connectors import make_connector, Connector

    @make_connector
    class FooConnector(Connector):
        IS_INSTANCE = True

        def __init__(label: str, **kw):
            super().__init__('foo', label, **kw)
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
delete connectors -c sql:myremoteconnector -y
```

## 🤔 Types

!!! tip "Connectors give you options"
    A connector's type determines the protocol it uses and its required attributes. Different types of connectors are capable of different tasks and have varying levels of flexibility and performance.

| Type     | Pros                                                         | Cons                                                         | Use Cases                                                    |
| -------- | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| `sql`    | - Fast transfer rates<br />- May be instance connector       | - Typically network restricted for security                  | - Internal use behind a firewall<br />- Connect API instance to a database<br />- Large data transfers (>100,000 rows) |
| `api`    | - Low resource requirements<br />- APIs may be chained together<br />- May be instance / repository connector | - Slower than direct database connection                     | - Endpoint for or deploy on IoT devices<br />- Expose SQLite databases<br />- Connect to a database instance that's behind firewall<br />- Chaining together API instances |
| `plugin` | - Allows developers to ingest any data source                | - Usually for one specific data source<br />- May not be an instance connector | - Ingesting data from other APIs<br />- Integrating legacy systems into Meerschaum |
| `mqtt`   | - Subscribe to MQTT topics                                   | - Meerschaum shell must be running to receive data<br />- May not be an instance connector | - Ingesting data from existing IoT devices        