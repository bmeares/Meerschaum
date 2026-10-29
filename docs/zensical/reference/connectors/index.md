
# 🔌 Connectors

Meerschaum coordinates everything via connectors. Use cases for Connectors include fetching and inserting data when [syncing pipes](/reference/pipes/syncing).

A connector is identified by two keys: its **type** and **label**, separated by a colon (`:`). For example, the connector `sql:main` refers to a connector with the type `sql` and the label `main`.

The command `mrsm show connectors` will print out your defined connectors and their attributes.

## 🗃️ Instances and Repositories

The terms **instance** and **repository** connectors refer to specific interfaces for connectors. When connecting to a Meerschaum instance, you use a standard `sql` or `api` connector, which expects to be able to access internal Meerschaum methods, such as retrieving users' and pipes' metadata.

<img src="/assets/diagrams/connectors_venn_diagram.png" alt="Meerschaum Connectors Venn Diagram" align="right">

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


## 🛡️ Protected Connectors

Mark a connector `protected: true` (a production database, a shared team instance) and
Meerschaum refuses every change through it: syncing, clearing, dropping, deleting, registering
or editing pipes (tags included), deduplicating, `verify schema`, creating or dropping indices,
compressing, vacuuming and repartitioning, and raw SQL from `mrsm sql`,
`SQLConnector.exec()` / `to_sql()` and plugins. Reads are never blocked (`analyze pipes` only
refreshes statistics and is allowed), and Meerschaum's own bookkeeping (users, tokens, plugins,
saved Pipelines, run history and sync status) keeps working. A sync is refused before anything
is fetched from its source.

Your own queries still read: `mrsm sql prod "SELECT ..."`, `SQLConnector.read()` and
`value()` run one read statement (`SELECT`, `WITH`, `SHOW`, `EXPLAIN`, ...) in a transaction
that is rolled back (and read-only on PostgreSQL, MySQL and MariaDB), and refuse anything that
writes (`DELETE`, `UPDATE`, `CREATE TABLE ... AS SELECT`, several statements at once) with
`PermissionError`.

```yaml
meerschaum:
  connectors:
    sql:
      prod:
        flavor: postgresql
        host: db.example.com
        database: analytics
        username: analyst
        password: ...
        protected: true
```

On a URI, add `?protected=true` (the database client never sees it). This works for every
connector type, so environment connectors can be protected too:

```bash
export MRSM_SQL_PROD='postgresql://analyst:...@db.example.com/analytics?protected=true'
export MRSM_VALKEY_CACHE='redis://cache.example.com:6379/0?protected=true'
export MRSM_API_PROD='https://user:pass@mrsm.example.com?protected=true'
```

A refused change reads:

```
Refusing to clear Pipe('plugin:noaa', 'weather', instance='sql:prod') on protected instance 'sql:prod'. Pass --allow-protected (or set MRSM_ALLOW_PROTECTED=1).
```

To write on purpose, pass `--allow-protected` to the command (`-y` does not count). Jobs started
with the flag keep it, because the flag is part of the job's command. `MRSM_ALLOW_PROTECTED=1`
in the environment allows writes for that process and its children (for example a server:
`mrsm start api --allow-protected`). In Python, wrap the writes in `allow_protected()`:

```python
import meerschaum as mrsm
from meerschaum.connectors import allow_protected

pipe = mrsm.Pipe('plugin:noaa', 'weather', instance='sql:prod')
print(pipe.instance_connector.protected)
# True

with allow_protected():
    success, msg = pipe.sync(df)
```

`allow_protected()` applies to the whole process while the block runs.

## ✅ Creating a Connector
To create a new connector (or redefine an existing one), run the command `bootstrap connector` and answer the following prompts. The new connector will be added to your configuration file (which may be accessed with `edit config`).

??? example "🎦 Watch an example"
    <asciinema-player src="/assets/casts/bootstrap-connector.cast" size="small"></asciinema-player>

## ❌ Deleting a Connector
To delete a connector, run the command `delete connectors` with the `-c` connector keys flag:

```bash
mrsm delete connectors -c sql:myremoteconnector -y
```
