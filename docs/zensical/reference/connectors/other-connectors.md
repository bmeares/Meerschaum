# ❓ Other Connector Types

Though the [SQL Connector](/reference/connectors/sql-connectors/) is the premier Meerschaum connector type, there are plenty of other implementations of the [instance connector interface](/reference/connectors/instance-connectors/).

## 🗝️ Valkey Connectors

Syncs pipes to a [Valkey](https://valkey.io/) (or Redis) instance. The built-in `valkey:main` connector points at the Valkey instance in the [Meerschaum stack](/reference/stack/).

See the dedicated [Valkey Connectors](/reference/connectors/valkey/) page for configuration, data layout, and examples.

- **Implementation:** built-in ([docs](https://docs.meerschaum.io/meerschaum/connectors/valkey.html#ValkeyConnector))
- **Type:** `valkey`

## 🌐 API Connector

Syncs to a Meerschaum instance via the [Web API](https://api.mrsm.io/docs). The pipes made available via the Web UI are from the instance set under `MRSM{api_instance}`, usually `sql:main`.

- **Implementation**: built-in ([docs](https://docs.meerschaum.io/meerschaum/connectors/api.html#APIConnector))
- **Type:** `api`

!!! example "Connector config"

    ```yaml
    protocol: http
    username: foo
    password: bar
    host: localhost
    port: 8000
    ```

    ```yaml
    uri: http://foo:bar@localhost:8000
    ```

## 🍃 MongoDB Connector

Syncs to a MongoDB cluster. Allows you to read from existing collections as well as storing your pipes' data in collections.

- **Implementation:** [`mongodb-connector` plugin](https://github.com/bmeares/mongodb-connector)
- **Type:** `mongodb`


!!! example "Connector config"

    ```yaml
    uri: mongodb://localhost:27017 
    database: foo
    ```

!!! example "Fetch pipe"

    ```yaml
    connector: mongodb:foo
    metric: bar
    parameters:
      fetch:
        query:
          a: 1
        projection: {}
        collection: bar
    ```

## 🛜 MQTT Connector

Subscribe to MQTT topics and sync the fetched messages.

- **Implementation:** [`mqtt-connector` plugin](https://github.com/bmeares/mqtt-connector)
- **Type:** `mqtt`

!!! example "Connector config"

    ```yaml
    host: localhost
    port: 1883
    username: foo
    password: bar
    transport: tcp
    keepalive: 60
    ```


!!! example "Fetch pipe"

    ```yaml
    connector: "mqtt:local"
    metric: "temperature"
    columns:
      datetime: "timestamp"
      topic: "topic"
    parameters:
      fetch:
        topic:
        - "foo/#"
        - "bar/#"
    ```