# Connectors
The way Meerschaum operates between machines is via connectors. Connectors are collections of configuration attributes (e.g. username, host, etc.) that allow Meerschaum to send and retrieve data by implementing Meerschaum's interface to another protocol.

## Structure
A connector is identified by two parts: its **type** and **label**, separated by a colon (`:`). For example, the connector `sql:local` refers to a connector with the type `sql` and the label `local`. By default, the connector `sql:local` is configured to use a direct database connection to `localhost`.


### Type

!!! tip
    A connector's type determines the protocol it uses and its required attributes. Different types of connectors are capable of different tasks and have varying levels of flexibility and performance.

| Type     | Pros                                                         | Cons                                                         | Use Cases                                                    |
| -------- | ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| `sql`    | - Fast transfer rates<br />- May be instance connector       | - Typically network restricted for security                  | - Internal use behind a firewall<br />- Connect API instance to a database<br />- Large data transfers (>100,000 rows) |
| `api`    | - Low resource requirements<br />- APIs may be chained together<br />- May be instance / repository connector | - Slower than direct database connection                     | - Endpoint for or deploy on IoT devices<br />- Expose SQLite databases<br />- Connect to a database instance that's behind firewall<br />- Chaining together API instances |
| `plugin` | - Allows developers to ingest any data source                | - Usually for one specific data source<br />- May not be an instance connector | - Ingesting data from other APIs<br />- Integrating legacy systems into Meerschaum |
| `mqtt`   | - Subscribe to MQTT topics                                   | - Meerschaum shell must be running to receive data<br />- May not be an instance connector | - Ingesting data from existing IoT devices                   |

## Instances and Repositories

The terms **instance** and **repository** connector refer to subclasses of standard Meerschaum connectors. When connecting to a Meerschaum instance, you use a standard `sql` or `api` connector, which expects to be able to access internal Meerschaum methods, such as retrieving users and pipes' metadata.

!!! info

​    Not all `sql` connections are instance connectors, but all `api` connectors are.

A *

## Creating a Connector
To create a new connector, run the command `bootstrap connector` and answer the following prompts. The new connector will be added to your configuration file (which may be accessed with `edit config`).