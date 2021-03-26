# Bootstrap a Pipe

The `bootstrap pipes` action includes a setup wizard to guide you through the process of creating new pipes. In this tutorial, we'll be looking at the prompts you'll encounter to help you better understand what each question is asking.

To get started, run the following command:

```bash
mrsm bootstrap pipe --shell
```

!!! tip ""
    The `--shell` flag launches an interactive Meerschaum shell after executing the provided action. You can open the Meershaum shell with the `mrsm` command or execute actions on the command line.
    
## Choose a Connector

The first question you will see is *Where are the data coming from?* along with a list of recognized [connectors](/reference/connectors/). A connector defines how Meerschaum talks with other servers (e.g. username, password, host, etc.).

If you know what connector you need, go ahead and type its keys (type and label separated by a colon, like `sql:myserver`), otherwise type 'New' to define a new connector. You can read more about how connectors work on the [Connectors reference page](/reference/connectors/).

### Defining a New Connector

If you chose 'New' to define a new connector, you'll be presented with a screen asking you to choose the connector's type. The type determines the protocol over which data will be transferred, so it's important to choose wisely! You can consult this [Connectors Type table](/reference/connectors/#type) for the pros, cons, and use cases for each type of connector.

After choosing the connector's type, answer the following prompts 