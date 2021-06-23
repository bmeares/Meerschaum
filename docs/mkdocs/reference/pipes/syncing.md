# Syncing

Meerschaum efficiently syncs immutable time-series data, such as IoT sensor data streams.

## Troubleshooting

In case a sync fails, you can correct the problem by editing the pipe's attributes with the command `edit pipes`. You may also bootstrap an existing pipe to wipe everything and start the process again from the top.

!!! tip "Try before you sync"
    When writing the definition for a `sql` pipe, it's a good idea to first test the SQL query before going through the hassle of bootstrapping a pipe. You can open an interactive SQL session with the `sql <label>` command or in the Python REPL with the `python` command and the following code:
    ```python
    >>> import meerschaum as mrsm
    >>> conn = mrsm.get_connector('sql', '<label>')
    >>> query = """
    ... SELECT * FROM my_table
    ... WHERE foo = 'bar'
    ... """
    >>> df = conn.read(query)
    >>> df
    ```


## Multiplexed Pipes

When syncing multiplexed data streams, keep in mind the following caveats to avoid missing data:

### Specify an ID column

When you [bootstrap a pipe](/reference/pipes/bootstrapping/#datetime-and-id-columns), you will be asked for a datetime and ID columns. If you've bootstrapped a pipe and forgot to specify its ID column, you may have to rebuild the [indices](https://docs.meerschaum.io/connectors/sql/SQLConnector.html#meerschaum.connectors.sql.SQLConnector.SQLConnector.create_indices):

```python-repl
>>> import meerschaum as mrsm
>>> pipe = mrsm.Pipe('sql:remote', 'weather')
>>>
>>> ### The instance connector is 'sql:main' or wherever the Pipe is stored.
>>> pipe.instance_connector.create_indices(pipe)
```

**NOTE:** More detailed information about the syncing process will be provided as soon as possible!
