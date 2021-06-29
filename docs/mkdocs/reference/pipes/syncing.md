<link rel="stylesheet" type="text/css" href="/assets/css/asciinema-player.css" />
<script src="/assets/js/asciinema-player.js"></script>
<style>
#btm {
  display: block;
  margin-left: auto;
  margin-right: auto;
}
@media screen and (max-width: 76.1875em) {
  #btm {
    width: 100%;
  }
}
@media screen and (min-width: 76.1875em) {
  #btm {
    width: 70%;
  }
}
</style>
# Syncing

Meerschaum efficiently syncs immutable time-series data, such as IoT sensor data streams. The syncing process consists of three basic stages, similar to ETL: fetch, filter, and insert.

??? example "Watch an example"
    <asciinema-player src="/assets/casts/sync-pipes.cast" preload="true" rows="37"></asciinema-player>

## Stages
The primary reason for syncing in this way is to take advantage of the properties of immutable time-series data to minimize the stress imposed on remote source databases.

### **Fetch** (*Extract* and *Transform*)  
This is where the real time-series optimizations come into play. When syncing a SQL pipe, the definition sub-query is executed with additional filtering in the `WHERE` clause to only fetch the newest data.

For example, if the definition of a pipe is `#!sql SELECT * FROM remote_table`, something like the following query would be executed (query syntax will vary depending on the remote database flavor):

```sql
WITH definition AS (
  SELECT * FROM remote_table
)
SELECT DISTINCT *
FROM
  definition
WHERE
  definition.datetime >= CAST(
    '2021-06-23 14:52:00' AS TIMESTAMP
  ) + INTERVAL '0 minute'
```

!!! question "How does fetch work?"
    The fetching process depends on the type of [connector](/reference/connectors/). SQL pipes generate and execute queries, API pipes read JSON from other Meerschaum API servers, MQTT pipes subscribe to a topic, and plugin pipes implement custom functionality. If you have your own data (e.g. a CSV file), you don't need a connector and may instead sync a DataFrame directly:
    ```python
    >>> from meerschaum import Pipe
    >>>
    >>> ### Columns only need to be defined if you're creating a new pipe.
    >>> pipe.columns = { 'datetime' : 'time', 'id' : 'station_id' }
    >>>
    >>> ### Create a Pandas DataFrame somehow,
    >>> ### or you can use a dictionary of lists instead.
    >>> df = pd.read_csv('data.csv')
    >>>
    >>> pipe.sync(df)
    ```

### **Filter** (remove duplicates)

After fetching remote data, the difference is taken to remove duplicate rows. The algorithm looks something like this:

=== "SQL"

    ```sql
    SELECT new_df.*
    FROM new_df
    LEFT JOIN old_df ON new_df.id = old_df.id
      AND new_df.datetime = old_df.datetime
    WHERE old_df.datetime IS NULL
    ```

=== "Pandas"

    ```python
    new_df[
      ~new_df.fillna(custom_nan).apply(tuple, 1).isin(
        old_df.fillna(custom_nan).apply(tuple, 1)
      )
    ].reset_index(drop=True)
    ```

!!! tip "Skip filtering"
    To skip the filter stage, you can use the `--skip-check-existing` flag.


### Insert (*Load*)

Once data are fetched and filtered, they are inserted into the table of the corresponding [Meerschaum instance](/reference/connectors/#instances-and-repositories). Depending on the type of instance connector, the data may be bulk uploaded (for TimescaleDB and PostgreSQL), inserted into a table, or posted to an API endpoint.

## Prevent Data Loss

Depending on the nature of your remote source, sometimes data may be missed. For example, when data are backlogged or a pipe contains multiple data streams (i.e. an ID column), the syncing algorithm might overlook old data.

!!! info "Enable multiplexed fetching"
    There is an experimental feature that can account for multiplexed data streams, but keep in mind that performance may be negatively affected for a large number of IDs.

    To enable this feature, run `edit config system` and under the `experimental` section, set `join_fetch` to `true`.

### Specify an ID column

When you [bootstrap a pipe](/reference/pipes/bootstrapping/#datetime-and-id-columns), you will be asked for a datetime and ID columns. If you've bootstrapped a pipe and forgot to specify its ID column, you may have to rebuild the [indices](https://docs.meerschaum.io/connectors/sql/SQLConnector.html#meerschaum.connectors.sql.SQLConnector.SQLConnector.create_indices):

```python
>>> import meerschaum as mrsm
>>> pipe = mrsm.Pipe('sql:remote', 'weather')
>>>
>>> ### The instance connector is 'sql:main' or wherever the Pipe is stored.
>>> pipe.instance_connector.create_indices(pipe)
```

### Add a backtrack interval

When syncing a SQL pipe, the most recent datetime value is used in the `WHERE` clause. If you have multiple IDs or backlogged data, you need to specify the backtrack minutes in order to catch all of the new remote data.

<img src="/assets/diagrams/backtrack-minutes.png" alt="Meerschaum backtrack minutes interval" width="75%" style="margin: auto;" id="btm"/>

Consider the image above. There are four data streams that grow at separate rates — the dotted lines represent remote data which have not yet been synced. By default, only data to the right of the red line will be fetched, which will miss data for the "slower" IDs.

To fix this, add a backtrack interval of 720 minutes (12 hours). This moves the starting point backwards to the blue line, and all of the new data will be fetched.

!!! tip "Choosing a backtrack interval"
    A larger backtrack interval will cover more ground but be less efficient. The backtrack interval works best when all IDs report within a known interval of each other (e.g. 1440 minutes / 12 hours).

To add a backtrack interval, edit a pipe and add the key `backtrack_minutes` under `fetch`:

```yaml
fetch:
  backtrack_minutes: 1440
```

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
