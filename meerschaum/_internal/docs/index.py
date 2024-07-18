#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<style>

.container {
    display: flex;
    justify-content: space-between;
}
.left, .right {
    width: 45%; /* Adjust width as needed */
}

</style>

<img src="https://meerschaum.io/assets/banner_1920x320.png" alt="Meerschaum banner" style="width: 100%;"/>

# Meerschaum Python API

Welcome to the Meerschaum Python API technical documentation! Here you can find information about the classes and functions provided by the `meerschaum` package. Visit [meerschaum.io](https://meerschaum.io) for general usage documentation.

## Root Module

For your convenience, the following classes and functions may be imported from the root `meerschaum` namespace:

<div class="container" style="display: flex; justify-content: space-between">

<div class="left" style="width: 45%;">

<h3>Classes</h3>

<ul>
<li><code>meerschaum.Connector</code></li>
<li><code>meerschaum.Pipe</code></li>
<li><code>meerschaum.Plugin</code></li>
<li><code>meerschaum.SuccessTuple</code></li>
<li><code>meerschaum.Venv</code></li>
</ul>

</div>


<div class="right" style="width: 45%">

<h3>Functions</h3>

<ul>
<li><code>meerschaum.get_config()</code></li>
<li><code>meerschaum.get_connector()</code></li>
<li><code>meerschaum.get_pipes()</code></li>
<li><code>meerschaum.make_connector()</code></li>
<li><code>meerschaum.pprint()</code></li>
<li><code>meerschaum.attempt_import()</code></li>
</ul>

</div>
</div>

### Examples

#### Build a Connector

```python
import meerschaum as mrsm

sql_conn = mrsm.get_connector(
    'sql:temp',
    flavor='sqlite',
    database='/tmp/tmp.db',
)
df = sql_conn.read("SELECT 1 AS foo")
print(df)
#    foo
# 0    1

sql_conn.to_sql(df, 'foo')
print(sql_conn.read('foo'))
#    foo
# 0    1
```

#### Create a Custom Connector Class

```python
from datetime import datetime, timezone
from random import randint
import meerschaum as mrsm
from meerschaum.utils.misc import round_time

@mrsm.make_connector
class FooConnector(mrsm.Connector):
    REQUIRED_ATTRIBUTES = ['username', 'password']

    def fetch(
        self,
        begin: datetime | None = None,
        end: datetime | None = None,
    ):
        now = begin or round_time(datetime.now(timezone.utc))
        return [
            {'ts': now, 'id': 1, 'vl': randint(1, 100)},
            {'ts': now, 'id': 2, 'vl': randint(1, 100)},
            {'ts': now, 'id': 3, 'vl': randint(1, 100)},
        ]

foo_conn = mrsm.get_connector(
    'foo:bar',
    username='foo',
    password='bar',
)
docs = foo_conn.fetch()
```

#### Build a Pipe

```python
from datetime import datetime
import meerschaum as mrsm

pipe = mrsm.Pipe(
    foo_conn, 'demo',
    instance=sql_conn,
    columns={'datetime': 'ts', 'id': 'id'},
    tags=['production'],
)
pipe.sync(begin=datetime(2024, 1, 1))
df = pipe.get_data()
print(df)
#           ts  id  vl
# 0 2024-01-01   1  97
# 1 2024-01-01   2  18
# 2 2024-01-01   3  96
```

#### Get Registered Pipes

```python
import meerschaum as mrsm

pipes = mrsm.get_pipes(
    tags=['production'],
    instance=sql_conn,
    as_list=True,
)
print(pipes)
# [Pipe('foo:bar', 'demo', instance='sql:temp')]
```

#### Access a Plugin's Module

```python
import meerschaum as mrsm

plugin = mrsm.Plugin('noaa')
with mrsm.Venv(plugin):
    noaa = plugin.module
    print(noaa.get_station_info('KGMU'))
# {'name': 'Greenville Downtown Airport', 'geometry': {'type': 'Point', 'coordinates': [-82.35004, 34.84873]}}
```

## Submodules

<details>
  <summary>
  `meerschaum.actions`<br>
  Access functions for actions and subactions.
  </summary>

  - `meerschaum.actions.actions`
  - `meerschaum.actions.get_action()`
  - `meerschaum.actions.get_completer()`
  - `meerschaum.actions.get_main_action_name()`
  - `meerschaum.actions.get_subactions()`

</details>

<details>
  <summary>
  `meerschaum.config`<br>
  Read and write the Meerschaum configuration registry.
  </summary>

  - `meerschaum.config.get_config()`
  - `meerschaum.config.get_plugin_config()`
  - `meerschaum.config.write_config()`
  - `meerschaum.config.write_plugin_config()`

</details>

<details>
  <summary>
  `meerschaum.connectors`<br>
  Build connectors to interact with databases and fetch data.
  </summary>

  - `meerschaum.connectors.get_connector()`
  - `meerschaum.connectors.make_connector()`
  - `meerschaum.connectors.is_connected()`
  - `meerschaum.connectors.poll.retry_connect()`
  - `meerschaum.connectors.Connector`
  - `meerschaum.connectors.SQLConnector`
  - `meerschaum.connectors.APIConnector`

</details>

<details>
  <summary>
  `meerschaum.plugins`<br>
  Access plugin modules and other API utilties.
  </summary>

  - `meerschaum.plugins.Plugin`
  - `meerschaum.plugins.api_plugin()`
  - `meerschaum.plugins.dash_plugin()`
  - `meerschaum.plugins.import_plugins()`
  - `meerschaum.plugins.reload_plugins()`
  - `meerschaum.plugins.get_plugins()`
  - `meerschaum.plugins.get_data_plugins()`
  - `meerschaum.plugins.add_plugin_argument()`
  - `meerschaum.plugins.pre_sync_hook()`
  - `meerschaum.plugins.post_sync_hook()`

</details>

<details>
  <summary><code>meerschaum.utils</code><br>
  Utility functions are available in several submodules:<br>
  </summary>

  <ul>
  <details>
    <summary>
    <code>meerschaum.utils.daemon</code><br>
    Manage background jobs.<br>
    </summary>
    <p></p>
    <ul>
      <li><code>meerschaum.utils.daemon.daemon_entry()</code></li>
      <li><code>meerschaum.utils.daemon.daemon_action()</code></li>
      <li><code>meerschaum.utils.daemon.get_daemons()</code></li>
      <li><code>meerschaum.utils.daemon.get_daemon_ids()</code></li>
      <li><code>meerschaum.utils.daemon.get_running_daemons()</code></li>
      <li><code>meerschaum.utils.daemon.get_paused_daemons()</code></li>
      <li><code>meerschaum.utils.daemon.get_stopped_daemons()</code></li>
      <li><code>meerschaum.utils.daemon.get_filtered_daemons()</code></li>
      <li><code>meerschaum.utils.daemon.run_daemon()</code></li>
      <li><code>meerschaum.utils.daemon.Daemon</code></li>
      <li><code>meerschaum.utils.daemon.FileDescriptorInterceptor</code></li>
      <li><code>meerschaum.utils.daemon.RotatingFile</code></li>
    </ul>
    </details>
  </ul>

  <ul>
  <details>
    <summary>
    <code>meerschaum.utils.debug</code><br>
    Debugging tools.<br>
    </summary>
    <p></p>
    <ul>
      <li><code>meerschaum.utils.debug.dprint()</code></li>
      <li><code>meerschaum.utils.debug.trace()</code></li>
    </ul>
  </details>
  </ul>

  <ul>
  <details>
    <summary>
    <code>meerschaum.utils.dataframe</code><br>
    Manipulate dataframes.<br>
    </summary>
    <p></p>
    <ul>
      <li><code>meerschaum.utils.dataframe.add_missing_cols_to_df()</code></li>
      <li><code>meerschaum.utils.dataframe.df_is_chunk_generator()</code></li>
      <li><code>meerschaum.utils.dataframe.enforce_dtypes()</code></li>
      <li><code>meerschaum.utils.dataframe.filter_unseen_df()</code></li>
      <li><code>meerschaum.utils.dataframe.get_datetime_bound_from_df()</code></li>
      <li><code>meerschaum.utils.dataframe.get_first_valid_dask_partition()</code></li>
      <li><code>meerschaum.utils.dataframe.get_json_cols()</code></li>
      <li><code>meerschaum.utils.dataframe.get_numeric_cols()</code></li>
      <li><code>meerschaum.utils.dataframe.get_unhashable_cols()</code></li>
      <li><code>meerschaum.utils.dataframe.parse_df_datetimes()</code></li>
      <li><code>meerschaum.utils.dataframe.query_df()</code></li>
    </ul>
  </details>
  </ul>

  <ul>
  <details>
    <summary>
    <code>meerschaum.utils.dtypes</code><br>
    Work with data types.<br>
    </summary>
    <p></p>
    <ul>
      <li><code>meerschaum.utils.dtypes.are_dtypes_equal()</code></li>
      <li><code>meerschaum.utils.dtypes.attempt_cast_to_numeric()</code></li>
      <li><code>meerschaum.utils.dtypes.is_dtype_numeric()</code></li>
      <li><code>meerschaum.utils.dtypes.none_if_null()</code></li>
      <li><code>meerschaum.utils.dtypes.quantize_decimal()</code></li>
      <li><code>meerschaum.utils.dtypes.to_pandas_dtype()</code></li>
      <li><code>meerschaum.utils.dtypes.value_is_null()</code></li>
      <li><code>meerschaum.utils.dtypes.sql.get_pd_type_from_db_type()</code></li>
      <li><code>meerschaum.utils.dtypes.sql.get_db_type_from_pd_type()</code></li>
    </ul>
  </details>
  </ul>

  <ul>
  <details>
    <summary>
    <code>meerschaum.utils.formatting</code><br>
    Format output text.<br>
    </summary>
    <p></p>
    <ul>
      <li><code>meerschaum.utils.formatting.colored()</code></li>
      <li><code>meerschaum.utils.formatting.extract_stats_from_message()</code></li>
      <li><code>meerschaum.utils.formatting.fill_ansi()</code></li>
      <li><code>meerschaum.utils.formatting.get_console()</code></li>
      <li><code>meerschaum.utils.formatting.highlight_pipes()</code></li>
      <li><code>meerschaum.utils.formatting.make_header()</code></li>
      <li><code>meerschaum.utils.formatting.pipe_repr()</code></li>
      <li><code>meerschaum.utils.formatting.pprint()</code></li>
      <li><code>meerschaum.utils.formatting.pprint_pipes()</code></li>
      <li><code>meerschaum.utils.formatting.print_options()</code></li>
      <li><code>meerschaum.utils.formatting.print_pipes_results()</code></li>
      <li><code>meerschaum.utils.formatting.print_tuple()</code></li>
      <li><code>meerschaum.utils.formatting.translate_rich_to_termcolor()</code></li>
    </ul>
  </details>
  </ul>

  <ul>
  <details>
    <summary>
    <code>meerschaum.utils.misc</code><br>
    Miscellaneous utility functions.<br>
    </summary>
    <p></p>
    <ul>
      <li><code>meerschaum.utils.misc.items_str()</code></li>
      <li><code>meerschaum.utils.misc.round_time()</code></li>
      <li><code>meerschaum.utils.misc.is_int()</code></li>
      <li><code>meerschaum.utils.misc.interval_str()</code></li>
      <li><code>meerschaum.utils.misc.filter_keywords()</code></li>
      <li><code>meerschaum.utils.misc.generate_password()</code></li>
      <li><code>meerschaum.utils.misc.string_to_dict()</code></li>
      <li><code>meerschaum.utils.misc.iterate_chunks()</code></li>
      <li><code>meerschaum.utils.misc.timed_input()</code></li>
      <li><code>meerschaum.utils.misc.replace_pipes_in_dict()</code></li>
      <li><code>meerschaum.utils.misc.is_valid_email()</code></li>
      <li><code>meerschaum.utils.misc.string_width()</code></li>
      <li><code>meerschaum.utils.misc.replace_password()</code></li>
      <li><code>meerschaum.utils.misc.parse_config_substitution()</code></li>
      <li><code>meerschaum.utils.misc.edit_file()</code></li>
      <li><code>meerschaum.utils.misc.get_in_ex_params()</code></li>
      <li><code>meerschaum.utils.misc.separate_negation_values()</code></li>
      <li><code>meerschaum.utils.misc.flatten_list()</code></li>
      <li><code>meerschaum.utils.misc.make_symlink()</code></li>
      <li><code>meerschaum.utils.misc.is_symlink()</code></li>
      <li><code>meerschaum.utils.misc.wget()</code></li>
      <li><code>meerschaum.utils.misc.add_method_to_class()</code></li>
      <li><code>meerschaum.utils.misc.is_pipe_registered()</code></li>
      <li><code>meerschaum.utils.misc.get_cols_lines()</code></li>
      <li><code>meerschaum.utils.misc.sorted_dict()</code></li>
      <li><code>meerschaum.utils.misc.flatten_pipes_dict()</code></li>
      <li><code>meerschaum.utils.misc.dict_from_od()</code></li>
      <li><code>meerschaum.utils.misc.remove_ansi()</code></li>
      <li><code>meerschaum.utils.misc.get_connector_labels()</code></li>
      <li><code>meerschaum.utils.misc.json_serialize_datetime()</code></li>
      <li><code>meerschaum.utils.misc.async_wrap()</code></li>
      <li><code>meerschaum.utils.misc.is_docker_available()</code></li>
      <li><code>meerschaum.utils.misc.is_android()</code></li>
      <li><code>meerschaum.utils.misc.is_bcp_available()</code></li>
      <li><code>meerschaum.utils.misc.truncate_string_sections()</code></li>
      <li><code>meerschaum.utils.misc.safely_extract_tar()</code></li>
    </ul>
  </details>
  </ul>

  <ul>
  <details>
    <summary>
    <code>meerschaum.utils.packages</code><br>
    Manage Python packages.
    <br>
    </summary>
    <p></p>
    <ul>
      <li><code>meerschaum.utils.packages.attempt_import()</code></li>
      <li><code>meerschaum.utils.packages.get_module_path()</code></li>
      <li><code>meerschaum.utils.packages.manually_import_module()</code></li>
      <li><code>meerschaum.utils.packages.get_install_no_version()</code></li>
      <li><code>meerschaum.utils.packages.determine_version()</code></li>
      <li><code>meerschaum.utils.packages.need_update()</code></li>
      <li><code>meerschaum.utils.packages.get_pip()</code></li>
      <li><code>meerschaum.utils.packages.pip_install()</code></li>
      <li><code>meerschaum.utils.packages.pip_uninstall()</code></li>
      <li><code>meerschaum.utils.packages.completely_uninstall_package()</code></li>
      <li><code>meerschaum.utils.packages.run_python_package()</code></li>
      <li><code>meerschaum.utils.packages.lazy_import()</code></li>
      <li><code>meerschaum.utils.packages.pandas_name()</code></li>
      <li><code>meerschaum.utils.packages.import_pandas()</code></li>
      <li><code>meerschaum.utils.packages.import_rich()</code></li>
      <li><code>meerschaum.utils.packages.import_dcc()</code></li>
      <li><code>meerschaum.utils.packages.import_html()</code></li>
      <li><code>meerschaum.utils.packages.get_modules_from_package()</code></li>
      <li><code>meerschaum.utils.packages.import_children()</code></li>
      <li><code>meerschaum.utils.packages.reload_package()</code></li>
      <li><code>meerschaum.utils.packages.reload_meerschaum()</code></li>
      <li><code>meerschaum.utils.packages.is_installed()</code></li>
      <li><code>meerschaum.utils.packages.venv_contains_package()</code></li>
      <li><code>meerschaum.utils.packages.package_venv()</code></li>
      <li><code>meerschaum.utils.packages.ensure_readline()</code></li>
      <li><code>meerschaum.utils.packages.get_prerelease_dependencies()</code></li>
    </ul>
  </details>
  </ul>

  <ul>
  <details>
    <summary>
    <code>meerschaum.utils.prompt</code><br>
    Read input from the user.
    <br>
    </summary>
    <p></p>
    <ul>
      <li><code>meerschaum.utils.prompt.prompt()</code></li>
      <li><code>meerschaum.utils.prompt.yes_no()</code></li>
      <li><code>meerschaum.utils.prompt.choose()</code></li>
      <li><code>meerschaum.utils.prompt.get_password()</code></li>
      <li><code>meerschaum.utils.prompt.get_email()</code></li>
    </ul>
  </details>
  </ul>

  <ul>
  <details>
    <summary>
    <code>meerschaum.utils.schedule</code><br>
    Schedule processes and threads.
    <br>
    </summary>
    <p></p>
    <ul>
      <li><code>meerschaum.utils.schedule.schedule_function()</code></li>
      <li><code>meerschaum.utils.schedule.parse_schedule()</code></li>
      <li><code>meerschaum.utils.schedule.parse_start_time()</code></li>
    </ul>
  </details>
  </ul>

  <ul>
  <details>
    <summary>
    <code>meerschaum.utils.sql</code><br>
    Build SQL queries.
    <br>
    </summary>
    <p></p>
    <ul>
      <li><code>meerschaum.utils.sql.build_where()</code></li>
      <li><code>meerschaum.utils.sql.clean()</code></li>
      <li><code>meerschaum.utils.sql.dateadd_str()</code></li>
      <li><code>meerschaum.utils.sql.test_connection()</code></li>
      <li><code>meerschaum.utils.sql.get_distinct_col_count()</code></li>
      <li><code>meerschaum.utils.sql.sql_item_name()</code></li>
      <li><code>meerschaum.utils.sql.pg_capital()</code></li>
      <li><code>meerschaum.utils.sql.oracle_capital()</code></li>
      <li><code>meerschaum.utils.sql.truncate_item_name()</code></li>
      <li><code>meerschaum.utils.sql.table_exists()</code></li>
      <li><code>meerschaum.utils.sql.get_table_cols_types()</code></li>
      <li><code>meerschaum.utils.sql.get_update_queries()</code></li>
      <li><code>meerschaum.utils.sql.get_null_replacement()</code></li>
      <li><code>meerschaum.utils.sql.get_db_version()</code></li>
      <li><code>meerschaum.utils.sql.get_rename_table_queries()</code></li>
      <li><code>meerschaum.utils.sql.get_create_table_query()</code></li>
      <li><code>meerschaum.utils.sql.format_cte_subquery()</code></li>
      <li><code>meerschaum.utils.sql.session_execute()</code></li>
    </ul>
  </details>
  </ul>

  <ul>
  <details>
    <summary>
    <code>meerschaum.utils.venv</code><br>
    Manage virtual environments.
    <br>
    </summary>
    <p></p>
    <ul>
      <li><code>meerschaum.utils.venv.Venv</code></li>
      <li><code>meerschaum.utils.venv.activate_venv()</code></li>
      <li><code>meerschaum.utils.venv.deactivate_venv()</code></li>
      <li><code>meerschaum.utils.venv.get_module_venv()</code></li>
      <li><code>meerschaum.utils.venv.get_venvs()</code></li>
      <li><code>meerschaum.utils.venv.init_venv()</code></li>
      <li><code>meerschaum.utils.venv.inside_venv()</code></li>
      <li><code>meerschaum.utils.venv.is_venv_active()</code></li>
      <li><code>meerschaum.utils.venv.venv_exec()</code></li>
      <li><code>meerschaum.utils.venv.venv_executable()</code></li>
      <li><code>meerschaum.utils.venv.venv_exists()</code></li>
      <li><code>meerschaum.utils.venv.venv_target_path()</code></li>
      <li><code>meerschaum.utils.venv.verify_venv()</code></li>
    </ul>
  </details>
  </ul>

  <ul>
  <details>
    <summary>
    <code>meerschaum.utils.warnings</code><br>
    Print warnings, errors, info, and debug messages.
    <br>
    </summary>
    <p></p>
    <ul>
      <li><code>meerschaum.utils.warnings.dprint()</code></li>
      <li><code>meerschaum.utils.warnings.error()</code></li>
      <li><code>meerschaum.utils.warnings.info()</code></li>
      <li><code>meerschaum.utils.warnings.warn()</code></li>
    </ul>
  </details>
  </ul>

</details>

"""
