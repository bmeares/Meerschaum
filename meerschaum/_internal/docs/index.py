#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

r"""
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
<li><code>meerschaum.Job</code></li>
<li><code>meerschaum.Venv</code></li>
<li><code>meerschaum.SuccessTuple</code></li>
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
<li><code>meerschaum.entry()</code></li>
</ul>

</div>
</div>

### Examples

<details>
    <summary><b>Build a Connector</b></summary>

Get existing connectors or build a new one in-memory with the `meerschaum.get_connector()` factory function:

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
</details>

<details>
    <summary><b>Create a Custom Connector Class</b></summary>

Decorate your connector classes with `meerschaum.make_connector()` to designate it as a custom connector:

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
</details>

<details>
    <summary><b>Build a Pipe</b></summary>

Build a `meerschaum.Pipe` in-memory:

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

Add `temporary=True` to skip registering the pipe in the pipes table.

</details>

<details>
    <summary><b>Get Registered Pipes</b></summary>

The `meerschaum.get_pipes()` function returns a dictionary hierarchy of pipes by connector, metric, and location:

```python
import meerschaum as mrsm

pipes = mrsm.get_pipes(instance='sql:temp')
pipe = pipes['foo:bar']['demo'][None]
```

Add `as_list=True` to flatten the hierarchy:

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
</details>

<details>
    <summary><b>Import Plugins</b></summary>

You can import a plugin's module through `meerschaum.Plugin.module`:

```python
import meerschaum as mrsm

plugin = mrsm.Plugin('noaa')
with mrsm.Venv(plugin):
    noaa = plugin.module
```

If your plugin has submodules, use `meerschaum.plugins.from_plugin_import`:

```python
from meerschaum.plugins import from_plugin_import
get_defined_pipes = from_plugin_import('compose.utils.pipes', 'get_defined_pipes')
```

Import multiple plugins with `meerschaum.plugins.import_plugins`:

```python
from meerschaum.plugins import import_plugins
noaa, compose = import_plugins('noaa', 'compose')
```

</details>

<details>
    <summary><b>Create a Job</b></summary>

Create a `meerschaum.Job` with `name` and `sysargs`:

```python
import meerschaum as mrsm

job = mrsm.Job('syncing-engine', 'sync pipes --loop')
success, msg = job.start()
```

Pass `executor_keys` as the connectors keys of an API instance to create a remote job:

```python
import meerschaum as mrsm

job = mrsm.Job(
    'foo',
    'sync pipes -s daily',
    executor_keys='api:main',
)
```

</details>

<details>
    <summary><b>Import from a Virtual Environment</b></summary>
Use the `meerschaum.Venv` context manager to activate a virtual environment:
```python
import meerschaum as mrsm

with mrsm.Venv('noaa'):
    import requests

print(requests.__file__)
# /home/bmeares/.config/meerschaum/venvs/noaa/lib/python3.12/site-packages/requests/__init__.py
```

To import packages which may not be installed, use `meerschaum.attempt_import()`:

```python
import meerschaum as mrsm

requests = mrsm.attempt_import('requests', venv='noaa')
print(requests.__file__)
# /home/bmeares/.config/meerschaum/venvs/noaa/lib/python3.12/site-packages/requests/__init__.py
```

</details>

<details>
    <summary><b>Run Actions</b></summary>

Run `sysargs` with `meerschaum.entry()`:

```python
import meerschaum as mrsm

success, msg = mrsm.entry('show pipes + show version : x2')
```

Use `meerschaum.actions.get_action()` to access an action function directly:

```python
from meerschaum.actions import get_action

show_pipes = get_action(['show', 'pipes'])
success, msg = show_pipes(connector_keys=['plugin:noaa'])
```

Get a dictionary of available subactions with `meerschaum.actions.get_subactions()`:

```python
from meerschaum.actions import get_subactions

subactions = get_subactions('show')
success, msg = subactions['pipes']()
```

</details>

<details>
    <summary><b>Create a Plugin</b></summary>

Run `bootstrap plugin` to create a new plugin:

```
mrsm bootstrap plugin example
```

This will create `example.py` in your plugins directory (default `~/.config/meerschaum/plugins/`, Windows: `%APPDATA%\Meerschaum\plugins`). You may paste the example code from the "Create a Custom Action" example below.

Open your plugin with `edit plugin`:

```
mrsm edit plugin example
```

*Run `edit plugin` and paste the example code below to try out the features.*

See the [writing plugins guide](https://meerschaum.io/reference/plugins/writing-plugins/) for more in-depth documentation.

</details>

<details>
    <summary><b>Create a Custom Action</b></summary>

Decorate a function with `meerschaum.actions.make_action` to designate it as an action. Subactions will be automatically detected if not decorated:

```python
from meerschaum.actions import make_action

@make_action
def sing():
    print('What would you like me to sing?')
    return True, "Success"

def sing_tune():
    return False, "I don't know that song!"

def sing_song():
    print('Hello, World!')
    return True, "Success"

```

Use `meerschaum.plugins.add_plugin_argument()` to create new parameters for your action:

```python
from meerschaum.plugins import make_action, add_plugin_argument

add_plugin_argument(
    '--song', type=str, help='What song to sing.',
)

@make_action
def sing_melody(action=None, song=None):
    to_sing = action[0] if action else song
    if not to_sing:
        return False, "Please tell me what to sing!"

    return True, f'~I am singing {to_sing}~'
```

```
mrsm sing melody lalala

mrsm sing melody --song do-re-mi
```

</details>

<details>
    <summary><b>Add a Page to the Web Dashboard</b></summary>
    Use the decorators `meerschaum.plugins.dash_plugin()` and `meerschaum.plugins.web_page()` to add new pages to the web dashboard:

```python
from meerschaum.plugins import dash_plugin, web_page

@dash_plugin
def init_dash(dash_app):

    import dash.html as html
    import dash_bootstrap_components as dbc
    from dash import Input, Output, no_update

    ### Routes to '/dash/my-page'
    @web_page('/my-page', login_required=False)
    def my_page():
        return dbc.Container([
            html.H1("Hello, World!"),
            dbc.Button("Click me", id='my-button'),
            html.Div(id="my-output-div"),
        ])

    @dash_app.callback(
        Output('my-output-div', 'children'),
        Input('my-button', 'n_clicks'),
    )
    def my_button_click(n_clicks):
        if not n_clicks:
            return no_update
        return html.P(f'You clicked {n_clicks} times!')
```
</details>

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
  - `meerschaum.connectors.sql.SQLConnector`
  - `meerschaum.connectors.api.APIConnector`
  - `meerschaum.connectors.valkey.ValkeyConnector`

</details>

<details>
    <summary>
    `meerschaum.jobs`<br>
    Start background jobs.
    </summary>

  - `meerschaum.jobs.Job`
  - `meerschaum.jobs.Executor`
  - `meerschaum.jobs.systemd.SystemdExecutor`
  - `meerschaum.jobs.get_jobs()`
  - `meerschaum.jobs.get_filtered_jobs()`
  - `meerschaum.jobs.get_running_jobs()`
  - `meerschaum.jobs.get_stopped_jobs()`
  - `meerschaum.jobs.get_paused_jobs()`
  - `meerschaum.jobs.get_restart_jobs()`
  - `meerschaum.jobs.make_executor()`
  - `meerschaum.jobs.check_restart_jobs()`
  - `meerschaum.jobs.start_check_jobs_thread()`
  - `meerschaum.jobs.stop_check_jobs_thread()`

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
      <li><code>meerschaum.utils.dataframe.to_json()</code></li>
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
