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

```python
import meerschaum as mrsm

# Build a pipe
pipe = mrsm.Pipe('plugin:noaa', 'weather')

# Get registered pipes
pipes = mrsm.get_pipes(tags=['production'], as_list=True)

# Build a connector
conn = mrsm.get_connector('sql:temp', flavor='sqlite', database='/tmp/tmp.db')
print(conn.read("SELECT 1 AS foo"))
#    foo
# 0    1

# Access a plugin's module
plugin = mrsm.Plugin('noaa')
with mrsm.Venv(plugin):
    noaa = plugin.module

# Create a custom connector class
@mrsm.make_connector
class FooConnector(mrsm.Connector):
    REQUIRED_ATTRIBUTES = ['username', 'password']

conn = mrsm.get_connector('foo:bar', username='foo', password='bar')
```

## Submodules

- <details>
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

- <details>
  <summary>
  `meerschaum.config`<br>
  Read and write the Meerschaum configuration registry.
  </summary>

  - `meerschaum.config.get_config()`
  - `meerschaum.config.get_plugin_config()`
  - `meerschaum.config.write_config()`
  - `meerschaum.config.write_plugin_config()`

</details>

- <details>
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

- <details>
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

<ul>
<details>
  <summary><code>meerschaum.utils</code><br>
  Utility functions are available in several submodules, such as functions for:<br><br>
  </summary>

  <ul>
  <details>
    <summary>
    <code>meerschaum.utils.daemon</code><br>

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
    `meerschaum.utils.dataframe`  
    Manipulating dataframes.\n
    </summary>

    - `meerschaum.utils.dataframe.TODO`

    </details>

  - `meerschaum.utils.dtypes`  
    Working with data types.\n
  - `meerschaum.utils.formatting`  
    Formatting output text.\n
  - `meerschaum.utils.misc`  
    Miscellaneous utility functions (e.g. `meerschaum.utils.misc.round_time()`).\n
  - `meerschaum.utils.packages`  
    Managing Python packages.\n
  - `meerschaum.utils.prompt`  
    Reading input from the user.\n
  - `meerschaum.utils.schedule`  
    Scheduling processes and threads.\n
  - `meerschaum.utils.sql`  
    Building SQL queries.\n
  - `meerschaum.utils.venv`  
    Managing virtual environments.\n
  - `meerschaum.utils.warnings`  
    Print warnings, errors, info, and debug messages.

</details>

"""
