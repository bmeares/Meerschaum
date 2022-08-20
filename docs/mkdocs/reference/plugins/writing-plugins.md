# ✍️ Writing Your Own Plugins

> This tutorial explains how to extend Meerschaum with plugins. For general information, consult the [Types of Plugins reference page](/reference/plugins/types-of-plugins/).

Meerschaum's plugin system is designed to be simple so you can get your plugins working quickly. Plugins are Python packages defined in the Meerschaum configuration `plugins` directory and are imported at startup under the global namespace `plugins`.

!!! warning "Performance Warning"
    To get the best performance and user experience, try to keep module-level code to a minimum ― especially heavy imports. Plugins are loaded at startup, and those tiny delays add up!
    ```python hl_lines="2"
    ### BAD - DON'T DO THIS
    import pandas as pd
    def fetch(pipe, **kw):
    	  return None
    ```
    ``` python hl_lines="3"
    ### GOOD - DO THIS INSTEAD
    def fetch(pipe, **kw):
    	  import pandas as pd
    	  return None
    ```

To create your plugin, follow these steps:

1. **Navigate to your Meerschaum plugins directory.**  

    - *Default:* `~/.config/meerschaum/plugins` (Windows: `%APPDATA%\Meerschaum\plugins`)
    - You may specify a path to a directory with the environment variable `MRSM_PLUGINS_DIR`. In this case, the folder can be named anything.
    - If you set `MRSM_ROOT_DIR`, then navigate to `$MRSM_ROOT_DIR/plugins`.

2. **Create your package file.**  
    You may either create `<plugin_name>.py` or `<plugin_name>/__init__.py` (in case your plugin needs submodules).

3. ***(Optional)* Define your plugin's `__version__` string.**

    !!! info "Plugin repositories need `__version__`"
        To publish changes to a repository with `register plugin`, you must increment `__version__` (according to [SemVer](https://semver.org/)). You may omit `__version__` for the initial release, but subsequent releases need the version defined.

    ```python
    # ~/.config/meerschaum/plugins/example.py
    __version__ = '0.0.1'
    ```

4. ***(Optional)* Define your required dependencies.**  

    Your plugin will be run from a virtual environment and therefore may not be able to import packages that aren't declared.

    These packagess will be installed into the plugin's virtual environment at installation or with `mrsm install required <plugins>`.

    !!! tip "Depend on other plugins"
        Dependencies that start with `plugin:` are Meerschaum plugins. To require a plugin from another repository, add add the repository's keys after the plugin name (e.g. `plugin:foo@api:bar`).

    ```python
    required = ['rich>=9.8.0', 'pandas', 'plugin:foo']
    ```

5. **Write your functions.**  
    
    Special functions are `#!python fetch()`, `#!python sync()`, `#!python register()`, `#!python setup()`, and `#!python <package_name>()`, and you can use the `#!python @make_action` decorator to designate additional functions as actions. Below is more information on these functions.

    !!! warning "Don't forget keyword arguments!"
        You must include a `**kwargs` argument to capture optional arguments. The functions `#!python fetch()`, `#!python sync()`, and `#!python register()` also require a `pipe` positional argument for the `#!python meerschaum.Pipe` object being synced.

        You may find the supplied arguments useful for your implementation (e.g. `begin` and `end` `#!python datetime.datetime` objects). Run `mrsm -h` or `mrsm show help` to see the available keyword arguments.

## Functions

Plugins are just modules with functions. This section explains the roles of the following special functions:

- **`#!python register(pipe: mrsm.Pipe, **kw)`**  
  Return a pipe's parameters dictionary (e.g. {'columns': {'datetime': 'timestamp', 'id': 'id'}}).
- **`#!python fetch(pipe: mrsm.Pipe, **kw)`**  
  Return a DataFrame (to be passed into `#!python Pipe.sync()` by the command `mrsm sync pipes`).
- **`#!python sync(pipe: mrsm.Pipe, **kw)`**  
  Override the default `sync` behavior for certain pipes.
- **`#!python @make_action`**  
  Create new commands.
- **`#!python @api_plugin`**  
  Create new FastAPI endpoints.
- **`#!python setup(**kw)`**  
  Executed upon plugin installation or with `mrsm setup plugin(s) <plugin>`.

### **The `#!python register()` Function**

The `register()` function is called whenever a new pipe is created with your plugin as its connector. This function returns a dictionary which will become your pipe's attributes. For example, if you already know the column names of your data stream, your `register()` function could be this one line:

```python
def register(pipe):
    return {'columns': {'datetime': 'timestamp', 'id': 'station'}}
```

The below example is the `register()` function from the [`noaa` plugin](/reference/plugins/list-of-plugins/#noaa):

??? example "Register function example"

    ```python
    def register(pipe: 'meerschaum.Pipe') -> Dict[str, Any]:
        """
        Prompt the user for stations when registering new pipes.
        """
        stations = ask_for_stations(pipe)
        return {
            'columns': {'datetime': 'timestamp', 'id': 'station',},
            'noaa': {'stations': stations,},
        }
    ```

### **The `#!python fetch()` Function**

The fastest way to leverage Meerschaum's syncing engine is with the `#!python fetch()` function. Simply return a DataFrame or a dictionary of lists.

!!! tip
    Just like when [writing sync plugins](#sync-plugins), there are additional keyword arguments available that you might find useful. Go ahead and inspect `**kw` and see if anything is helpful (e.g. `begin`, `end`, `blocking`, etc.). Check out [Keyword Arguments](#keyword-arguments) for further information.

Below is an example of what a typical fetch plugin may look like:
??? example "Fetch Plugin Example"
    ```python
    ### ~/.config/meerschaum/plugins/example.py

    __version__ = '0.0.1'
    required = []

    def register(pipe, **kw):
        return {
            'columns': {
                'datetime': 'dt',
                'id': 'id',
                'value': 'val',
            }
        }

    def fetch(pipe, **kw):
        import datetime, random
        return [{
            'dt': datetime.datetime.utcnow(),
            'id': 1,
            'val': random.randint(0, 100),
        }]
    ```

### **The `#!python sync()` Function**

The `#!python sync()` function makes `sync pipes` behave as a custom action on a per-pipe basis, allowing you more control over the syncing process.

Sync plugins allow for much more flexibility than fetch plugins, so what you come up with may differ from the following example. In any case, below is a simple `#!python sync()` plugin.

!!! note
    The only required argument is the positional `pipe` argument. The following example demonstrates one use of the `begin` and `end` arguments. Check out [Keyword Arguments](#keyword-arguments) for further information.

??? example "Sync Plugin Example"
    ```python
    ### ~/.config/meerschaum/plugins/example.py

    from __future__ import annotations
    from typing import Tuple, Any, Optional, Dict

    __version__ = '0.0.1'

    required = ['requests', 'pandas']

    def sync(
            pipe: meerschaum.Pipe,
            begin: Optional[datetime.datetime] = None,
            end: Optional[datetime.datetime] = None,
            params: Dict[str, Any] = None,
            **kw: Any
        ) -> Tuple[bool, str]:
        """
        This example `sync` plugin syncs multiple pipes.

        Parameters
        ----------
        pipe: meerschaum.Pipe
           The pipe to be synced.

        begin: Optional[datetime.datetime], default None
            The datetime to start searching for data (specified with `--begin`).

        end: Optional[datetime.datetime], default None
            The datetime to stop searching for data (specified with `--end`).

        kw: Any
            Additional keyword arguments you might find useful.

        Returns
        -------
        A tuple in the form (success [bool], message [str]).
        """

        ### Get data from somewhere. You decide how!
        import pandas as pd
        import requests
        import meerschaum as mrsm

        url = "https://api.example.com/json"
        params = params or {}
        if begin is not None:
            params['begin'] = begin.isoformat()
        if end is not None:
            params['end'] = end.isoformat()

        try:
            df = pd.read_json(requests.get(url, params=params).text)
        except Exception as e:
            df = None

        if df is None:
            return False, f"Failed to sync data from {url} with params: {params}"

        success, msg = pipe.sync(df)
        if not success:
            return success, msg

        another_pipe = mrsm.Pipe('foo', 'bar', instance='sql:local')
        return another_pipe.sync(df)
    ```

### **The `#!python @make_action` Decorator**

Your plugin may extend Meerschaum by providing additional actions. Actions are regular Python functions but with perks:

1. *Actions are integrated into the larger Meerschaum system.*  
    Actions from plugins may be executed as background jobs, via the web console, or via the API, just like standard actions.
2. *Actions inherit the standard Meerschaum keyword arguments.*  
    You can use flags such as `--begin`, `--end`, etc., or even [add your own custom flags](#keyword-arguments).

!!! tip "Sub-Actions"

    Actions with underscores are a good way to add sub-actions (e.g. `foo_bar` is the same as `foo bar`).

??? example "Action Plugin Example"

    ```python
    ### ~/.config/meerschaum/plugins/sing.py

    from meerschaum.plugins import make_action

    @make_action
    def sing_song(**kw):
        return True, "This action is called 'sing song'."

    @make_action
    def sing_tune(**kw):
        return True, "This action is called 'sing tune'."

    def sing(**kw):
        """
        Functions with the same name as the plugin are considered actions.
        """

        ### An action returns a tuple of success and message.
        ### If the success bool is `True` and the message is 'Success',
        ### nothing will be printed.

        return True, "Hello, World!"
    ```

#### Suggest Auto-Completions

Return a list of options from a function `complete_<action>()`, and these options will be suggested in the Meerschaum shell. The keyword arguments passed to `#!python complete_<action>()` are `line`, `sysargs`, `action`, and the currently parsed flags.

![Meerschaum shell auto-completion.](/assets/screenshots/shell-complete.png){align=left}

```python
@make_action
def foo_bar(**kw):
    return True, "Success"

def complete_foo_bar(**kw):
    return ['option 1', 'option 2']
```

### **The `#!python @api_plugin` Decorator**

Meerschaum plugins may also extend the web API by accessing the [`FastAPI`](https://fastapi.tiangolo.com/) app. Use the `#!python @api_plugin` decorator to define an initialization function that will be executed on the command `mrsm start api`.

The only argument for the initalization function should be `app` for the FastAPI application.

For your endpoints, arguments will be used as HTTP parameters, and to require that the user must be logged in, import `manager` from `meerschaum.api` and add the argument `curr_user = fastapi.Depends(manager)`:

!!! tip "Swagger Endpoint Tester"
    
    Navigate to [https://localhost:8000/docs](https://localhost:8000/docs) to test your endpoint in the browser.

??? example "API Plugin Example"

    ```python
    ### ~/.config/meerschaum/plugins/example.py

    from meerschaum.plugins import api_plugin

    @api_plugin
    def init_plugin(app):
        """
        This function is executed immediately after the `app` is initialized.
        """

        import fastapi
        from meerschaum.api import manager

        @app.get('/my/new/path')
        def new_path(
            curr_user = fastapi.Depends(manager)
        ):
            """
            This is my new API endpoint.
            """
            return {'message': f'The current user is {curr_user.name}'}
    ```

    ![Custom Meerschaum API endpoint which requires a login.](/assets/screenshots/api-plugin-endpoint-login-required.png)


### **The `#!python setup()` Function**

When your plugin is first installed, its `required` list will be installed, but in case you need to do any extra setup upon installation, you can write a `#!python setup()` function which returns a tuple of a boolean and a message.

Below is a snippet from the `apex` plugin which initializes a Selenium WebDriver.

??? example "Setup Function Example"

    ```python
    def setup(**kw):
        global geckodriver_location
        try:
            from webdriver_manager.firefox import GeckoDriverManager
            geckodriver_location = GeckoDriverManager().install()
        except Exception as e:
            return False, str(e)
        return True, "Success"
    ```

## Keyword Arguments

There are a many useful command line arguments provided to plugins as keyword arguments. To add customizability to your plugins, consider inspecting the keys in the `#!python **kwargs` dictionary.

!!! tip
    You can see the value of `#!python **kwargs` with the command `mrsm show arguments`.

??? example "Inspecting `**kwargs`"
    ```python
	### ~/.config/meerschaum/plugins/example.py

	def example(**kwargs):
		from meerschaum.utils.formatting import pprint
		pprint(kwargs)
		return True, "Success"
	```

### Useful Command Line and Keyword Arguments

You can see the available command line options with the command `mrsm show help` or `mrsm --help`. Expand the table below to see more information about usefult builtin arguments.

??? info "Useful Keyword Arguments"

    | Keyword Argument            | Command Line Flags                          | Type                                 | Examples                              | Description                                                  |
    | --------------------------- | ------------------------------------------- | ------------------------------------ | ------------------------------------- | ------------------------------------------------------------ |
    | `action`                    | Positional arguments                        | `Optional[List[str]]`                | `['foo', 'bar']`                      | The positional action arguments following the action are passed into the `action` list. For example, the command `mrsm example foo bar` will be parsed into the list `['foo', 'bar']` for the action function `example(action : Optional[List[str]] = None)`. |
    | `yes`                       | `-y`, `--yes`                               | `Bool = False`                       | `False` (default), `True`             | When a user provides the `-y` flag, yes/no prompts should default to the `yes` option. You can easily use this functionality with the builtin [`#!python yes_no()` function](https://docs.meerschaum.io/utils/prompt.html). |
    | `force`                     | `-f`, `--force`                             | `Bool = False`                       | `False` (default), `True`             | The `-f` flag implies `-y`. Generally, when `-f` is passed, you may skip asking for confirmation altogether. |
    | `loop`                      | `--loop`                                    | `Bool = False`                       | `False` (default), `True`             | The `--loop`  flag implies that the action should be continuously executed. The exact looping logic is left up to the action developer and therefore is only used in certain contexts (such as in `sync pipes`). |
    | `begin`                     | `--begin`                                   | `Optional[datetime.datetime] = None` | `datetime.datetime(2021, 1, 1, 0, 0)` | A user may provide a `begin` datetime. Consult `sync pipes` or `show data` for examples. |
    | `end`                       | `--end`                                     | `Optional[datetime.datetime] = None` | `datetime.datetime(2021, 1, 1, 0, 0)` | A user may provide an `end` datetime. Consult `sync pipes` or `show data` for examples. |
    | `connector_keys`            | `-c`, `-C`, `--connector-keys`              | `Optional[List[str]] = None`         | `['sql:main', 'sql:remote']`          | The list of [connectors](/reference/connectors) is used to filter [pipes](/reference/pipes). This filtering is done with the [`get_pipes()` function](https://docs.meerschaum.io/utils/get_pipes.html). |
    | `metric_keys`               | `-m`, `-M`, `--metric-keys`                 | `Optional[List[str]] = None`         | `['weather', 'power']`                | The list of metrics is used to filter [pipes](/reference/pipes). This filtering is done with the [`get_pipes()` function](https://docs.meerschaum.io/utils/get_pipes.html). |
    | `location_keys`             | `-l`, `-L`, `--location-keys`               | `Optional[List[str]] = None`         | `[None, 'clemson']`                   | The list of metrics is used to filter [pipes](/reference/pipes). This filtering is done with the [`get_pipes()` function](https://docs.meerschaum.io/utils/get_pipes.html). The location `None` is preserved and always parsed into `None` (`NULL`). |
    | `mrsm_instance`, `instance` | `-i`, `-I`, `--instance`, `--mrsm_instance` | `Optional[str] = None`               | `'sql:main'`                          | The connector keys of the corresponding [Meerschaum instance](/reference/connectors/#instances-and-repositories). This filtering is done with the [`get_pipes()` function](https://docs.meerschaum.io/utils/get_pipes.html). If `mrsm_instance` is `None` (default), the configured instance will be used (`'sql:main'` by default). When actions are launched from within the Meerschaum shell, the current instance is passed. |
    | `repository`                | `-r`, `--repository`, `--repo`              | `Optional[str] = None`               | `'api:mrsm'`                          | The connector keys of the corresponding [Meerschaum repository](/reference/connectors/#instances-and-repositories). If `repository` is `None` (default), the configured repository will be used (`'api:mrsm'` by default). When actions are launched from within the Meerschaum shell, the current repository is passed. |


### Custom Command Line Options

In case the built-in command line options are not sufficient, you can add arguments with `#!python add_plugin_argument()`. This function takes the same arguments as the `#!python parser.add_argument()` function from [`argparse`](https://docs.python.org/3/library/argparse.html) and will include your plugin's arguments in the `--help` text.

```python
### ~/.config/meerschaum/plugins/example.py

from meerschaum.actions.arguments import add_plugin_argument
add_plugin_argument('--foo', type=int, help="This is my help text!")

```

The above code snippet will produce append the following text to the `--help` or `show help` text:

```
Plugin 'example' options:
  --foo FOO             This is my help text!
```

### `sysargs` , `shell`, and Other Edge Cases

Generally, using built-in or custom arguments mentioned above should cover almost every use case. In case you have specific needs, the arguments `sysargs`, `sub_args`, `filtered_sysargs`, `shell`, and `line` are provided.

??? info "Edge Case Keyword Arguments"

    | Keyword Argument   | Type                         | Example                          | Description                                                  |
    | ------------------ | ---------------------------- | -------------------------------- | ------------------------------------------------------------ |
    | `sysargs`          | `List[str]`                  | `['ls', '[-l]', '[-a]', '[-h]']` | The `sysargs` keyword corresponds to `sys.argv[1:]`.         |
    | `line`             | `Optional[List[str]] = None` | `'ls [-l] [-a] [-h]'`            | The `line` keyword is a string which corresponds to `sysargs` joined by spaces. `line` is only provided when the Meerschaum shell is used. |
    | `sub_args`         | `Optional[List[str]] = None` | `['-l', '-a', -h']`              | The `sub_args` keyword corresponds to items in `sysargs` enclosed in square brackets (`[]`). |
    | `filtered_sysargs` | `List[str]`                  | `['ls']`                         | `filtered_sysargs` contains the values of `sysargs` without `sub_args`. |
    | `shell`            | `Bool`                       | `True`, `False`                  | The `shell` boolean indicates whether or not an action was executed from within the Meerschaum shell. |


## Working With Plugins

Plugins are just Python modules, so you can write custom code and share it amongst other plugins (i.e. a [library plugin](/reference/plugins/types-of-plugins/#-library-plugins)).

At run time, plugins are imported under the global `plugins` namespace, but you'll probably be testing plugins directly when the `plugins` namespace isn't created. That's where [`Plugin` objects](https://docs.meerschaum.io/#meerschaum.Plugin) come in handy: they contain a number of convenience functions so you can cross-pollinate between plugins.

### Import Another Plugin

Accessing the member `module` of the `Plugin` object will import its module:

```python
# ~/.config/meerschaum/plugins/foo.py
import meerschaum as mrsm
bar = mrsm.Plugin('bar').module
```

### Plugins in a REPL

Plugins run from virtual environments, so to import your plugin in a REPL, you'll need to activate its environment before importing:

```python
>>> import meerschaum as mrsm
>>> plugin = mrsm.Plugin('foo')
>>> plugin.activate_venv()
>>> foo = plugin.module
```

### Plugins in Scripts

You can also pass a plugin to the [`Venv` virtual environment manager](https://docs.meerschaum.io/utils/venv/index.html#meerschaum.utils.venv.Venv), which handles activating and deactivating environments. 

```python
from meerschaum.utils.venv import Venv
from meerschaum import Plugin
plugin = Plugin('foo')
with Venv(plugin):
    foo = plugin.module
```