# Writing Your Own Plugins

> This tutorial explains how to extend Meerschaum with plugins. For general information, consult the [Types of Plugins reference page](/reference/plugins/types-of-plugins/).

Meerschaum's plugin system is designed to be simple so you can get your plugins working quickly. Plugins are Python packages defined in the Meerschaum configuration `plugins` directory and are imported at startup.

!!! warning "Performance Warning"
    To get the best performance and user experience, try to keep module-level code to a minimum ― especially imports. Plugins are loaded at startup, and those tiny delays add up!
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

1. Navigate to the Meerschaum plugins directory.

    === "Linux / Mac OS"

        ```console
        ~/.config/meerschaum/plugins/
        ```

    === "Windows"

        ```DOS
        %AppData%\Meerschaum\plugins\
        ```

    === "Meerschaum Portable"

        ```console
        root/plugins/
        ```

2. Create your package file. You may either create a `<plugin_name>.py` file or `<plugin_name>/__init__.py` directory and file (in case your plugin needs sub-modules).

3. *(Optional)* Define your plugin's `__version__` string.

    !!! info "Specify the version number"
        You must increment your `__version__` (according to [SemVer](https://semver.org/)) to publish changes to a repository. You may omit `__version__` for the initial release, but subsequent releases need the version defined.

    ```python
    __version__ = '0.0.1'
    ```

4. *(Optional)* Define your required dependencies. Your plugin will be run from a virtual environment and therefore may not be able to import packages that aren't declared.

    !!! tip "Depend on other plugins"
        Dependencies that start with `plugin:` will be treated as other Meerschaum plugins in the same repository.

    ```python
    required = ['rich>=9.8.0', 'pandas', 'plugin:foo']
    ```

5. Write your functions. Special functions are `#!python fetch()`, `#!python sync()`, `#!python register()`, and `#!python <package_name>()`, and you can use the `#!python @make_action` decorator to designate additional functions as actions. Below is more information on these functions.

    !!! warning "Don't forget keyword arguments!"
        You must include a `**kwargs` argument to capture optional arguments. You may find the supplied arguments useful for your implementation (e.g. `begin` and `end` `#!python datetime.datetime` objects). For fetch and sync plugins, you need a `pipe` positional argument for the `#!python meerschaum.Pipe` object being synced.

## Functions

Plugins are just modules with functions. This section explains the roles of the following special functions:

- [**`#!python fetch()`**](#fetch)  
  Return a DataFrame (which is later passed into `#!python Pipe.sync()` by the command `sync pipes`).
- [**`#!python sync()`**](#sync)  
  Override the default `sync` behavior for certain pipes.
- [**`#!python @make_action`**](#make_action)  
  Create new commands.
- [**`#!python @api_plugin`**](#api_plugin)  
  Create new FastAPI endpoints.
- [**`#!python setup()`**](#setup)  
  Executed upon plugin installation.
- [**`#!python register()`**](#register)  
  Executed when new pipes are registered to set their attributes.

### `#!python fetch()`

If your data source isn't too complicated, you can leverage Meerschaum's built-in syncing functionality by writing a [fetch plugin](/reference/plugins/types-of-plugins/#fetch-plugins). You might want to write a fetch plugin for the following reasons:

- Your source consists of a single stream of data.
- You are parsing your own data files.
- You don't want to sync pipes yourself.

!!! tip
    Just like when [writing sync plugins](#sync-plugins), there are additional keyword arguments available that you might find useful. Go ahead and inspect `**kw` and see if anything is helpful (e.g. `begin`, `end`, `blocking`, etc.). Check out [Keyword Arguments](#keyword-arguments) for further information.

Below is an example of what a typical fetch plugin may look like:
??? example "Fetch Plugin Example"
    ```python
    ### ~/.config/meerschaum/plugins/example.py

    __version__ = '0.0.1'

    required = ['requests', 'pandas']

    def fetch(pipe, **kw):
        ### Define columns if undefined.
        if pipe.columns is None:
            pipe.columns = { 'datetime' : 'dt_col' }
            pipe.edit(interactive=False)

        ### Do something here to build a dictionary of lists or DataFrame
        import pandas as pd
        import requests
        url = "https://api.example.com/json"
        try:
        	 df = pd.read_json(requests.get(url).text)
        except:
          df = None
        return df
    ```

### `#!python sync()`

Sometimes Meerschaum's built-in syncing process isn't enough for your needs. You can override the syncing process with your own `#!python sync()` method. Sync plugins have much more freedom than fetch plugins, so what you come up with may differ from the following example. In any case, below is a simple `#!python sync()` plugin.

!!! note
    The only required argument is the positional `pipe` argument. The following example demonstrates one use of the `begin` and `end` arguments. Check out [Keyword Arguments](#keyword-arguments) for further information.

??? example "Sync Plugin Example"
    ```python
    ### ~/.config/meerschaum/plugins/example.py

    from __future__ import annotations
    from typing import Tuple, Any, Optional, Mapping

    __version__ = '0.0.1'

    required = ['requests', 'pandas']

    def sync(
            pipe : meerschaum.Pipe,
            begin : Optional[datetime.datetime] = None,
            end : Optional[datetime.datetime] = None,
            params : Mapping[str, Any] = None,
            **kw : Any
        ) -> Tuple[bool, str]:
        """
        An example `sync` plugin.
        Returns a tuple of (success, message) [bool, str].

        :param pipe:
            The pipe to be synced.

        :param begin:
            The datetime to start searching for data (specified with `--begin`).
            Defaults to `None`.

        :param end:
            The datetime to stop searching for data (specified with `--end`).
            Defaults to `None`.

        :param kw:
            Additional keyword arguments you might find useful.
        """

        ### Get data from somewhere. You decide how!
        import pandas as pd
        import requests

        url = "https://api.example.com/json"
        params = {} if params is None else params
        if begin is not None:
            params['begin'] = begin.isoformat()
        if end is not None:
            params['end'] = end.isoformat()

        try:
            df = pd.read_json(requests.get(url, params=params).text)
        except:
            df = None

        if df is None:
            return False, f"Failed to sync data from {url} with params: {params}"

        return pipe.sync(df)
    ```

### `#!python @make_action`

Your plugin may extend Meerschaum by providing additional actions. Action plugins are like regular Python scripts but come with two advantages:

1. *Actions are integrated into the larger Meerschaum system.*  
    Actions from plugins may be executed as background jobs, via the web console, or via the API, just like standard actions.
2. *Actions inherit the standard Meerschaum keyword arguments.*

For example, let's write the plugin `example.py` which will provide the action `example`. Functions which share the name of the plugin are automatically considered actions.

```python
### ~/.config/meerschaum/plugins/example.py

def example(**kw):
  	"""
  	This the help string for the new action `example`.
  	"""

  	### An action returns a tuple of success and message.
  	### If the success bool is `True` and the message is 'Success',
  	### nothing will be printed.

  	return True, "Hello, World!"
```

#### Multiple Actions

Action plugins can create any number of actions. To make a function into an action, use the `#!python @make_action` decorator.

```python
### ~/.config/meerschaum/plugins/example.py

from meerschaum.plugins import make_action

@make_action
def myaction(**kw):
    return True, "This action is called 'myaction'"

@make_action
def anotheraction(**kw):
    return True, "This action is called 'anotheraction'"
```

### `#!python @api_plugin`

Meerschaum plugins may also extend the web API by accessing the [`fastapi`](https://fastapi.tiangolo.com/) app. To delay importing the app until the API is actually initialized, use the `#!python @api_plugin` decorator to define an intialization function. The only required argument for your function should be a variable for the `fastapi` app.

```python
### ~/.config/meerschaum/plugins/example.py

from meerschaum.plugins import api_plugin

@api_plugin
def init_plugin(app):
    """
    This function is executed immediately after the `app` is initialized.
    """

    @app.get('/my/new/path')
    def new_path():
        return {'message': 'Hello, World!'}
```

### `#!python setup()`

When your plugin is first installed, its `required` list will be installed, but in case you need to do any extra setup upon installation, you can write a `#!python setup()` function which returns a tuple of a boolean and a message.

Below is a snippet from the `apex` plugin which initializes a Selenium WebDriver.

??? example "Setup function example"

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

### `#!python register()`

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
