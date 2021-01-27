# Writing Your Own Plugins

Meerschaum's plugin system is designed to be simple so you can get your plugins working quickly. Plugins are Python packages defined in the Meerschaum configuration `plugins` directory and are imported at startup into `meerschaum.actions.plugins`.

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

2. Create your package file. You may either create a `<plugin_name>.py` file or `<plugin_name>/__init__.py` directory and file (in case your plugin needs sub-modules).

3. *(Optional)* Define your plugin's `__version__` string.

    !!! info
        You must increment your `__version__` to publish changes to a repository. You may omit `__version__` for the initial release, but subsequent releases need the version defined.

    ```python
    __version__ = '0.0.1'
    ```

4. *(Optional)* Define your required dependencies. Your plugin will be run from a virtual environment and therefore may not be able to import packages that aren't declared.

    ```python
    required = ['rich>=9.8.0', 'pandas']
    ```

5. Write your functions. Special functions are `#!python fetch()`, `#!python sync()`, and `#!python <package_name>()`, and you can use the `#!python @make_action` decorator to designate additional functions as actions. Below is more information on these functions.

    !!! info
        You must include a `**keywords` argument to capture optional arguments. You may find the supplied arguments useful for your implementation (e.g. `begin` and `end` `#!python datetime.datetime` objects). For fetch and sync plugins, you need a `pipe` positional argument for the `#!python meerschaum.Pipe` object being synced.


## Writing Fetch Plugins

> **Difficulty:** Easy

If your data source isn't very complicated, you can leverage Meerschaum's built-in syncing functionality by writing a [fetch plugin](#fetch-plugins). You might want to write a fetch plugin for the following reasons:

- Your source consists of a single stream of data.
- You are parsing your own data files.
- You don't want to sync pipes yourself.

!!! tip
    Just like when [writing sync plugins](#writing-sync-plugins), there are additional keyword arguments available that you might find useful. Go ahead and inspect `**kw` and see if anything is helpful (e.g. `begin`, `end`, `blocking`, etc.). Check out [Plugin Keyword Arguments](#plugin-keyword-arguments) for further information.

Below is an example of what a typical fetch plugin may look like:
??? example "Fetch Plugin Example"
    ```python
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

## Writing Sync Plugins

> **Difficulty:** Medium

Sometimes Meerschaum's built-in syncing process isn't enough for your needs. You can override the syncing process with your own `#!python sync()` method. Sync plugins have much more freedom than fetch plugins, so what you come up with may differ from the following example. In any case, below is a simple `#!python sync()` plugin.

!!! note
    The only required argument is the positional `pipe` argument. The following example demonstrates one use of the `begin` and `end` arguments. Check out [Plugin Keyword Arguments](#plugin-keyword-arguments) for further information.

??? example "Sync Plugin Example"
    ```python
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
            Defaults to None.
    
        :param end:
            The datetime to stop searching for data (specified with `--end`).
            Defaults to None.
    
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

## Writing Action Plugins
Your plugin may

## Plugin Keyword Arguments

TODO