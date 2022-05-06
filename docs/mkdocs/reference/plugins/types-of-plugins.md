# 🤔 Types of Plugins

> This page introduces the different types of Meerschaum plugins. For technical information, consult the [Writing Your Own Plugins tutorial](/reference/plugins/writing-plugins/).

There are three main kinds of Meerschaum plugin: those which **provide data**, **perform actions**, or **extend the API**, and you may choose to use any or all of the three purposes. You may also create [library plugins](#library-plugins) to contain utilities for other plugins.

## 📈 Data Plugins

Data plugins retrieve and parse data, then pass it on to Meerschaum for later analysis. There are two types of data plugins: [**fetch**](#fetch-plugins) and [**sync**](#sync-plugins) plugins.

### 🐶 Fetch Plugins

Fetch plugins are the most straightforward: they pull data from some data source and return a dictionary or Pandas DataFrame. For example, one common use-case for a fetch plugin is to parse JSON data from a web API. All you need to do is retrieve and parse data, and Meerschaum handles [filtering out duplicates](https://docs.meerschaum.io/#meerschaum.Pipe.filter_existing) and updating your tables.

??? example "Fetch plugin example"
    ```python
    # ~/.config/meerschaum/plugins/foo.py
    __version__ = '0.0.1'
    required = []

    import datetime
    from meerschaum.utils.typing import Dict, List

    def register(pipe, **kw) -> Dict[str, Dict[str, str]]:
        return {'columns': {'datetime': 'dt'}}

    def fetch(pipe, **kw) -> Dict[List[datetime.datetime]]:
        """You can return a pandas DataFrame or a dictionary of lists."""
        return {'dt': [datetime.datetime.utcnow()]}
    ```

### 📥 Sync Plugins

Like fetch plugins, sync plugins define how to get and parse data. Sync plugins, however, override the built-in `sync` process and give you complete control over the syncing process. For example, you could get really fancy with multiprocessing, distributed computing, or creating additional pipes.

??? example "Sync plugin example"
    ```python
    # ~/.config/meerschaum/plugins/foo.py
    __version__ = '0.0.1'
    required = []

    import datetime
    from meerschaum.utils.typing import Dict, SuccessTuple

    def register(pipe, **kw) -> Dict[str, Dict[str, str]]:
        return {'columns': {'datetime': 'dt'}}

    def sync(pipe, **kw) -> SuccessTuple:
        data = {'dt': [datetime.datetime.utcnow()]}
        success, msg = pipe.sync(data, **kw)
        if not success:
            return False, "Oopsie! Received error:\n" + msg
        return True, "Success"
    ```

## ⏯️ Action Plugins

Action plugins add additional commands to Meerschaum, such as the [built-in actions](/reference/actions) like `sync`, `bootstrap`, and `show`. The sky is the limit for actions ― the action function serves as an entry point from `mrsm`.

For example, the `color` plugin provides the `color` action, which is a convenience command to toggle the shell's Unicode and ANSI configuration.

An action plugin can provide multiple actions, and because plugins are loaded last, there is potential for overwriting built-in actions and greatly extending Meerschaum.

Actions are a blank slate, and I'm really excited to see the creativity the community comes up with!

??? example "Action plugin example"
    ```python
    # ~/.config/meerschaum/plugins/foo.py
    __version__ = '0.0.1'
    required = []

    from meerschaum.plugins import make_action
    from meerschaum.utils.typing import Optional, List, SuccessTuple

    @make_action
    def foo(action: Optional[List[str]] = None, **kw) -> SuccessTuple:
        """Echo back the words that follow `mrsm foo`."""
        if not action:
            return False, "You didn't say anything."
        return True, "You said: " + ' '.join(action)

    @make_action
    def bar(**kw) -> SuccessTuple:
        """Help text for the command `bar`."""
        from meerschaum.utils.formatting import pprint
        print("This is the entry from the command `mrsm bar`.")
        print("These are the flags you provided:")
        pprint(kw)
        return True, "Success"
    ```

## 📚 Library Plugins

You can list other plugins in your [`required` list](/reference/plugins/writing-plugins/), so you could create a foundational plugin that your other plugins rely upon.

??? example "Library plugin example"
    The cleanest way to cross-pollinate your plugins (or use them outside Meerschaum) is with [`mrsm.Plugin`](https://docs.meerschaum.io/#meerschaum.Plugin).

    For example, let's create a plugin `lib` to act as our common library plugin:

    ```python
    # ~/.config/meerschaum/plugins/lib.py
    __version__ = '1.0.0'
    required = ['pandas']

    def transform_df(df: 'pd.DataFrame') -> 'pd.DataFrame':
        df['foo'] = 'bar'
        return df
    ```

    Because `lib.py` above lists `pandas` in its `required` list, plugins which rely upon it inherit its dependencies without needing to reinstall them. Consider plugin `foo` below:

    ```python
    # ~/.config/meerschaum/plugins/foo.py
    __version__ = '0.0.1'
    required = ['plugin:lib']

    import meerschaum as mrsm
    import datetime
    lib = mrsm.Plugin('lib')

    def register(pipe, **kw):
        return {'columns': {'datetime': 'dt'}}

    def fetch(pipe, **kw) -> 'pd.DataFrame':
        ### inherited from `lib`; no need to specify in `required`
        import pandas as pd

        df = pd.DataFrame({'dt': [datetime.datetime.utcnow()]})

        return lib.module.transform_df(df)
    ```


## 🌐 API Plugins

Plugins may also be used to extend the Meerschaum Web API by adding endpoints. For example, an API plugin may be written to integrate Meerschaum's web API functionality with an existing login system, such as Google SSO. Rather than writing an API backend from the ground up, Meerschaum API plugins allow you to directly connect web requests with your custom Meerschaum back-end.

??? example "API plugin example"

  ```python
  # ~/.config/meerschaum/plugins/foo.py
  from meerschaum.plugins import api_plugin

  @api_plugin
  def init_api(app):
      @app.get('/my/new/path')
      def my_new_path():
          return {'message': 'Eureka!'}
  ```
