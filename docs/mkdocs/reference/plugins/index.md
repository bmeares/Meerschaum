# ➕ Plugins

Meerschaum plugins let you ingest any data source into [pipes](/reference/pipes) as well as defining custom actions, API endpoints, and more.

!!! question "What can I do with plugins?"

    Plugins are just Python modules, so the sky is the limit as to what's possible. The main idea behind plugins is to connect to any arbitrary data source: read more about the [types of plugins](/reference/plugins/types-of-plugins/) to get into the specifics.

!!! question "If plugins are just Python modules, why don't I just use a normal Python package?"

    Of course you can still use Meerschaum in typical Python packages! The plugins interface provides these benefits for your convenience, however:

    - **No boilerplate.**  
      You might only need to define a short `fetch()` function. Skip `setup.py` / `pyproject.toml` and write only what you need.

    - **Fearless refactoring.**  
      Plugins are self-contained and portable, which means you can safely refactor your plugins without worrying about breaking imports somewhere else in your codebase.

    - **Get core functionality for free.**  
      Writing your module as a plugin unlocks access to the rest of the Meerschaum system, e.g. the [connector management system](/reference/connectors), [date-bounded syncing](/reference/pipes/syncing/), and the [Meerschaum Compose workflow](/reference/compose/).

!!! question "Ok, I think I understand. How do I make my own plugins?"

    Here is the [complete guide to writing your own plugins](/reference/plugins/writing-plugins/), but the TL;DR is this:

    1. Create a new file `example.py` in `~/.config/meerschaum/plugins/` (Windows: `%APPDATA%\Meerschaum\plugins\`).

    2. Paste this starter code:

        ```python
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

    3. Create a new pipe with your plugin as the connector and sync data into it.

        ```
        mrsm register pipe -c plugin:example -m test
        ```
        ```
        mrsm sync pipes -c plugin:example
        ```
