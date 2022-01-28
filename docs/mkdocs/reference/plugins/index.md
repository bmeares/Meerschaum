# ➕ Plugins

Meerschaum plugins let you ingest any data source into [pipes](/reference/pipes) as well as definiting custom actions, API endpoints, and more.

!!! faq "What can I do with plugins?"

    Plugins are just Python modules, so the sky is the limit as to what's possible. The main idea behind plugins is to connect to any arbitrary data source: read more about the [types of plugins](/reference/plugins/types-of-plugins/) to get into the specifics.

!!! faq "How do I use plugins?"

    Check out [this page](/reference/plugins/using-plugins/) for information on installing, hosting, and executing your plugins.

!!! faq "Ok, I think I understand. How do I make my own plugins?"

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
            return {
                'dt': [datetime.datetime.utcnow()],
                'id': [1],
                'val': [random.randint(0, 100)],
            }
        ```

    3. Create a new pipe with your plugin as the connector and sync data into it.

        ```
        mrsm register pipe -c plugin:example -m test
        ```
        ```
        mrsm sync pipes -c plugin:example
        ```
