#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define the bootstrapping wizard for creating plugins.
"""

import re
import pathlib
import meerschaum as mrsm
from meerschaum.utils.typing import Any, SuccessTuple, Dict, List
from meerschaum.utils.warnings import warn, info
from meerschaum.utils.prompt import prompt, choose, yes_no
from meerschaum.utils.formatting._shell import clear_screen

FEATURE_CHOICES: Dict[str, str] = {
    'fetch'    : 'Fetch data\n     (e.g. extracting from a remote API)\n',
    'connector': 'Connector\n     (fetch data & manage credentials)\n',
    'instance-connector': 'Instance connector\n     (implement the pipes interface)\n',
    'action'   : 'New actions\n     (e.g. `mrsm sing song`)\n',
    'api'      : 'New API endpoints\n     (e.g. `POST /my/new/endpoint`)\n',
    'web'      : 'New web console page\n     (e.g. `/dash/my-web-app`)\n',
}

IMPORTS_LINES: Dict[str, str] = {
    'stdlib': (
        "from datetime import datetime, timedelta, timezone\n"
        "from typing import Any, Union, List, Dict\n"
    ),
    'default': (
        "import meerschaum as mrsm\n"
        "from meerschaum.config import get_plugin_config, write_plugin_config\n"
    ),
    'connector': (
        "from meerschaum.connectors import Connector, make_connector\n"
    ),
    'instance-connector': (
        "from meerschaum.connectors import InstanceConnector, make_connector\n"
    ),
    'action': (
        "from meerschaum.actions import make_action\n"
    ),
    'api': (
        "from meerschaum.plugins import api_plugin\n"
    ),
    'web': (
        "from meerschaum.plugins import web_page, dash_plugin\n"
    ),
    'api+web': (
        "from meerschaum.plugins import api_plugin, web_page, dash_plugin\n"
    ),
}

FEATURE_LINES: Dict[str, str] = {
    'header': (
        "#! /usr/bin/env python3\n"
        "# -*- coding: utf-8 -*-\n\n"
        "\"\"\"\n"
        "Implement the plugin '{plugin_name}'.\n\n"
        "See the Writing Plugins guide for more information:\n"
        "https://meerschaum.io/reference/plugins/writing-plugins/\n"
        "\"\"\"\n\n"
    ),
    'default': (
        "__version__ = '0.0.1'\n"
        "\n# Add any dependencies to `required` (similar to `requirements.txt`).\n"
        "required: list[str] = []\n\n\n"
    ),
    'setup': (
        "def setup(**kwargs) -> mrsm.SuccessTuple:\n"
        "    \"\"\"Executed during installation and `mrsm setup plugin {plugin_name}`.\"\"\"\n"
        "    return True, \"Success\"\n\n\n"
    ),
    'register': (
        "def register(pipe: mrsm.Pipe):\n"
        "    \"\"\"Return the default parameters for a new pipe.\"\"\"\n"
        "    return {{\n"
        "        'columns': {{\n"
        "            'datetime': {dt_col_name},\n"
        "        }}\n"
        "    }}\n\n\n"
    ),
    'fetch': (
        "def fetch(\n"
        "    pipe: mrsm.Pipe,\n"
        "    begin: datetime | None = None,\n"
        "    end: datetime | None = None,\n"
        "    **kwargs\n"
        "):\n"
        "    \"\"\"Return or yield dataframes.\"\"\"\n"
        "    docs = []\n"
        "    # populate docs with dictionaries (rows).\n"
        "    return docs\n\n\n"
    ),
    'connector': (
        "@make_connector\n"
        "class {plugin_name_capitalized}Connector(Connector):\n"
        "    \"\"\"Implement '{plugin_name_lower}' connectors.\"\"\"\n\n"
        "    REQUIRED_ATTRIBUTES: list[str] = []\n"
        "\n"
        "    def fetch(\n"
        "        self,\n"
        "        pipe: mrsm.Pipe,\n"
        "        begin: datetime | None = None,\n"
        "        end: datetime | None = None,\n"
        "        **kwargs\n"
        "    ):\n"
        "        \"\"\"Return or yield dataframes.\"\"\"\n"
        "        docs = []\n"
        "        # populate docs with dictionaries (rows).\n"
        "        return docs\n\n\n"
    ),
    'instance-connector': (
        "@make_connector\n"
        "class {plugin_name_capitalized}Connector(InstanceConnector):\n"
        "    \"\"\"Implement '{plugin_name_lower}' connectors.\"\"\"\n\n"
        "    REQUIRED_ATTRIBUTES: list[str] = []\n"
        "\n"
        "    def fetch(\n"
        "        self,\n"
        "        pipe: mrsm.Pipe,\n"
        "        begin: datetime | None = None,\n"
        "        end: datetime | None = None,\n"
        "        **kwargs\n"
        "    ):\n"
        "        \"\"\"Return or yield dataframes.\"\"\"\n"
        "        docs = []\n"
        "        # populate docs with dictionaries (rows).\n"
        "        return docs\n\n"
        """
    def register_pipe(
        self,
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kwargs: Any
    ) -> mrsm.SuccessTuple:
        \"\"\"
        Insert the pipe's attributes into the internal `pipes` table.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe to be registered.

        Returns
        -------
        A `SuccessTuple` of the result.
        \"\"\"
        attributes = {{
            'connector_keys': str(pipe.connector_keys),
            'metric_key': str(pipe.metric_key),
            'location_key': str(pipe.location_key),
            'parameters': pipe._attributes.get('parameters', {{}}),
        }}

        ### TODO insert `attributes` as a row in the pipes table.
        # self.pipes_collection.insert_one(attributes)

        return True, \"Success\"

    def get_pipe_attributes(
        self,
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kwargs: Any
    ) -> dict[str, Any]:
        \"\"\"
        Return the pipe's document from the internal `pipes` collection.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe whose attributes should be retrieved.

        Returns
        -------
        The document that matches the keys of the pipe.
        \"\"\"
        query = {{
            'connector_keys': str(pipe.connector_keys),
            'metric_key': str(pipe.metric_key),
            'location_key': str(pipe.location_key),
        }}
        ### TODO query the `pipes` table either using these keys or `get_pipe_id()`.
        result = {{}}
        # result = self.pipes_collection.find_one(query) or {{}}
        return result

    def get_pipe_id(
        self,
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kwargs: Any
    ) -> Union[str, int, None]:
        \"\"\"
        Return the `_id` for the pipe if it exists.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe whose `_id` to fetch.

        Returns
        -------
        The `_id` for the pipe's document or `None`.
        \"\"\"
        query = {{
            'connector_keys': str(pipe.connector_keys),
            'metric_key': str(pipe.metric_key),
            'location_key': str(pipe.location_key),
        }}
        ### TODO fetch the ID mapped to this pipe.
        # oid = (self.pipes_collection.find_one(query, {{'_id': 1}}) or {{}}).get('_id', None)
        # return str(oid) if oid is not None else None

    def edit_pipe(
        self,
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kwargs: Any
    ) -> mrsm.SuccessTuple:
        \"\"\"
        Edit the attributes of the pipe.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe whose in-memory parameters must be persisted.

        Returns
        -------
        A `SuccessTuple` indicating success.
        \"\"\"
        query = {{
            'connector_keys': str(pipe.connector_keys),
            'metric_key': str(pipe.metric_key),
            'location_key': str(pipe.location_key),
        }}
        pipe_parameters = pipe._attributes.get('parameters', {{}})
        ### TODO Update the row with new parameters.
        # self.pipes_collection.update_one(query, {{'$set': {{'parameters': pipe_parameters}}}})
        return True, "Success"

    def delete_pipe(
        self,
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kwargs: Any
    ) -> mrsm.SuccessTuple:
        \"\"\"
        Delete a pipe's registration from the `pipes` collection.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe to be deleted.

        Returns
        -------
        A `SuccessTuple` indicating success.
        \"\"\"
        ### TODO Delete the pipe's row from the pipes table.
        # self.pipes_collection.delete_one({{'_id': pipe_id}})
        return True, "Success"

    def fetch_pipes_keys(
        self,
        connector_keys: list[str] | None = None,
        metric_keys: list[str] | None = None,
        location_keys: list[str] | None = None,
        tags: list[str] | None = None,
        debug: bool = False,
        **kwargs: Any
    ) -> list[tuple[str, str, str]]:
        \"\"\"
        Return a list of tuples for the registered pipes' keys according to the provided filters.

        Parameters
        ----------
        connector_keys: list[str] | None, default None
            The keys passed via `-c`.

        metric_keys: list[str] | None, default None
            The keys passed via `-m`.

        location_keys: list[str] | None, default None
            The keys passed via `-l`.

        tags: List[str] | None, default None
            Tags passed via `--tags` which are stored under `parameters:tags`.

        Returns
        -------
        A list of connector, metric, and location keys in tuples.
        You may return the string "None" for location keys in place of nulls.

        Examples
        --------
        >>> import meerschaum as mrsm
        >>> conn = mrsm.get_connector('example:demo')
        >>> 
        >>> pipe_a = mrsm.Pipe('a', 'demo', tags=['foo'], instance=conn)
        >>> pipe_b = mrsm.Pipe('b', 'demo', tags=['bar'], instance=conn)
        >>> pipe_a.register()
        >>> pipe_b.register()
        >>> 
        >>> conn.fetch_pipes_keys(['a', 'b'])
        [('a', 'demo', 'None'), ('b', 'demo', 'None')]
        >>> conn.fetch_pipes_keys(metric_keys=['demo'])
        [('a', 'demo', 'None'), ('b', 'demo', 'None')]
        >>> conn.fetch_pipes_keys(tags=['foo'])
        [('a', 'demo', 'None')]
        >>> conn.fetch_pipes_keys(location_keys=[None])
        [('a', 'demo', 'None'), ('b', 'demo', 'None')]
        
        \"\"\"
        from meerschaum.utils.misc import separate_negation_values

        in_ck, nin_ck = separate_negation_values([str(val) for val in (connector_keys or [])])
        in_mk, nin_mk = separate_negation_values([str(val) for val in (metric_keys or [])])
        in_lk, nin_lk = separate_negation_values([str(val) for val in (location_keys or [])])
        in_tags, nin_tags = separate_negation_values([str(val) for val in (tags or [])])

        ### TODO build a query like so, only including clauses if the given list is not empty.
        ### The `tags` clause is an OR ("?|"), meaning any of the tags may match.
        ### 
        ### 
        ### SELECT connector_keys, metric_key, location_key
        ### FROM pipes
        ### WHERE connector_keys IN ({{in_ck}})
        ###   AND connector_keys NOT IN ({{nin_ck}})
        ###   AND metric_key IN ({{in_mk}})
        ###   AND metric_key NOT IN ({{nin_mk}})
        ###   AND location_key IN ({{in_lk}})
        ###   AND location_key NOT IN ({{nin_lk}})
        ###   AND (parameters->'tags')::JSONB ?| ARRAY[{{tags}}]
        ###   AND NOT (parameters->'tags')::JSONB ?| ARRAY[{{nin_tags}}]
        return []

    def pipe_exists(
        self,
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kwargs: Any
    ) -> bool:
        \"\"\"
        Check whether a pipe's target table exists.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe to check whether its table exists.

        Returns
        -------
        A `bool` indicating the table exists.
        \"\"\"
        table_name = pipe.target
        ### TODO write a query to determine the existence of `table_name`.
        table_exists = False
        return table_exists

    def drop_pipe(
        self,
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kwargs: Any
    ) -> mrsm.SuccessTuple:
        \"\"\"
        Drop a pipe's collection if it exists.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe to be dropped.

        Returns
        -------
        A `SuccessTuple` indicating success.
        \"\"\"
        ### TODO write a query to drop `table_name`.
        table_name = pipe.target
        return True, \"Success\"

    def sync_pipe(
        self,
        pipe: mrsm.Pipe,
        df: 'pd.DataFrame',
        debug: bool = False,
        **kwargs: Any
    ) -> mrsm.SuccessTuple:
        \"\"\"
        Upsert new documents into the pipe's target table.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe to which the data should be upserted.

        df: pd.DataFrame
            The data to be synced.

        Returns
        -------
        A `SuccessTuple` indicating success.
        \"\"\"
        ### TODO Write the upsert logic for the target table.
        ### `pipe.filter_existing()` is provided for your convenience to
        ### remove duplicates and separate inserts from updates.

        unseen_df, update_df, delta_df = pipe.filter_existing(df, debug=debug)
        return True, \"Success\"

    def clear_pipe(
        self,
        pipe: mrsm.Pipe,
        begin: datetime | int | None = None,
        end: datetime | int | None = None,
        params: dict[str, Any] | None = None,
        debug: bool = False,
    ) -> mrsm.SuccessTuple:
        \"\"\"
        Delete rows within `begin`, `end`, and `params`.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe whose rows to clear.

        begin: datetime | int | None, default None
            If provided, remove rows >= `begin`.

        end: datetime | int | None, default None
            If provided, remove rows < `end`.

        params: dict[str, Any] | None, default None
            If provided, only remove rows which match the `params` filter.

        Returns
        -------
        A `SuccessTuple` indicating success.
        \"\"\"
        ### TODO Write a query to remove rows which match `begin`, `end`, and `params`.
        return True, \"Success\"

    def get_pipe_data(
        self,
        pipe: mrsm.Pipe,
        select_columns: list[str] | None = None,
        omit_columns: list[str] | None = None,
        begin: datetime | int | None = None,
        end: datetime | int | None = None,
        params: dict[str, Any] | None = None,
        debug: bool = False,
        **kwargs: Any
    ) -> Union['pd.DataFrame', None]:
        \"\"\"
        Query a pipe's target table and return the DataFrame.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe with the target table from which to read.

        select_columns: list[str] | None, default None
            If provided, only select these given columns.
            Otherwise select all available columns (i.e. `SELECT *`).

        omit_columns: list[str] | None, default None
            If provided, remove these columns from the selection.

        begin: datetime | int | None, default None
            The earliest `datetime` value to search from (inclusive).

        end: datetime | int | None, default None
            The lastest `datetime` value to search from (exclusive).

        params: dict[str | str] | None, default None
            Additional filters to apply to the query.

        Returns
        -------
        The target table's data as a DataFrame.
        \"\"\"
        if not pipe.exists(debug=debug):
            return None

        table_name = pipe.target
        dt_col = pipe.columns.get(\"datetime\", None)

        ### TODO Write a query to fetch from `table_name`
        ###      and apply the filters `begin`, `end`, and `params`.
        ### 
        ###      To improve performance, add logic to only read from
        ###      `select_columns` and not `omit_columns` (if provided).
        ### 
        ### SELECT {{', '.join(cols_to_select)}}
        ### FROM \"{{table_name}}\"
        ### WHERE \"{{dt_col}}\" >= '{{begin}}'
        ###   AND \"{{dt_col}}\" <  '{{end}}'

        ### The function `parse_df_datetimes()` is a convenience function
        ### to cast a list of dictionaries into a DataFrame and convert datetime columns.
        from meerschaum.utils.dataframe import parse_df_datetimes
        rows = []
        return parse_df_datetimes(rows)

    def get_sync_time(
        self,
        pipe: mrsm.Pipe,
        params: dict[str, Any] | None = None,
        newest: bool = True,
        debug: bool = False,
        **kwargs: Any
    ) -> datetime | int | None:
        \"\"\"
        Return the most recent value for the `datetime` axis.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe whose collection contains documents.

        params: dict[str, Any] | None, default None
            Filter certain parameters when determining the sync time.

        newest: bool, default True
            If `True`, return the maximum value for the column.

        Returns
        -------
        The largest `datetime` or `int` value of the `datetime` axis. 
        \"\"\"
        dt_col = pipe.columns.get('dt_col', None)
        if dt_col is None:
            return None

        ### TODO write a query to get the largest value for `dt_col`.
        ### If `newest` is `False`, return the smallest value.
        ### Apply the `params` filter in case of multiplexing.

    def get_pipe_columns_types(
        self,
        pipe: mrsm.Pipe,
        debug: bool = False,
        **kwargs: Any
    ) -> dict[str, str]:
        \"\"\"
        Return the data types for the columns in the target table for data type enforcement.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe whose target table contains columns and data types.

        Returns
        -------
        A dictionary mapping columns to data types.
        \"\"\"
        table_name = pipe.target
        ### TODO write a query to fetch the columns contained in `table_name`.
        columns_types = {{}}

        ### Return a dictionary mapping the columns
        ### to their Pandas dtypes, e.g.:
        ### `{{'foo': 'int64'`}}`
        ### or to SQL-style dtypes, e.g.:
        ### `{{'bar': 'INT'}}`
        return columns_types

    def get_pipe_rowcount(
        self,
        pipe: mrsm.Pipe,
        begin: datetime | int | None = None,
        end: datetime | int | None = None,
        params: dict[str, Any] | None = None,
        remote: bool = False,
        debug: bool = False,
        **kwargs: Any
    ) -> int:
        \"\"\"
        Return the rowcount for the pipe's table.

        Parameters
        ----------
        pipe: mrsm.Pipe
            The pipe whose table should be counted.

        begin: datetime | int | None, default None
            If provided, only count rows >= `begin`.

        end: datetime | int | None, default None
            If provided, only count rows < `end`.

        params: dict[str, Any] | None
            If provided, only count rows othat match the `params` filter.

        remote: bool, default False
            If `True`, return the rowcount for the pipe's fetch definition.
            In this case, `self` refers to `Pipe.connector`, not `Pipe.instance_connector`.

        Returns
        -------
        The rowcount for this pipe's table according the given parameters.
        \"\"\"
        ### TODO write a query to count how many rows exist in `table_name` according to the filters.
        table_name = pipe.target
        count = 0
        return count
"""
    ),
    'action': (
        "@make_action\n"
        "def {action_name}(**kwargs) -> mrsm.SuccessTuple:\n"
        "    \"\"\"Run `mrsm {action_spaces}` to trigger.\"\"\"\n"
        "    return True, \"Success\"\n\n\n"
    ),
    'api': (
        "@api_plugin\n"
        "def init_app(fastapi_app):\n"
        "    \"\"\"Add new endpoints to the FastAPI app.\"\"\"\n\n"
        "    import fastapi\n"
        "    from meerschaum.api import manager\n\n"
        "    @fastapi_app.get('/{plugin_name}')\n"
        "    def get_my_endpoint(curr_user=fastapi.Depends(manager)):\n"
        "        return {{'message': \"Hello from plugin '{plugin_name}'!\"}}\n\n\n"
    ),
    'web': (
        "@dash_plugin\n"
        "def init_dash(dash_app):\n"
        "    \"\"\"Initialize the Plotly Dash application.\"\"\"\n\n"
        "    import dash.html as html\n"
        "    import dash.dcc as dcc\n"
        "    from dash import Input, Output, State, no_update\n"
        "    from dash.exceptions import PreventUpdate\n"
        "    import dash_bootstrap_components as dbc\n\n"
        "    # Create a new page at the path `/dash/{plugin_name}`.\n"
        "    @web_page('{plugin_name}', page_group='{plugin_name}', login_required=False)\n"
        "    def page_layout():\n"
        "        \"\"\"Return the layout objects for this page.\"\"\"\n"
        "        return dbc.Container([\n"
        "            dcc.Location(id='{plugin_name}-location'),\n"
        "            html.Div(id='output-div'),\n"
        "        ])\n\n"
        "    @dash_app.callback(\n"
        "        Output('output-div', 'children'),\n"
        "        Input('{plugin_name}-location', 'pathname'),\n"
        "    )\n"
        "    def render_page_on_url_change(pathname: str):\n"
        "        \"\"\"Reload page contents when the URL path changes.\"\"\"\n"
        "        return html.H1(\"Hello from plugin '{plugin_name}'!\")\n\n\n"
    ),
}


def bootstrap_plugin(
    plugin_name: str,
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Prompt the user for features and create a plugin file.
    """
    from meerschaum.utils.misc import edit_file
    plugins_dir_path = _get_plugins_dir_path()
    clear_screen(debug=debug)
    info(
        "Answer the questions below to pick out features.\n"
        + "    See the Writing Plugins guide for documentation:\n"
        + "    https://meerschaum.io/reference/plugins/writing-plugins/ "
        + "for documentation.\n"
    )

    plugin = mrsm.Plugin(plugin_name)
    if plugin.is_installed():
        uninstall_success, uninstall_msg = _ask_to_uninstall(plugin)
        if not uninstall_success:
            return uninstall_success, uninstall_msg
        clear_screen(debug=debug)

    features: List[str] = choose(
        "Which of the following features would you like to add to your plugin?",
        list(FEATURE_CHOICES.items()),
        default='fetch',
        multiple=True,
        as_indices=True,
        **kwargs
    )

    clear_screen(debug=debug)

    action_name = ''
    if 'action' in features:
        action_name = _get_action_name()
        clear_screen(debug=debug)
    action_spaces = action_name.replace('_', ' ')

    dt_col_name = None
    if 'fetch' in features:
        dt_col_name = _get_quoted_dt_col_name()
        clear_screen(debug=debug)

    plugin_labels = {
        'plugin_name': plugin_name,
        'plugin_name_capitalized': re.split(
            r'[-_]', plugin_name.lower()
        )[0].capitalize(),
        'plugin_name_lower': plugin_name.lower(),
        'action_name': action_name,
        'action_spaces': action_spaces,
        'dt_col_name': dt_col_name,
    }

    body_text = ""
    body_text += FEATURE_LINES['header'].format(**plugin_labels)
    body_text += IMPORTS_LINES['stdlib'].format(**plugin_labels)
    body_text += IMPORTS_LINES['default'].format(**plugin_labels)
    if 'connector' in features and 'instance-connector' not in features:
        body_text += IMPORTS_LINES['connector'].format(**plugin_labels)
    if 'instance-connector' in features:
        body_text += IMPORTS_LINES['instance-connector'].format(**plugin_labels)
    if 'action' in features:
        body_text += IMPORTS_LINES['action'].format(**plugin_labels)
    if 'api' in features and 'web' in features:
        body_text += IMPORTS_LINES['api+web'].format(**plugin_labels)
    elif 'api' in features:
        body_text += IMPORTS_LINES['api'].format(**plugin_labels)
    elif 'web' in features:
        body_text += IMPORTS_LINES['web'].format(**plugin_labels)

    body_text += "\n"
    body_text += FEATURE_LINES['default'].format(**plugin_labels)
    body_text += FEATURE_LINES['setup'].format(**plugin_labels)

    if 'fetch' in features:
        body_text += FEATURE_LINES['register'].format(**plugin_labels)
        body_text += FEATURE_LINES['fetch'].format(**plugin_labels)

    if 'connector' in features and 'instance-connector' not in features:
        body_text += FEATURE_LINES['connector'].format(**plugin_labels)

    if 'instance-connector' in features:
        body_text += FEATURE_LINES['instance-connector'].format(**plugin_labels)

    if 'action' in features:
        body_text += FEATURE_LINES['action'].format(**plugin_labels)

    if 'api' in features:
        body_text += FEATURE_LINES['api'].format(**plugin_labels)

    if 'web' in features:
        body_text += FEATURE_LINES['web'].format(**plugin_labels)

    try:
        plugin_path = plugins_dir_path / (plugin_name + '.py')
        with open(plugin_path, 'w+', encoding='utf-8') as f:
            f.write(body_text.rstrip())
    except Exception as e:
        error_msg = f"Failed to write file '{plugin_path}':\n{e}"
        return False, error_msg

    clear_screen(debug=debug)
    mrsm.pprint((True, f"Successfully created file '{plugin_path}'."))
    try:
        _ = prompt(
            f"Press [Enter] to edit plugin '{plugin_name}',"
            + " [CTRL+C] to skip.",
            icon=False,
        )
    except (KeyboardInterrupt, Exception):
        return True, "Success"

    edit_file(plugin_path, debug=debug)
    return True, "Success"


def _get_plugins_dir_path() -> pathlib.Path:
    from meerschaum.config.paths import PLUGINS_DIR_PATHS

    if not PLUGINS_DIR_PATHS:
        raise EnvironmentError("No plugin dir path could be found.")

    if len(PLUGINS_DIR_PATHS) == 1:
        return PLUGINS_DIR_PATHS[0]

    return pathlib.Path(
        choose(
            "In which directory do you want to write your plugin?",
            [path.as_posix() for path in PLUGINS_DIR_PATHS],
            numeric=True,
            multiple=False,
            default=PLUGINS_DIR_PATHS[0].as_posix(),
        )
    )
 

def _ask_to_uninstall(plugin: mrsm.Plugin, **kwargs: Any) -> SuccessTuple:
    from meerschaum._internal.entry import entry
    warn(f"Plugin '{plugin}' is already installed!", stack=False)
    uninstall_plugin = yes_no(
        f"Do you want to first uninstall '{plugin}'?",
        default='n',
        **kwargs
    )
    if not uninstall_plugin:
        return False, f"Plugin '{plugin}' already exists."

    return entry(['uninstall', 'plugin', plugin.name, '-f'])


def _get_action_name() -> str:
    while True:
        try:
            action_name = prompt(
                "What is name of your action?\n    "
                + "(separate subactions with spaces, e.g. `sing song`):"
            ).replace(' ', '_')
        except KeyboardInterrupt:
            return False, "Aborted plugin creation."

        if action_name:
            break
        warn("Please enter an action.", stack=False)
    return action_name


def _get_quoted_dt_col_name() -> str:
    try:
        dt_col_name = prompt(
            "Enter the datetime column name ([CTRL+C] to skip):"
        )
    except (Exception, KeyboardInterrupt):
        dt_col_name = None

    if dt_col_name is None:
        dt_col_name = 'None'
    elif '"' in dt_col_name or "'" in dt_col_name:
        dt_col_name = f"\"\"\"{dt_col_name}\"\"\""
    else:
        dt_col_name = f"\"{dt_col_name}\""

    return dt_col_name
