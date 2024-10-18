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
    'connector': 'Custom connector\n     (e.g. manage credentials)\n',
    'action'   : 'New actions\n     (e.g. `mrsm sing song`)\n',
    'api'      : 'New API endpoints\n     (e.g. `POST /my/new/endpoint`)\n',
    'web'      : 'New web console page\n     (e.g. `/dash/my-web-app`)\n',
}

IMPORTS_LINES: Dict[str, str] = {
    'stdlib': (
        "from datetime import datetime, timedelta, timezone\n"
    ),
    'default': (
        "import meerschaum as mrsm\n"
        "from meerschaum.config import get_plugin_config, write_plugin_config\n"
    ),
    'connector': (
        "from meerschaum.connectors import Connector, make_connector\n"
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
        "    import dash_bootstrap_components as dbc\n\n"
        "    # Create a new page at the path `/dash/{plugin_name}`.\n"
        "    @web_page('{plugin_name}', login_required=False)\n"
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
    if 'connector' in features:
        body_text += IMPORTS_LINES['connector'].format(**plugin_labels)
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

    if 'connector' in features:
        body_text += FEATURE_LINES['connector'].format(**plugin_labels)

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
