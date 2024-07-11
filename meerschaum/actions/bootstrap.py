#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for bootstrapping elements
(pipes, configuration, etc)
"""

from __future__ import annotations
from meerschaum.utils.typing import Union, Any, Sequence, SuccessTuple, Optional, Tuple, List

def bootstrap(
        action: Optional[List[str]] = None,
        **kw: Any
    ) -> SuccessTuple:
    """
    Launch an interactive wizard to bootstrap pipes or connectors.
    
    Example:
        `bootstrap pipes`

    """
    from meerschaum.actions import choose_subaction
    options = {
        'pipes'      : _bootstrap_pipes,
        'connectors' : _bootstrap_connectors,
        'plugins'    : _bootstrap_plugins,
    }
    return choose_subaction(action, options, **kw)


def _bootstrap_pipes(
        action: Optional[List[str]] = None,
        connector_keys: Optional[List[str]] = None,
        metric_keys: Optional[List[str]] = None,
        location_keys: Optional[List[Optional[str]]] = None,
        yes: bool = False,
        force: bool = False,
        noask: bool = False,
        debug: bool = False,
        mrsm_instance: Optional[str] = None,
        shell: bool = False,
        **kw: Any
    ) -> SuccessTuple:
    """
    Create a new pipe.
    If no keys are provided, guide the user through the steps required.

    """
    from meerschaum import get_pipes
    from meerschaum.config import get_config
    from meerschaum.utils.warnings import info, warn, error
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.prompt import yes_no, prompt, choose
    from meerschaum.connectors.parse import is_valid_connector_keys, parse_instance_keys
    from meerschaum.utils.misc import get_connector_labels
    from meerschaum.utils.formatting._shell import clear_screen

    instance_connector = parse_instance_keys(mrsm_instance)

    if connector_keys is None:
        connector_keys = []
    if metric_keys is None:
        metric_keys = []
    if location_keys is None:
        location_keys = []

    _clear = get_config('shell', 'clear_screen', patch=True)
    abort_tuple = (False, "No pipes were bootstrapped.")

    if (
        len(connector_keys) > 0 and
        len(metric_keys) > 0
    ):
        info(
            "You've provided the following keys:\n" +
            "  - Connector Keys: " + str(connector_keys) + "\n" +
            "  - Metric Keys: " + str(metric_keys) + "\n" +
            (
                ("  - Location Keys: " + str(location_keys) + "\n")
                if len(location_keys) > 0 else ""
            )
        )
        if not force:
            try:
                if not yes_no(
                    ("Would you like to bootstrap pipes with these keys?\n"
                    + "Existing pipes will be deleted!"),
                    default = 'n',
                    yes = yes,
                    noask = noask
                ):
                    return abort_tuple
            except KeyboardInterrupt:
                return abort_tuple
    else:
        ### Get the connector.
        new_label = 'New'
        info(
            f"To create a pipe without explicitly using a connector, "
            + "use the `register pipes` command.\n"
        )
        try:
            ck = choose(
                f"Where are the data coming from?\n\n" +
                f"    Please type the keys of a connector from below,\n" +
                f"    or enter '{new_label}' to register a new connector.\n\n" +
                f" {get_config('formatting', 'emoji', 'connector')} Connector:\n",
                get_connector_labels() + [new_label],
                numeric = False
            )
        except KeyboardInterrupt:
            return abort_tuple

        if ck == new_label:
            if _clear:
                clear_screen(debug=debug)
            while True:
                tup = _bootstrap_connectors(
                    [], yes=yes, force=force, debug=debug, return_keys=True, **kw
                )
                if isinstance(tup[0], str):
                    if _clear:
                        clear_screen(debug=debug)
                    ck = tup[0] + ':' + tup[1]
                    break
                elif isinstance(tup[0], bool) and not tup[0]:
                    return abort_tuple
                warn(f"Please register a new connector or press CTRL+C to cancel.", stack=False)
        connector_keys = [ck]

        ### Get the metric.
        while True:
            if _clear:
                clear_screen(debug=debug)
            try:
                mk = prompt(
                    f"What kind of data is this?\n\n" +
                    f"    The metric is the label for the contents of the pipe.\n" +
                    f"    For example, 'weather' might be a metric for weather station data.\n\n" +
                    f" {get_config('formatting', 'emoji', 'metric')} Metric:"
                )
            except KeyboardInterrupt:
                return abort_tuple
            if mk:
                break
            warn("Please enter a metric.", stack=False)
        metric_keys = [mk]

        ### Get the location
        if _clear:
            clear_screen(debug=debug)
        try:
            lk = prompt(
                "Where are the data located?\n\n"
                + "    You can build pipes that share a connector and metric\n"
                + "    but are in different locations.\n\n"
                + f"    For example, you could create Pipe('{ck}', '{mk}', 'home') and\n"
                + f"    Pipe('{ck}', '{mk}', 'work'), which would share a connector\n"
                + "    and metric, but may come from different tables.\n\n"
                + "    In most cases, you can omit the location.\n\n"
                + f" {get_config('formatting', 'emoji', 'location')} Location (Empty to omit):"
            )
        except KeyboardInterrupt:
            return abort_tuple
        lk = None if lk == '' else lk
        location_keys = [lk]

    if _clear:
        clear_screen(debug=debug)
    _pipes = get_pipes(
        connector_keys, metric_keys, location_keys,
        method = 'explicit',
        as_list = True,
        debug = debug,
        mrsm_instance = instance_connector,
        **kw
    )
    pipes = []
    for p in _pipes:
        if p.get_id(debug=debug) is not None and not force:
            try:
                if not yes_no(
                    f"{p} already exists.\n\n    Delete {p}?\n    Data will be lost!",
                    default='n'
                ):
                    info(f"Skipping bootstrapping {p}...")
                    continue
            except KeyboardInterrupt:
                return abort_tuple
        pipes.append(p)

    if len(pipes) == 0:
        return abort_tuple

    success_dict = {}
    successes, failures = 0, 0
    for p in pipes:
        try:
            tup = p.bootstrap(
                interactive=True, force=force, noask=noask, yes=yes, shell=shell, debug=debug
            )
        except Exception as e:
            import traceback
            traceback.print_exc()
            tup = False, f"Failed to bootstrap {p} with exception:\n" + str(e)
        success_dict[p] = tup
        if tup[0]:
            successes += 1
        else:
            failures += 1

    msg = (
        f"Finished bootstrapping {len(pipes)} pipe" + ("s" if len(pipes) != 1 else "") + "\n" +
        f"    ({successes} succeeded, {failures} failed)."
    )

    return (successes > 0), msg

def _bootstrap_connectors(
        action : Optional[List[str]] = None,
        connector_keys : Optional[List[str]] = None,
        yes : bool = False,
        force : bool = False,
        noask : bool = False,
        debug : bool = False,
        return_keys : bool = False,
        **kw : Any
    ) -> Union[SuccessTuple, Tuple[str, str]]:
    """
    Prompt the user for the details necessary to create a Connector.

    """
    from meerschaum.connectors.parse import is_valid_connector_keys
    from meerschaum.connectors import connectors, get_connector, types, custom_types
    from meerschaum.utils.prompt import prompt, yes_no, choose
    from meerschaum.config import get_config
    from meerschaum.config._edit import write_config
    from meerschaum.utils.formatting import pprint
    from meerschaum.utils.formatting._shell import clear_screen
    from meerschaum.connectors import attributes as connector_attributes
    from meerschaum.utils.warnings import warn, info
    from meerschaum.utils.misc import is_int

    abort_tuple = False, "No connectors bootstrapped."
    _clear = get_config('shell', 'clear_screen', patch=True)

    if action is None:
        action = []
    if connector_keys is None:
        connector_keys = []

    if len(connector_keys) == 0:
        pass

    try:
        _type = choose(
            (
                'Please choose a connector type.\n'
                + 'For more information on connectors, '
                + 'please visit https://meerschaum.io/reference/connectors'
            ),
            sorted(list(connectors)),
            default = 'sql'
        )
    except KeyboardInterrupt:
        return abort_tuple

    if _clear:
        clear_screen(debug=debug)

    existing_labels = get_config('meerschaum', 'connectors', _type, warn=False) or []
    _label_choices = sorted([
        label
        for label in existing_labels
        if label != 'default' and label is not None
    ])
    new_connector_label = 'New connector'
    _label_choices.append(new_connector_label)
    while True:
        try:
            _label = prompt(f"New label for '{_type}' connector:")
        except KeyboardInterrupt:
            return abort_tuple
        if _label in existing_labels:
            warn(f"Connector '{_type}:{_label}' already exists.", stack=False)
            overwrite = yes_no(
                f"Do you want to overwrite connector '{_type}:{_label}'?",
                default = 'n',
                yes = yes,
                noask = noask,
            )
            if not overwrite and not force:
                return False, f"No changes made to connector configuration."
                break
        elif _label == "":
            warn(f"Please enter a label.", stack=False)
        else:
            break

    cls = types.get(_type)
    cls_required_attrs = getattr(cls, 'REQUIRED_ATTRIBUTES', [])
    type_attributes = connector_attributes.get(_type, {'required': cls_required_attrs})

    new_attributes = {}
    if 'flavors' in type_attributes:
        try:
            flavor = choose(
                f"Flavor for connector '{_type}:{_label}':",
                sorted(list(type_attributes['flavors'])),
                default = (
                    'timescaledb'
                    if 'timescaledb' in type_attributes['flavors']
                    else None
                )
            )
        except KeyboardInterrupt:
            return abort_tuple
        new_attributes['flavor'] = flavor
        required = sorted(list(connector_attributes[_type]['flavors'][flavor]['requirements']))
        default = type_attributes['flavors'][flavor].get('defaults', {})
    else:
        required = sorted(list(type_attributes.get('required', {})))
        default = type_attributes.get('default', {})
    info(
        f"Please answer the following questions to configure the new connector '{_type}:{_label}'."
        + '\n' + "Press [Ctrl + C] to skip."
    )
    for r in required:
        try:
            val = prompt(f"Value for {r}:")
        except KeyboardInterrupt:
            continue
        if is_int(val):
            val = int(val)
        new_attributes[r] = val

    for k, v in default.items():
        ### skip already configured attributes, (e.g. flavor or from required)
        if k in new_attributes:
            continue
        try:
            val = prompt(f"Value for {k}:", default=str(v))
        except KeyboardInterrupt:
            continue
        if is_int(val):
            val = int(val)
        new_attributes[k] = val

    if _clear:
        clear_screen(debug=debug)
    try:
        conn = get_connector(_type, _label, **new_attributes)
    except Exception as e:
        return False, f"Failed to bootstrap connector '{_type}:{_label}' with exception:\n{e}"

    pprint(new_attributes)
    try:
        ok = (
            yes_no(
                f"Are you ok with these new attributes for connector '{conn}'?",
                default = 'y',
                noask = noask,
                yes = yes
            ) if not yes else yes
        )
    except KeyboardInterrupt:
        ok = False
    if not ok:
        return False, "No changes made to connectors configuration."

    meerschaum_config = get_config('meerschaum')

    if 'connectors' not in meerschaum_config:
        meerschaum_config['connectors'] = {}
    if _type not in meerschaum_config['connectors']:
        meerschaum_config['connectors'][_type] = {}
    meerschaum_config['connectors'][_type][_label] = new_attributes
    write_config({'meerschaum': meerschaum_config}, debug=debug)
    if return_keys:
        return _type, _label
    return True, "Success"


def _bootstrap_plugins(
        action: Optional[List[str]] = None,
        debug: bool = False,
        **kwargs: Any
    ) -> SuccessTuple:
    """
    Launch an interactive wizard to guide the user to creating a new plugin.
    """
    import pathlib
    import meerschaum as mrsm
    from meerschaum.utils.warnings import info, warn
    from meerschaum.utils.prompt import prompt, choose, yes_no
    from meerschaum.utils.formatting._shell import clear_screen
    from meerschaum.utils.misc import edit_file
    from meerschaum.config.paths import PLUGINS_DIR_PATHS
    from meerschaum._internal.entry import entry

    if not action:
        action = [prompt("Enter the name of your new plugin:")]

    if len(PLUGINS_DIR_PATHS) > 1:
        plugins_dir_path = pathlib.Path(
            choose(
                "In which directory do you want to write your plugin?",
                [path.as_posix() for path in PLUGINS_DIR_PATHS],
                numeric = True,
                multiple = False,
                default = PLUGINS_DIR_PATHS[0].as_posix(),
            )
        )
    else:
        plugins_dir_path = PLUGINS_DIR_PATHS[0]
        
    clear_screen(debug=debug)
    info(
        "Answer the questions below to pick out features.\n"
        + "    See the Writing Plugins guide for documentation:\n"
        + "    https://meerschaum.io/reference/plugins/writing-plugins/ for documentation.\n"
    )

    imports_lines = {
        'default': (
            "import meerschaum as mrsm\n"
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

    ### TODO: Add feature for custom connectors.
    feature_lines = {
        'header': (
            "# {plugin_name}.py\n\n"
        ),
        'default': (
            "__version__ = '0.0.1'\n"
            "\n# Add any depedencies to `required` (similar to `requirements.txt`).\n"
            "required = []\n\n"
        ),
        'setup': (
            "def setup(**kwargs) -> mrsm.SuccessTuple:\n"
            "    \"\"\"Executed during installation and `mrsm setup plugin {plugin_name}`.\"\"\"\n"
            "    return True, \"Success\"\n\n\n"
        ),
        'register': (
            "def register(pipe: mrsm.Pipe):\n"
            "    \"\"\"Return the default parameters for a new pipe.\"\"\"\n"
            "    return {\n"
            "        'columns': {\n"
            "            'datetime': None,\n"
            "        }\n"
            "    }\n\n\n"
        ),
        'fetch': (
            "def fetch(pipe: mrsm.Pipe, **kwargs):\n"
            "    \"\"\"Return or yield dataframe-like objects.\"\"\"\n"
            "    docs = []\n"
            "    # populate docs with dictionaries (rows).\n"
            "    return docs\n\n\n"
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
            "    @fastapi_app.get('/my/endpoint')\n"
            "    def get_my_endpoint(curr_user=fastapi.Depends(manager)):\n"
            "        return {'message': 'Hello, World!'}\n\n\n"
        ),
        'web': (
            "@dash_plugin\n"
            "def init_dash(dash_app):\n"
            "    \"\"\"Initialize the Plotly Dash application.\"\"\"\n"
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

    for plugin_name in action:
        plugin_path = plugins_dir_path / (plugin_name + '.py')
        plugin = mrsm.Plugin(plugin_name)
        if plugin.is_installed():
            warn(f"Plugin '{plugin_name}' is already installed!", stack=False)
            uninstall_plugin = yes_no(
                f"Do you want to first uninstall '{plugin}'?",
                default = 'n',
                **kwargs
            )
            if not uninstall_plugin:
                return False, f"Plugin '{plugin_name}' already exists."

            uninstall_success, uninstall_msg = entry(['uninstall', 'plugin', plugin_name, '-f'])
            if not uninstall_success:
                return uninstall_success, uninstall_msg
            clear_screen(debug=debug)

        features = choose(
            "Which of the following features would you like to add to your plugin?",
            [
                (
                    'fetch',
                    'Fetch data\n     (e.g. extracting from a remote API)\n'
                ),
                (
                    'action',
                    'New actions\n     (e.g. `mrsm sing song`)\n'
                ),
                (
                    'api',
                    'New API endpoints\n     (e.g. `POST /my/new/endpoint`)\n',
                ),
                (
                    'web',
                    'New web console page\n     (e.g. `/dash/my-web-app`)\n',
                ),
            ],
            default = 'fetch',
            multiple = True,
            as_indices = True,
            **kwargs
        )

        action_name = ''
        if 'action' in features:
            while True:
                try:
                    action_name = prompt(
                        "What is name of your action?\n    "
                        + "(separate subactions with spaces, e.g. `sing song`):"
                    ).replace(' ', '_')
                except KeyboardInterrupt as e:
                    return False, "Aborted plugin creation."

                if action_name:
                    break
                warn("Please enter an action.", stack=False)

        action_spaces = action_name.replace('_', ' ')

        plugin_labels = {
            'plugin_name': plugin_name,
            'action_name': action_name,
            'action_spaces': action_spaces,
        }

        body_text = ""
        body_text += feature_lines['header'].format(**plugin_labels)
        body_text += imports_lines['default'].format(**plugin_labels)
        if 'action' in features:
            body_text += imports_lines['action']
        if 'api' in features and 'web' in features:
            body_text += imports_lines['api+web']
        elif 'api' in features:
            body_text += imports_lines['api']
        elif 'web' in features:
            body_text += imports_lines['web']

        body_text += "\n"
        body_text += feature_lines['default'].format(**plugin_labels)
        body_text += feature_lines['setup'].format(**plugin_labels)

        if 'fetch' in features:
            body_text += feature_lines['register']
            body_text += feature_lines['fetch']

        if 'action' in features:
            body_text += feature_lines['action'].format(**plugin_labels)

        if 'api' in features:
            body_text += feature_lines['api']

        if 'web' in features:
            body_text += feature_lines['web'].format(**plugin_labels)

        try:
            with open(plugin_path, 'w+', encoding='utf-8') as f:
                f.write(body_text.rstrip())
        except Exception as e:
            error_msg = f"Failed to write file '{plugin_path}':\n{e}"
            return False, error_msg

        mrsm.pprint((True, f"Successfully created file '{plugin_path}'."))
        try:
            _ = prompt(f"Press [Enter] to edit plugin '{plugin_name}', [CTRL+C] to skip.")
        except (KeyboardInterrupt, Exception):
            continue

        edit_file(plugin_path, debug=debug)

    return True, "Success"


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
bootstrap.__doc__ += _choices_docstring('bootstrap')
