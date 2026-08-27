#! /usr/bin/env python3

"""Entrypoint for subactions of the built-in ``compose`` command."""

import copy
from functools import partial
import importlib
import os
import pathlib
import sys


def get_subactions():
    """Return the available Compose subaction names."""
    return [
        path.stem
        for path in pathlib.Path(__file__).parent.glob('*.py')
        if not path.name.startswith('_')
    ]


def _get_subaction_function(subaction):
    """Import and return a Compose subaction function."""
    module = importlib.import_module(f'meerschaum.compose.subactions.{subaction}')
    return getattr(module, f'_compose_{subaction}')


def _do_subaction(subaction: str, debug: bool = False, **kwargs):
    """Run a Compose subaction inside the project's isolated environment."""
    from meerschaum.config import replace_config
    from meerschaum.config._default import default_config
    from meerschaum.config.environment import replace_env
    from meerschaum.plugins import unload_plugins, load_plugins, get_plugins_names
    from meerschaum.utils.warnings import dprint
    from meerschaum.compose.utils.config import get_env_dict
    from meerschaum.compose.utils import init

    subactions = get_subactions()
    selected_subaction = subaction if subaction in subactions else 'default'
    subaction_function = _get_subaction_function(selected_subaction)
    if subaction == 'init':
        with replace_env({}):
            return subaction_function({}, debug=debug, **kwargs)

    ### `init()` loads `.env` values for config substitution. Snapshot that setup so
    ### project variables are available below without leaking into the host process.
    with replace_env({}):
        compose_config = init(debug=debug, **kwargs)
        config = copy.deepcopy(compose_config.get('config', default_config))
        env = get_env_dict(compose_config)
    need_unload = 'MRSM__COMPOSE_CONFIG' not in os.environ
    old_plugins_names = get_plugins_names()

    new_plugin_names = []
    try:
        if need_unload:
            if debug:
                dprint("Compose: Unloading plugins before replacing config.", icon=False)
            if old_plugins_names:
                unload_plugins(old_plugins_names, debug=debug)
            _ = sys.modules.pop('plugins', None)

        if debug:
            dprint("Compose: Replacing config.", icon=False)

        with replace_config(config):
            with replace_env(env):
                try:
                    new_plugin_names = get_plugins_names()
                    if debug:
                        dprint(f"Compose: Loading plugins: {new_plugin_names}", icon=False)
                    load_plugins(debug=debug)

                    if debug:
                        name = subaction_function.__name__.lstrip('_').replace('_', ' ')
                        dprint(f"Compose: Calling `{name}`...", icon=False)

                    success, msg = subaction_function(compose_config, debug=debug, **kwargs)
                finally:
                    if need_unload and new_plugin_names:
                        if debug:
                            dprint("Compose: Unloading project plugins.", icon=False)
                        unload_plugins(new_plugin_names, debug=debug)
    finally:
        if need_unload:
            if debug:
                dprint("Compose: Loading back existing plugins.")
            _ = sys.modules.pop('plugins', None)
            if old_plugins_names:
                load_plugins(debug=debug)
    return success, msg


### Preserve the legacy plugin's programmatic subaction entrypoints. These wrappers
### intentionally enter the complete Compose isolation boundary.
globals().update({
    f'_compose_{subaction}': partial(_do_subaction, subaction)
    for subaction in get_subactions()
})
