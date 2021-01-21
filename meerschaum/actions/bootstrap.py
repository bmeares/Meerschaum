#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Functions for bootstrapping elements
(pipes, configuration, etc)
"""

from __future__ import annotations
from meerschaum.utils.Typing import Union, Any, Sequence, SuccessTuple, Optional

def bootstrap(
        action : Sequence[str] = [''],
        **kw : Any
    ) -> SuccessTuple:
    """
    Bootstrap an element (pipe, configuration).

    Command:
        `bootstrap {option}`

    Example:
        `bootstrap config`
    """
    from meerschaum.utils.misc import choose_subaction
    options = {
        'pipes'   : _bootstrap_pipes,
        'config'  : _bootstrap_config,
        'stack'   : _bootstrap_stack,
        'grafana' : _bootstrap_grafana,
    }
    return choose_subaction(action, options, **kw)

def _bootstrap_pipes(
        action : Sequence[str] = [],
        connector_keys : Sequence[str] = [],
        metric_keys : Sequence[str] = [],
        location_keys : Optional[Sequence[Optional[str]]] = [],
        debug : bool,
        **kw : Any
    ) -> SuccessTuple:
    """
    Create a new pipe.
    If no keys are provided, guide the user through the steps required.
    """
    from meerschaum.utils.config import get_config
    from meerschaum.utils.warnings import info, warn, error
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.prompt import yes_no
    prompt_toolkit = attempt_import('prompt_toolkit')

    if (
        len(connector_keys) > 0 or
        len(metric_keys) > 0 or
        len(location_keys) > 0:
    ):
        info()

    return (True, "Success")

def _bootstrap_config(
        action : Sequence[str] = [''],
        yes : bool = False,
        force : bool = False,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Delete and regenerate the default Meerschaum configuration.
    """
    possible_config_funcs = {
        'stack'   : _bootstrap_stack,
        'grafana' : _bootstrap_grafana,
    }
    if len(action) > 0:
        if action[0] in possible_config_funcs:
            return possible_config_funcs[action[0]](**kw)

    from meerschaum.config._edit import write_default_config, write_config
    from meerschaum.config._default import default_config

    from meerschaum.actions import actions
    if not actions['delete'](['config'], debug=debug, yes=yes, force=force, **kw)[0]:
        return False, "Aborting bootstrap"

    if not write_config(default_config, debug=debug, **kw):
        return (False, "Failed to write default configuration")

    if not write_default_config(debug=debug, **kw):
        return (False, "Failed to write default configuration")

    from meerschaum.config.stack import write_stack
    if not write_stack(debug=debug):
        return False, "Failed to write stack"

    return (True, "Successfully bootstrapped configuration files")

def _bootstrap_stack(
        yes : bool = False,
        force : bool = False,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Delete and regenerate the default Meerschaum stack configuration
    """
    from meerschaum.utils.prompt import yes_no
    from meerschaum.config._paths import STACK_COMPOSE_PATH, STACK_ENV_PATH
    from meerschaum.config.stack import write_stack
    from meerschaum.utils.debug import dprint
    answer = False
    if not yes:
        answer = yes_no(f"Delete {STACK_COMPOSE_PATH}?", default='n')

    if answer or force:
        if not write_stack(debug=debug):
            return False, "Failed to write stack configuration"
    else:
        msg = "No edits made"
        if debug: dprint(msg)
        return True, msg
    return True, "Success"

def _bootstrap_grafana(
        yes : bool = False,
        force : bool = False,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Delete and regenerate the default Meerschaum stack configuration
    """
    from meerschaum.utils.prompt import yes_no
    from meerschaum.config._paths import GRAFANA_DATASOURCE_PATH, GRAFANA_DASHBOARD_PATH
    from meerschaum.config._edit import general_write_config
    from meerschaum.config import config as cf, get_config
    from meerschaum.utils.debug import dprint
    answer = False
    if not yes:
        answer = yes_no(f"Delete {GRAFANA_DATASOURCE_PATH} and {GRAFANA_DASHBOARD_PATH}?", default='n')

    if answer or force:
        general_write_config(
            {
                GRAFANA_DATASOURCE_PATH : get_config('stack', 'grafana', 'datasource', patch=True),
                GRAFANA_DASHBOARD_PATH : get_config('stack', 'grafana', 'dashboard'),
            },
            debug=debug
        )
    else:
        msg = "No edits made"
        if debug: dprint(msg)
        return True, msg
    return True, "Success"

### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
bootstrap.__doc__ += _choices_docstring('bootstrap')