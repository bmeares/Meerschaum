#! /usr/bin/env python3

"""
Entrypoint for subactions of the built-in ``compose`` command.

The subactions run in the native dispatcher (`meerschaum-compose`); the functions here are
delegations that keep the names of the legacy plugin's programmatic entry points.
"""

from functools import partial
import importlib

SUBACTIONS = ('default', 'down', 'explain', 'init', 'logs', 'ps', 'run', 'up')


def get_subactions():
    """Return the available Compose subaction names."""
    return list(SUBACTIONS)


def _get_subaction_function(subaction):
    """Import and return a Compose subaction function."""
    module = importlib.import_module(f'meerschaum.compose.subactions.{subaction}')
    return getattr(module, f'_compose_{subaction}')


def _do_subaction(subaction: str, debug: bool = False, **kwargs):
    """
    Run a Compose subaction natively (`mrsm compose <subaction> ...`). Any other word is the
    passthrough: the words after `compose` run in the project's environment.
    """
    from meerschaum import _core
    from meerschaum._internal.entry import adhoc_context

    action = list(kwargs.pop('action', None) or [])
    if subaction != 'default' and (not action or action[0] != subaction):
        action = [subaction, *action]
    kwargs['debug'] = debug
    return _core.run_action(['compose', *action], kwargs, adhoc_context())


def _delegate(subaction: str, compose_config, debug: bool = False, **kwargs):
    """The body of every `_compose_<subaction>(compose_config, ...)`: the file selects the project."""
    if kwargs.get('file', None) is None and (compose_config or {}).get('__file__', None):
        kwargs['file'] = compose_config['__file__']
    return _do_subaction(subaction, debug=debug, **kwargs)


### Preserve the legacy plugin's programmatic subaction entrypoints.
globals().update({
    f'_compose_{subaction}': partial(_do_subaction, subaction)
    for subaction in get_subactions()
})
