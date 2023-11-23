#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Default actions available to the mrsm CLI.
"""

from __future__ import annotations
from meerschaum.utils.typing import Callable, Any, Optional, Union, List, Dict, SuccessTuple
from meerschaum.utils.packages import get_modules_from_package
_custom_actions = []

def get_subactions(
        action: Union[str, List[str]],
        _actions: Optional[Dict[str, Callable[[Any], Any]]] = None,
    ) -> Dict[str, Callable[[Any], Any]]:
    """
    Return a dictionary of an action's sub-action functions.

    Examples
    --------
    >>> get_subactions('install').keys()
    dict_keys(['packages', 'plugins', 'required'])
    >>> 
    """
    import importlib, inspect
    subactions = {}
    if isinstance(action, str):
        action = [action]
    action_function = get_action(action[0], _actions=_actions)
    if action_function is None:
        return subactions
    try:
        action_module = importlib.import_module(action_function.__module__)
    except ImportError:
        action_module = None
    if action_module is None:
        return subactions
    for name, f in inspect.getmembers(action_module):
        if not inspect.isfunction(f):
            continue
        if action_function.__name__ + '_' in name and not name.lstrip('_').startswith('complete'):
            _name = name.replace(action_function.__name__, '')
            _name = _name.lstrip('_')
            subactions[_name] = f
    return subactions


def get_action(
        action: Union[str, List[str]],
        _actions: Optional[Dict[str, Callable[[Any], Any]]] = None,
    ) -> Union[Callable[[Any], Any], None]:
    """
    Return a function corresponding to the given action list.
    This may be a custom action with an underscore, in which case, allow for underscores.
    This may also be a subactions, which is handled by `get_subactions()`
    """
    if _actions is None:
        _actions = actions
    if isinstance(action, str):
        action = [action]

    if not any(action):
        return None

    ### Simple case, e.g. ['show']
    if len(action) == 1:
        if action[0] in _actions:
            return _actions[action[0]]

        ### e.g. ['foo'] (and no custom action available)
        return None

    ### Last case: it could be a custom action with an underscore in the name.
    action_name_with_underscores = '_'.join(action)
    candidates = []
    for action_key, action_function in _actions.items():
        if not '_' in action_key:
            continue
        if action_name_with_underscores.startswith(action_key):
            leftovers = action_name_with_underscores.replace(action_key, '')
            candidates.append((len(leftovers), action_function))
    if len(candidates) > 0:
        return sorted(candidates)[0][1]

    ### Might be dealing with a subaction.
    if action[0] in _actions:
        subactions = get_subactions([action[0]], _actions=_actions)
        if action[1] not in subactions:
            return _actions[action[0]]
        return subactions[action[1]]

    return None


def get_main_action_name(
        action: Union[List[str], str],
        _actions: Optional[Dict[str, Callable[[Any], SuccessTuple]]] = None,
    ) -> Union[str, None]:
    """
    Given an action list, return the name of the main function.
    For subactions, this will return the root function.
    If no action function can be found, return `None`.

    Parameters
    ----------
    action: Union[List[str], str]
        The standard action list.

    Examples
    --------
    >>> get_main_action_name(['show', 'pipes'])
    'show'
    >>> 
    """
    if _actions is None:
        _actions = actions
    action_function = get_action(action, _actions=_actions)
    if action_function is None:
        return None
    clean_name = action_function.__name__.lstrip('_')
    if clean_name in _actions:
        return clean_name
    words = clean_name.split('_')
    first_word = words[0]
    if first_word in _actions:
        return first_word
    ### We might be dealing with shell `do_` functions.
    second_word = words[1] if len(words) > 1 else None
    if second_word and second_word in _actions:
        return second_word
    return None


def get_completer(
        action: Union[List[str], str],
        _actions: Optional[Dict[str, Callable[[Any], SuccessTuple]]] = None,
    ) -> Union[
        Callable[['meerschaum._internal.shell.Shell', str, str, int, int], List[str]], None
    ]:
    """Search for a custom completer function for an action."""
    import importlib, inspect
    if isinstance(action, str):
        action = [action]
    action_function = get_action(action)
    if action_function is None:
        return None
    try:
        action_module = importlib.import_module(action_function.__module__)
    except ImportError:
        action_module = None
    if action_module is None:
        return None
    candidates = []
    for name, f in inspect.getmembers(action_module):
        if not inspect.isfunction(f):
            continue
        name_lstrip = name.lstrip('_')
        if not name_lstrip.startswith('complete_'):
            continue
        if name_lstrip == 'complete_' + action_function.__name__:
            candidates.append((name_lstrip, f))
            continue
        if name_lstrip == 'complete_' + action_function.__name__.split('_')[0]:
            candidates.append((name_lstrip, f))
    if len(candidates) == 1:
        return candidates[0][1]
    if len(candidates) > 1:
        return sorted(candidates)[-1][1]
    
    return None


def choose_subaction(
        action: Optional[List[str]] = None,
        options: Optional[Dict[str, Any]] = None,
        **kw
    ) -> SuccessTuple:
    """
    Given a dictionary of options and the standard Meerschaum actions list,
    check if choice is valid and execute chosen function, else show available
    options and return False

    Parameters
    ----------
    action: Optional[List[str]], default None
        A list of subactions (e.g. `show pipes` -> ['pipes']).

    options: Optional[Dict[str, Any]], default None
        Available options to execute.
        option (key) -> function (value)
        Functions must accept **kw keyword arguments
        and return a tuple of success bool and message.

    Returns
    -------
    The return value of the chosen subaction (assumed to be a `SuccessTuple`).

    """
    from meerschaum.utils.warnings import warn, info
    import inspect
    if action is None:
        action = []
    if options is None:
        options = {}
    parent_action = inspect.stack()[1][3]
    if len(action) == 0:
        action = ['']
    choice = action[0]

    def valid_choice(_choice : str, _options : dict):
        if _choice in _options:
            return _choice
        if (_choice + 's') in options:
            return _choice + 's'
        return None

    parsed_choice = valid_choice(choice, options)
    if parsed_choice is None:
        warn(f"Cannot {parent_action} '{choice}'. Choose one:", stack=False)
        for option in sorted(options):
            print(f"  - {parent_action} {option}")
        return (False, f"Invalid choice '{choice}'.")
    ### remove parent sub-action
    kw['action'] = list(action)
    del kw['action'][0]
    return options[parsed_choice](**kw)


def _get_subaction_names(action: str, globs: dict = None) -> List[str]:
    """Use `meerschaum.actions.get_subactions()` instead."""
    if globs is None:
        import importlib
        module = importlib.import_module(f'meerschaum.actions.{action}')
        globs = vars(module)
    subactions = []
    for item in globs:
        if f'_{action}' in item and 'complete' not in item.lstrip('_'):
            subactions.append(globs[item])
    return subactions


def choices_docstring(action: str, globs : Optional[Dict[str, Any]] = None) -> str:
    """
    Append the an action's available options to the module docstring.
    This function is to be placed at the bottom of each action module.

    Parameters
    ----------
    action: str
        The name of the action module (e.g. 'install').
        
    globs: Optional[Dict[str, Any]], default None
        An optional dictionary of global variables.

    Returns
    -------
    The generated docstring for the module.

    Examples
    --------
    >>> from meerschaum.utils.misc import choices_docstring as _choices_docstring
    >>> install.__doc__ += _choices_docstring('install')

    """
    options_str = f"\n    Options:\n        `{action} "
    subactions = _get_subaction_names(action, globs=globs)
    options_str += "["
    sa_names = []
    for sa in subactions:
        try:
            sa_names.append(sa.__name__[len(f"_{action}") + 1:])
        except Exception as e:
            print(e)
            return ""
    for sa_name in sorted(sa_names):
        options_str += f"{sa_name}, "
    options_str = options_str[:-2] + "]`"
    return options_str

### build __all__ from other .py files in this package
import sys
modules = get_modules_from_package(
    sys.modules[__name__],
    names = False,
)
__all__ = ['actions', 'get_subactions', 'get_action', 'get_main_action_name', 'get_completer']

### Build the actions dictionary by importing all
### functions that do not begin with '_' from all submodules.
from inspect import getmembers, isfunction
actions = {}

for module in modules:
    ### A couple important things happening here:
    ### 1. Find all functions in all modules in `actions` package
    ###     (skip functions that begin with '_')
    ### 2. Add them as members to the Shell class
    ###     - Original definition : meerschaum._internal.shell.Shell
    ###     - New definition      : meerschaum._internal.Shell
    ### 3. Populate the actions dictionary with function names and functions
    ###
    ### UPDATE:
    ### Shell modifications have been deferred to get_shell in order to improve lazy loading.

    actions.update(
        dict(
            [
                ### __name__ and new function pointer
                (ob[0], ob[1])
                    for ob in getmembers(module)
                        if isfunction(ob[1])
                            ### check that the function belongs to the module
                            and ob[0] == module.__name__.replace('_', '').split('.')[-1]
                            ### skip functions that start with '_'
                            and ob[0][0] != '_'
            ]
        )
    )

original_actions = actions.copy()
from meerschaum._internal.entry import entry, get_shell
import meerschaum.plugins
make_action = meerschaum.plugins.make_action
pre_sync_hook = meerschaum.plugins.pre_sync_hook
post_sync_hook = meerschaum.plugins.post_sync_hook

### Instruct pdoc to skip the `meerschaum.actions.plugins` subdirectory.
__pdoc__ = {
    'plugins': False,
    'arguments': True,
    'shell': False,
    'actions': (
        "Access functions of the standard Meerschaum actions.\n\n"
        + "Visit the [actions reference page](https://meerschaum.io/reference/actions/) "
        + "for documentation about each action.\n\n"
        + """\n\nExamples
--------
>>> actions['show'](['pipes'])

"""
        + "Keys\n----\n"
        + ', '.join(sorted([f'`{a}`' for a in actions]))
    )
}
for a in actions:
    __pdoc__[a] = False
meerschaum.plugins.load_plugins()
