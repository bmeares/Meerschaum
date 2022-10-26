#! /usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This module contains functions for parsing arguments
"""

from __future__ import annotations
from meerschaum.utils.typing import List, Dict, Any, Optional, Callable, SuccessTuple
from meerschaum.utils.threading import RLock

_locks = {
    '_loaded_plugins_args': RLock(),
}
_loaded_plugins_args: bool = False


def parse_arguments(sysargs: List[str]) -> Dict[str, Any]:
    """
    Parse a list of arguments into standard Meerschaum arguments.
    Returns a dictionary of argument_name -> argument_value.

    Parameters
    ----------
    sysargs: List[str]
        List of command-line arguments to process. Does not include the executable.
        E.g. ['show', 'version', '--nopretty']

    Returns
    -------
    A dictionary of keyword arguments.

    """
    import shlex
    import copy
    from meerschaum.config.static import STATIC_CONFIG
    from meerschaum._internal.arguments._parser import parser

    global _loaded_plugins_args
    with _locks['_loaded_plugins_args']:
        if not _loaded_plugins_args:
            load_plugin_args()
            _loaded_plugins_args = True

    sub_arguments = []
    sub_arg_indices = []
    begin_decorator, end_decorator = STATIC_CONFIG['system']['arguments']['sub_decorators']
    found_begin_decorator = False
    for i, word in enumerate(sysargs):
        is_sub_arg = False
        if not found_begin_decorator:
            found_begin_decorator = word.startswith(begin_decorator)
            found_end_decorator = word.endswith(end_decorator)

        if found_begin_decorator:
            ### check if sub arg is ever closed
            for a in sysargs[i:]:
                if a.endswith(end_decorator):
                    is_sub_arg = True
                    found_begin_decorator = False
        elif found_end_decorator:
            for a in sysargs[:i]:
                if a.startswith(begin_decorator):
                    is_sub_arg = True
                    found_begin_decorator = False
        if is_sub_arg:
            ### remove decorators
            sa = word
            if sa.startswith(begin_decorator):
                sa = sa[len(begin_decorator):]
            if sa.endswith(end_decorator):
                sa = sa[:-1 * len(end_decorator)]
            sub_arguments.append(sa)
            ### remove sub-argument from action list
            sub_arg_indices.append(i)

    ### rebuild sysargs without sub_arguments
    filtered_sysargs = [
        word for i, word in enumerate(sysargs)
            if i not in sub_arg_indices
    ]

    try:
        args, unknown = parser.parse_known_args(filtered_sysargs, exit_on_error=False)
        args_dict = vars(args)
    except Exception as e:
        _action = []
        for a in filtered_sysargs:
            if a.startswith('-'):
                break
            _action.append(a)
        args_dict = {'action': _action, 'sysargs': sysargs}
        try:
            args_dict['text'] = shlex.join(sysargs)
        except Exception as _e:
            args_dict['text'] = ' '.join(sysargs)
        unknown = []

    false_flags = [arg for arg, val in args_dict.items() if val is False]
    for arg in false_flags:
        args_dict.pop(arg, None)

    args_dict['sysargs'] = sysargs
    args_dict['filtered_sysargs'] = filtered_sysargs
    ### append decorated arguments to sub_arguments list
    if 'sub_args' not in args_dict:
        args_dict['sub_args'] = []
    if args_dict['sub_args'] is None:
        args_dict['sub_args'] = []
    sub_arguments = args_dict['sub_args'] + sub_arguments
    parsed_sub_arguments = []
    for sub_arg in sub_arguments:
        if ' ' in sub_arg:
            parsed_sub_arguments += sub_arg.split(' ')
        else:
            parsed_sub_arguments.append(sub_arg)
    args_dict['sub_args'] = parsed_sub_arguments
    ### In case of empty subargs
    if args_dict['sub_args'] == ['']:
        args_dict['sub_args'] = []

    ### remove None (but not False) args
    none_args = []
    for a, v in args_dict.items():
        if v is None:
            none_args.append(a)
    for a in none_args:
        del args_dict[a]

    ### location_key '[None]' or 'None' -> None
    if 'location_keys' in args_dict:
        args_dict['location_keys'] = [
            None if lk in ('[None]', 'None')
            else lk for lk in args_dict['location_keys'] 
        ]

    return parse_synonyms(args_dict)


def parse_line(line: str) -> Dict[str, Any]:
    """
    Parse a line of text into standard Meerschaum arguments.
    
    Parameters
    ----------
    line: str
        The line of text to be parsed.

    Returns
    -------
    A dictionary of arguments.

    Examples
    --------
    >>> parse_line('show pipes --debug')
    {'action': ['show', 'pipes'], 'debug': True,}

    """
    import shlex
    try:
        return parse_arguments(shlex.split(line))
    except Exception as e:
        return {'action': [], 'text': line,}


def parse_synonyms(
        args_dict: Dict[str, Any]
    ) -> Dict[str, Any]:
    """Check for synonyms (e.g. `async` = `True` -> `unblock` = `True`)"""
    if args_dict.get('async', None):
        args_dict['unblock'] = True
    if args_dict.get('mrsm_instance', None):
        args_dict['instance'] = args_dict['mrsm_instance']
    if args_dict.get('skip_check_existing', None):
        args_dict['check_existing'] = False
    if args_dict.get('venv', None) in ('None', '[None]'):
        args_dict['venv'] = None
    return args_dict


def parse_dict_to_sysargs(
        args_dict : Dict[str, Any]
    ) -> List[str]:
    """Revert an arguments dictionary back to a command line list."""
    import json
    from meerschaum._internal.arguments._parser import get_arguments_triggers
    sysargs = []
    sysargs += args_dict.get('action', [])
    allow_none_args = {'location_keys'}

    triggers = get_arguments_triggers()

    for a, t in triggers.items():
        if a == 'action' or a not in args_dict:
            continue
        ### Add boolean flags
        if isinstance(args_dict[a], bool):
            if args_dict[a] is True:
                sysargs += [t[0]]
        else:
            ### Add list flags
            if isinstance(args_dict[a], (list, tuple)):
                if len(args_dict[a]) > 0:
                    sysargs += [t[0]] + list(args_dict[a])

            ### Add dict flags
            elif isinstance(args_dict[a], dict):
                if len(args_dict[a]) > 0:
                    sysargs += [t[0], json.dumps(args_dict[a])]

            ### Account for None and other values
            elif (args_dict[a] is not None) or (args_dict[a] is None and a in allow_none_args):
                sysargs += [t[0], args_dict[a]]

    return sysargs


def remove_leading_action(
        action: List[str],
        _actions: Optional[Dict[str, Callable[[Any], SuccessTuple]]] = None,
    ) -> List[str]:
    """
    Remove the leading strings in the `action` list.

    Parameters
    ----------
    actions: List[str]
        The standard, unaltered action dictionary.

    Returns
    -------
    The portion of the action list without the leading action.

    Examples
    --------
    >>> remove_leading_action(['show'])
    []
    >>> remove_leading_action(['show', 'pipes'])
    []
    >>> remove_leading_action(['show', 'pipes', 'baz'])
    ['baz']
    >>> ### foo_bar is a custom action.
    >>> remove_leading_action(['foo', 'bar'])
    []
    >>> remove_leading_action(['foo', 'bar', 'baz'])
    ['baz']
    >>> 
    """
    from meerschaum.actions import get_action, get_main_action_name
    from meerschaum.utils.warnings import warn
    from meerschaum.config.static import STATIC_CONFIG
    action_function = get_action(action, _actions=_actions)
    if action_function is None:
        return action

    UNDERSCORE_STANDIN = STATIC_CONFIG['system']['arguments']['underscore_standin']

    ### e.g. 'show'
    main_action_name = get_main_action_name(action, _actions)
    if main_action_name is None:
        return []

    ### Replace underscores with a standin so we can preserve the exising underscores.
    _action = []
    for a in action:
        _action.append(a.replace('_', UNDERSCORE_STANDIN))

    ### e.g. 'show_pipes_baz'
    action_str = '_'.join(_action)

    ### e.g. 'show_pipes'
    action_name = action_function.__name__.lstrip('_')

    ### Could contain a prefix ("do_"), so find where to begin.
    main_action_index = action_name.find(main_action_name)

    ### Strip away any leading prefices.
    action_name = action_name[main_action_index:]

    if not action_str.replace(UNDERSCORE_STANDIN, '_').startswith(action_name):
        warn(f"Unable to parse '{action_str}' for action '{action_name}'.")
        return action
    
    parsed_action = action_str[len(action_name)+1:].split('_')

    ### Substitute the underscores back in.
    _parsed_action = []
    for a in parsed_action:
        _parsed_action.append(
            a.replace(UNDERSCORE_STANDIN, '_')
        )
    if _parsed_action and _parsed_action[0] == '' and action[0] != '':
        del _parsed_action[0]
    return _parsed_action


def load_plugin_args() -> None:
    """
    If a plugin makes use of the `add_plugin_argument` function,
    load its module.
    """
    from meerschaum.plugins import get_plugins, import_plugins
    to_import = []
    for plugin in get_plugins():
        with open(plugin.__file__, encoding='utf-8') as f:
            text = f.read()
        if 'add_plugin_argument' in text:
            to_import.append(plugin.name)
    if not to_import:
        return
    import_plugins(*to_import) 
