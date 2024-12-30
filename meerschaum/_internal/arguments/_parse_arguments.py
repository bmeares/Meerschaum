#! /usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
This module contains functions for parsing arguments
"""

from __future__ import annotations
import json
from datetime import timedelta
from meerschaum.utils.typing import List, Dict, Any, Optional, Callable, SuccessTuple, Tuple
from meerschaum.utils.threading import RLock

_locks = {
    '_loaded_plugins_args': RLock(),
}
_loaded_plugins_args: bool = False


def split_pipeline_sysargs(sysargs: List[str]) -> Tuple[List[str], List[str]]:
    """
    Split `sysargs` into the main pipeline and the flags following the pipeline separator (`:`).
    """
    from meerschaum.config.static import STATIC_CONFIG
    pipeline_key = STATIC_CONFIG['system']['arguments']['pipeline_key']
    if pipeline_key not in sysargs:
        return sysargs, []

    ### Find the index of the last occurrence of `:`.
    pipeline_ix = len(sysargs) - 1 - sysargs[::-1].index(pipeline_key)
    sysargs_after_pipeline_key = sysargs[pipeline_ix+1:]
    sysargs = [arg for arg in sysargs[:pipeline_ix] if arg != pipeline_key]
    return sysargs, sysargs_after_pipeline_key


def split_chained_sysargs(sysargs: List[str]) -> List[List[str]]:
    """
    Split a `sysargs` list containing "and" keys (`+`)
    into a list of individual `sysargs`.
    """
    from meerschaum.config.static import STATIC_CONFIG
    and_key = STATIC_CONFIG['system']['arguments']['and_key']

    if not sysargs or and_key not in sysargs:
        return [sysargs]

    ### Coalesce and consecutive joiners into one.
    coalesce_args = []
    previous_arg = None
    for arg in [_arg for _arg in sysargs]:
        if arg == and_key and previous_arg == and_key:
            continue
        coalesce_args.append(arg)
        previous_arg = arg

    ### Remove any joiners from the ends.
    if coalesce_args[0] == and_key:
        coalesce_args = coalesce_args[1:]
    if coalesce_args[-1] == and_key:
        coalesce_args = coalesce_args[:-1]

    chained_sysargs = []
    current_sysargs = []
    for arg in coalesce_args:
        if arg != and_key:
            current_sysargs.append(arg)
        else:
            chained_sysargs.append(current_sysargs)
            current_sysargs = []
    chained_sysargs.append(current_sysargs)
    return chained_sysargs


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
            found_begin_decorator = str(word).startswith(begin_decorator)
            found_end_decorator = str(word).endswith(end_decorator)

        if found_begin_decorator:
            ### check if sub arg is ever closed
            for a in sysargs[i:]:
                if a.endswith(end_decorator):
                    is_sub_arg = True
                    found_begin_decorator = False
        elif found_end_decorator:
            for a in sysargs[:i]:
                if str(a).startswith(begin_decorator):
                    is_sub_arg = True
                    found_begin_decorator = False
        if is_sub_arg:
            ### remove decorators
            sa = word
            if str(sa).startswith(begin_decorator):
                sa = sa[len(begin_decorator):]
            if str(sa).endswith(end_decorator):
                sa = sa[:-1 * len(end_decorator)]
            sub_arguments.append(sa)
            ### remove sub-argument from action list
            sub_arg_indices.append(i)

    ### rebuild sysargs without sub_arguments
    filtered_sysargs = [
        word
        for i, word in enumerate(sysargs)
        if i not in sub_arg_indices
    ]

    try:
        args, unknown = parser.parse_known_args(filtered_sysargs, exit_on_error=False)
        args_dict = vars(args)
    except Exception as e:
        _action = []
        for a in filtered_sysargs:
            if str(a).startswith('-'):
                break
            _action.append(a)
        args_dict = {'action': _action, 'sysargs': sysargs}
        try:
            args_dict['text'] = shlex.join(sysargs)
        except Exception as _e:
            args_dict['text'] = ' '.join(sysargs)
        args_dict[STATIC_CONFIG['system']['arguments']['failure_key']] = e

    false_flags = [arg for arg, val in args_dict.items() if val is False]
    for arg in false_flags:
        _ = args_dict.pop(arg, None)

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
    none_args_keep = []
    for a, v in args_dict.items():
        if v is None:
            none_args.append(a)
        elif v == 'None':
            none_args_keep.append(a)
    for a in none_args:
        del args_dict[a]
    for a in none_args_keep:
        args_dict[a] = None

    ### location_key '[None]' or 'None' -> None
    if 'location_keys' in args_dict:
        args_dict['location_keys'] = [
            (
                None
                if lk in ('[None]', 'None')
                else lk
            )
            for lk in args_dict['location_keys']
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
    except Exception:
        return {'action': [], 'text': line}


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
    if args_dict.get('skip_enforce_dtypes', None):
        args_dict['enforce_dtypes'] = False
    if args_dict.get('venv', None) in ('None', '[None]'):
        args_dict['venv'] = None
    chunk_minutes = args_dict.get('chunk_minutes', None)
    chunk_hours = args_dict.get('chunk_hours', None)
    chunk_days = args_dict.get('chunk_days', None)
    if isinstance(chunk_minutes, int):
        args_dict['chunk_interval'] = timedelta(minutes=chunk_minutes)
    elif isinstance(chunk_hours, int):
        args_dict['chunk_interval'] = timedelta(hours=chunk_hours)
    elif isinstance(chunk_days, int):
        args_dict['chunk_interval'] = timedelta(days=chunk_days)

    return args_dict


def parse_dict_to_sysargs(
    args_dict: Dict[str, Any],
    coerce_dates: bool = True,
) -> List[str]:
    """Revert an arguments dictionary back to a command line list."""
    import shlex
    from meerschaum._internal.arguments._parser import get_arguments_triggers

    action = args_dict.get('action', None)
    sysargs: List[str] = []
    sysargs.extend(action or [])
    allow_none_args = {'location_keys', 'begin', 'end', 'executor_keys'}

    triggers = get_arguments_triggers()

    for a, t in triggers.items():
        if a == 'action' or a not in args_dict:
            continue

        ### Add boolean flags
        if isinstance(args_dict[a], bool):
            if args_dict[a] is True:
                sysargs.extend([t[0]])
        else:
            ### Add list flags
            if isinstance(args_dict[a], (list, tuple)):
                if len(args_dict[a]) > 0:
                    if a == 'sub_args' and args_dict[a] != ['']:
                        sysargs.extend(
                            [
                                '-A',
                                shlex.join([
                                    str(item) for item in args_dict[a]
                                ]),
                            ]
                        )
                    else:
                        sysargs.extend(
                            [t[0]]
                            + [
                                str(item)
                                for item in args_dict[a]
                            ]
                        )

            ### Add dict flags
            elif isinstance(args_dict[a], dict):
                if len(args_dict[a]) > 0:
                    sysargs += [t[0], json.dumps(args_dict[a], separators=(',', ':'))]

            ### Preserve the original datetime strings if possible
            elif a in ('begin', 'end') and 'sysargs' in args_dict:
                flag = t[0]
                flag_ix = args_dict['sysargs'].index(flag)
                if flag_ix < 0:
                    continue
                try:
                    flag_val = args_dict['sysargs'][flag_ix + 1]
                except IndexError:
                    flag_val = str(args_dict[a])

                sysargs += [flag, str(flag_val)]

            ### Account for None and other values
            elif (args_dict[a] is not None) or (args_dict[a] is None and a in allow_none_args):
                sysargs += [t[0], str(args_dict[a])]

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

    ### e.g. 'show_pipes'
    action_name = action_function.__name__.lstrip('_')

    ### Could contain a prefix ("do_"), so find where to begin.
    main_action_index = action_name.find(main_action_name)

    ### Strip away any leading prefices.
    action_name = action_name[main_action_index:]

    subaction_parts = action_name.replace(main_action_name, '').lstrip('_').split('_')
    subaction_name = subaction_parts[0] if subaction_parts else None

    ### e.g. 'pipe' -> 'pipes'
    if subaction_name and subaction_name.endswith('s') and not action[1].endswith('s'):
        _action[1] += 's'

    ### e.g. 'show_pipes_baz'
    action_str = '_'.join(_action)

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


def sysargs_has_api_executor_keys(sysargs: List[str]) -> bool:
    """
    Check whether a `sysargs` list contains an `api` executor.
    """
    if '-e' not in sysargs and '--executor-keys' not in sysargs:
        return False

    for i, arg in enumerate(sysargs):
        if arg not in ('-e', '--executor-keys'):
            continue

        executor_keys_ix = i + 1
        if len(sysargs) <= executor_keys_ix:
            return False

        executor_keys = sysargs[executor_keys_ix]
        if executor_keys.startswith('api:'):
            return True

    return False


def remove_api_executor_keys(sysargs: List[str]) -> List[str]:
    """
    Remove any api executor keys from `sysargs`.
    """
    from meerschaum.utils.misc import flatten_list

    if not sysargs_has_api_executor_keys(sysargs):
        return sysargs

    skip_indices = set(flatten_list(
        [
            [i, i+1]
            for i, arg in enumerate(sysargs)
            if arg in ('-e', '--executor-keys')
        ]
    ))

    return [
        arg
        for i, arg in enumerate(sysargs)
        if i not in skip_indices
    ]


def get_pipeline_sysargs(
    sysargs: List[str],
    pipeline_args: List[str],
    _patch_args: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """
    Parse `sysargs` and `pipeline_args` into a single `start pipeline` sysargs.
    """
    import shlex
    start_pipeline_params = {
        'sub_args_line': shlex.join(sysargs),
        'patch_args': _patch_args,
    }
    return (
        ['start', 'pipeline']
        + [str(arg) for arg in pipeline_args]
        + ['-P', json.dumps(start_pipeline_params, separators=(',', ':'))]
    )


def compress_pipeline_sysargs(pipeline_sysargs: List[str]) -> List[str]:
    """
    Given a `start pipeline` sysargs, return a condensed syntax rendition.
    """
    import shlex

    if pipeline_sysargs[:2] != ['start', 'pipeline']:
        return pipeline_sysargs

    if '-P' not in pipeline_sysargs:
        return pipeline_sysargs

    params_ix = pipeline_sysargs.index('-P')
    pipeline_args = pipeline_sysargs[2:params_ix]
    params_str = pipeline_sysargs[-1]
    try:
        start_pipeline_params = json.loads(params_str)
    except Exception:
        return pipeline_sysargs

    sub_args_line = start_pipeline_params.get('sub_args_line', None)
    if not sub_args_line:
        return pipeline_sysargs

    return (
        shlex.split(sub_args_line)
        + [':']
        + pipeline_args
    )
