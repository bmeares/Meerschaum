#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Pretty printing wrapper
"""

def pprint(
    *args,
    detect_password: bool = True,
    nopretty: bool = False,
    **kw
) -> None:
    """Pretty print an object according to the configured ANSI and UNICODE settings.
    If detect_password is True (default), search and replace passwords with '*' characters.
    Does not mutate objects.
    """
    import copy
    import json
    from meerschaum.utils.packages import attempt_import, import_rich
    from meerschaum.utils.formatting import ANSI, get_console, print_tuple
    from meerschaum.utils.warnings import error
    from meerschaum.utils.misc import replace_password, dict_from_od, filter_keywords
    from collections import OrderedDict

    if (
        len(args) == 1
        and
        isinstance(args[0], tuple)
        and
        len(args[0]) == 2
        and
        isinstance(args[0][0], bool)
        and
        isinstance(args[0][1], str)
    ):
        return print_tuple(args[0], **filter_keywords(print_tuple, **kw))

    modify = True
    rich_pprint = None
    if ANSI and not nopretty:
        rich = import_rich()
        if rich is not None:
            rich_pretty = attempt_import('rich.pretty')
        if rich_pretty is not None:
            def _rich_pprint(*args, **kw):
                _console = get_console()
                _kw = filter_keywords(_console.print, **kw)
                _console.print(*args, **_kw)
            rich_pprint = _rich_pprint
    elif not nopretty:
        pprintpp = attempt_import('pprintpp', warn=False)
        try:
            _pprint = pprintpp.pprint
        except Exception :
            import pprint as _pprint_module
            _pprint = _pprint_module.pprint

    func = (
        _pprint if rich_pprint is None else rich_pprint
    ) if not nopretty else print

    try:
        args_copy = copy.deepcopy(args)
    except Exception:
        args_copy = args
        modify = False
    _args = []
    for a in args:
        c = a
        ### convert OrderedDict into dict
        if isinstance(a, OrderedDict) or issubclass(type(a), OrderedDict):
            c = dict_from_od(copy.deepcopy(c))
        _args.append(c)
    args = _args

    _args = list(args)
    if detect_password and modify:
        _args = []
        for a in args:
            c = a
            if isinstance(c, dict):
                c = replace_password(copy.deepcopy(c))
            if nopretty:
                try:
                    c = json.dumps(c)
                    is_json = True
                except Exception:
                    is_json = False
                if not is_json:
                    try:
                        c = str(c)
                    except Exception:
                        pass
            _args.append(c)

    ### filter out unsupported keywords
    func_kw = filter_keywords(func, **kw) if not nopretty else {}
    error_msg = None
    try:
        func(*_args, **func_kw)
    except Exception as e:
        error_msg = e
    if error_msg is not None:
        error(error_msg)
