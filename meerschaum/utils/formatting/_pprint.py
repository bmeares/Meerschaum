#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Pretty printing wrapper
"""

def pprint(
        *args,
        detect_password : bool = True,
        **kw
    ):
    """
    Pretty print an object according to the configured ANSI and UNICODE settings.
    If detect_password is True (default), search and replace passwords with '*' characters.
    Does not mutate objects.
    """
    from meerschaum.utils.packages import attempt_import, import_rich
    from meerschaum.utils.formatting import ANSI, UNICODE
    from meerschaum.utils.warnings import error
    from meerschaum.utils.misc import replace_password, dict_from_od
    from collections import OrderedDict
    import copy
    modify = True
    pprintpp = attempt_import('pprintpp', warn=False)
    rich = import_rich()
    try:
        _pprint = pprintpp.pprint
    except:
        import pprint
        _pprint = pprint.pprint
    try:
        if rich is not None:
            rich_pretty = attempt_import('rich.pretty')
        rich_pprint = rich_pretty.pprint
    except:
        rich_pprint = _pprint

    if ANSI: func = rich_pprint
    else: func = _pprint

    try:
        args_copy = copy.deepcopy(args)
    except:
        args_copy = args
        modify = False
    _args = []
    for a in args:
        c = a
        ### convert OrderedDict into dict
        if isinstance(a, OrderedDict) or issubclass(type(a), OrderedDict):
            c = dict_from_od(c.copy())
        _args.append(c)
    args = _args

    _args = list(args)
    if detect_password and modify:
        _args = []
        for a in args:
            c = a
            if isinstance(c, dict):
                c = replace_password(c.copy())
            _args.append(c)

    ### filter out unsupported keywords
    from meerschaum.utils.misc import filter_keywords
    func_kw = filter_keywords(func, **kw)
    error_msg = None
    try:
        func(*_args, **func_kw)
    except Exception as e:
        error_msg = e
    if error_msg is not None: error(error_msg)
