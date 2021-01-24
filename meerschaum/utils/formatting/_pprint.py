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
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.formatting import ANSI, UNICODE, console
    from meerschaum.utils.warnings import error
    from meerschaum.utils.misc import replace_password
    pprintpp = attempt_import('pprintpp', warn=False)
    rich_pretty = attempt_import('rich.pretty', warn=False)
    try:
        _pprint = pprintpp.pprint
    except:
        import pprint
        _pprint = pprint.pprint
    try:
        rich_pprint = rich_pretty.pprint
    except:
        rich_pprint = _pprint

    if ANSI: func = rich_pprint
    else: func = _pprint

    _args = list(args)
    if detect_password:
        _args = []
        for a in args:
            c = a
            if isinstance(c, dict):
                c = replace_password(c.copy())
            _args.append(c)

    ### filter out unsupported keywords
    import inspect
    func_params = inspect.signature(func).parameters
    func_kw = dict()
    for k, v in kw.items():
        if k in func_params:
            func_kw[k] = v
    error_msg = None
    try:
        func(*_args, **func_kw)
    except Exception as e:
        error_msg = e
    if error_msg is not None: error(e)
