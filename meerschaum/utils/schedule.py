#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Schedule processes and threads.
"""

from __future__ import annotations
from meerschaum.utils.typing import Callable, Any, Optional

def schedule_function(
        function: Callable[[Any], Any],
        frequency: str,
        *args,
        debug: bool = False,
        **kw
    ) -> None:
    """
    Block the process and execute the function intermittently according to the frequency.
    https://red-engine.readthedocs.io/en/stable/condition_syntax/index.html

    Parameters
    ----------
    function: Callable[[Any], Any]
        The function to execute.

    frequency: str
        The frequency at which `function` should be executed (e.g. `'daily'`).

    """
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.misc import filter_keywords
    kw['debug'] = debug
    kw = filter_keywords(function, **kw)

    def _wrapper():
        return function(*args, **kw)

    redengine = attempt_import('redengine', debug=debug)
    app = redengine.RedEngine()
    FuncTask = redengine.tasks.FuncTask
    task = FuncTask(_wrapper, start_cond=frequency)
    app.session.add_task(task)
    return app.run(debug=debug)
