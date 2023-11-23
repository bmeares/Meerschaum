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
    https://rocketry.readthedocs.io/en/stable/condition_syntax/index.html

    Parameters
    ----------
    function: Callable[[Any], Any]
        The function to execute.

    frequency: str
        The frequency at which `function` should be executed (e.g. `'daily'`).

    """
    import warnings
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.misc import filter_keywords
    from concurrent.futures._base import CancelledError
    kw['debug'] = debug
    kw = filter_keywords(function, **kw)

    def _wrapper():
        return function(*args, **kw)

    pydantic = attempt_import('pydantic', debug=debug, lazy=False)
    rocketry = attempt_import('rocketry', debug=debug, lazy=False)
    try:
        app = rocketry.Rocketry()
        FuncTask = rocketry.tasks.FuncTask
        with warnings.catch_warnings():
            warnings.filterwarnings('ignore', 'Task\'s session not defined.')
            task = FuncTask(_wrapper, start_cond=frequency)
            app.session.add_task(task)
        return app.run(debug=debug)
    except (KeyboardInterrupt, CancelledError):
        try:
            app.session.shut_down(force=True)
        except CancelledError:
            pass
        return None
    except AttributeError:
        warn(
            "Failed to import scheduler.\n\n   "
            + "Run `mrsm install package 'pydantic<2.0.0'` and try again.",
            stack = False,
        )

