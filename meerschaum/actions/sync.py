#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Synchronize pipes' data with sources.

NOTE: `sync` required a SQL connection and is not intended for client use
"""

from __future__ import annotations
from meerschaum.utils.typing import SuccessTuple, Any, List, Optional, Tuple

def sync(
        action : Optional[List[str]] = None,
        **kw : Any
    ) -> SuccessTuple:
    """
    Fetch and sync data for pipes.
    """
    from meerschaum.utils.misc import choose_subaction
    options = {
        'pipes'   : _sync_pipes,
    }
    return choose_subaction(action, options, **kw)

def _pipes_lap(
        workers : Optional[int] = None,
        debug : bool = None,
        unblock : bool = False,
        force : bool = False,
        min_seconds : int = 1,
        **kw : Any
    ) -> Tuple[List[meerschaum.Pipe], List[meerschaum.Pipe]]:
    """
    Do a lap of syncing pipes.
    """
    from meerschaum import get_pipes
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.formatting import print_tuple
    from meerschaum.utils.warnings import warn
    import time
    pipes = get_pipes(
        as_list = True,
        method = 'registered',
        debug = debug,
        **kw
    )

    def sync_pipe(p):
        """
        Wrapper function for the Pool.
        """
        from meerschaum.utils.warnings import warn
        try:
            return_tuple = p.sync(
                blocking = (not unblock),
                force = force,
                debug = debug,
                min_seconds = min_seconds,
                workers = workers,
                **kw
            )
        except Exception as e:
            import traceback
            traceback.print_exception(type(e), e, e.__traceback__)
            print("Error: " + str(e))
            return_tuple = (False, f"Failed to sync pipe '{p}' with exception:" + "\n" + str(e))

        print_tuple(return_tuple)
        return return_tuple

    from meerschaum.utils.pool import get_pool

    pool = get_pool(workers=workers)
    results = pool.map(sync_pipe, pipes) if pool is not None else [sync_pipe(p) for p in pipes]

    #  results = pool.map_async(sync_pipe, pipes)
    #  results = results.get()
    if results is None:
        warn(f"Failed to fetch results from syncing pipes.")
        succeeded_pipes = []
        failed_pipes = pipes
        results_dict = {
            p: (False, f"Could not fetch sync result for pipe '{p}'.") for p in pipes
        }
    else:
        ### Determine which pipes failed to sync.
        pipe_indices = [i for p, i in enumerate(pipes)]
        try:
            succeeded_pipes = [pipe_indices[i] for i, r in enumerate(results) if r[0]]
            failed_pipes = [pipe_indices[i] for i, r in enumerate(results) if not r[0]]
        except TypeError:
            succeeded_pipes = []
            failed_pipes = [p for p in pipes]
        results_dict = { p: r for p, r in zip(pipe_indices, results) }

    if len(failed_pipes) > 0:
        print("\n" + f"Failed to sync pipes:")
        for p in failed_pipes:
            print(f"  - {p}")

    if len(succeeded_pipes) > 0:
        success_msg = "\nSuccessfully synced pipes:"
        if unblock:
            success_msg = "\nSuccessfully spawned threads for pipes:"
        print(success_msg)
        for p in succeeded_pipes:
            print(f"  - {p}")

    if debug:
        from meerschaum.utils.formatting import pprint
        dprint("\n" + f"Return values for each pipe:")
        pprint(results_dict)

    return succeeded_pipes, failed_pipes

def _sync_pipes(
        loop : bool = False,
        min_seconds : int = 1,
        debug : bool = False,
        **kw : Any
    ) -> SuccessTuple:
    """
    Fetch and sync new data for pipes.
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn, info
    import time, sys
    run = True
    msg = ""
    interrupt_warning_msg = "Syncing was interrupted due to a keyboard interrupt."
    cooldown = 2 * (min_seconds + 1)
    success = []
    while run:
        lap_begin = time.time()
        try:
            success, fail = _pipes_lap(
                min_seconds = min_seconds,
                debug = debug,
                **kw
            )
        except Exception as e:
            import traceback
            traceback.print_exc()
            warn(
                f"Failed to sync all pipes. Waiting for {cooldown} seconds, then trying again.",
                stack = False
            )
            try:
                time.sleep(cooldown)
            except KeyboardInterrupt:
                warn(interrupt_warning_msg, stack=False)
                loop, run = False, False
            else:
                cooldown = int(cooldown * 1.5)
                continue
        except KeyboardInterrupt:
            warn(interrupt_warning_msg, stack=False)
            loop, run = False, False
            success, fail = None, None
        cooldown = 2 * (min_seconds + 1)
        lap_end = time.time()
        print()
        msg = (
            f"It took {round(lap_end - lap_begin, 2)} seconds to sync " +
            f"{len(success) + len(fail)} pipe" +
                ("s" if (len(success) + len(fail)) > 1 else "") + "\n" +
            f"    ({len(success)} succeeded, {len(fail)} failed)."
        ) if success is not None else "Syncing was aborted."
        if min_seconds > 0 and loop:
            print()
            info(
                f"Sleeping for {min_seconds} second" +
                ("s" if abs(min_seconds) != 1 else "")
                + '.'
            )
            try:
                time.sleep(min_seconds)
            except KeyboardInterrupt:
                loop, run = False, False
                warn(interrupt_warning_msg, stack=False)
        run = loop
    return (len(success) > 0 if success else False), msg

### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
sync.__doc__ += _choices_docstring('sync')
