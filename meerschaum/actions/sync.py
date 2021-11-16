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
        action: Optional[List[str]] = None,
        **kw: Any
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
        _progress: Optional['rich.progress.Progress'] = None,
        **kw : Any
    ) -> Tuple[List[meerschaum.Pipe], List[meerschaum.Pipe]]:
    """
    Do a lap of syncing pipes.
    """
    from meerschaum import get_pipes
    from meerschaum.utils.debug import dprint, _checkpoint
    from meerschaum.utils.packages import attempt_import, import_rich
    from meerschaum.utils.formatting import print_tuple, ANSI, UNICODE, get_console
    from meerschaum.utils.formatting._shell import clear_screen, flush_with_newlines
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.threading import Lock, RLock
    from meerschaum.utils.misc import print_options, get_cols_lines
    from meerschaum.utils.pool import get_pool_executor
    import contextlib
    import time, os
    rich_table, rich_text, rich_box = attempt_import('rich.table', 'rich.text', 'rich.box')
    locks = {'remaining_count': Lock(), 'results_dict': Lock(), }
    pipes = get_pipes(
        as_list = True,
        method = 'registered',
        debug = debug,
        **kw
    )
    results_dict = {}

    def _task_label(count: int):
        return f"[cyan]Syncing {count} pipe{'s' if count != 1 else ''}..."

    remaining_count = len(pipes)
    _task = (
        _progress.add_task(_task_label(remaining_count), start=True, total=remaining_count)
    ) if _progress is not None else None


    def _print_progress_table():
        ### Print the results in a table.
        cols, lines = get_cols_lines()
        table = rich_table.Table(
            title = rich_text.Text('Synced Pipes'),
            box = (rich_box.ROUNDED if UNICODE else rich_box.ASCII),
            show_lines = True,
            show_header = ANSI,
        )
        table.add_column("Pipe", justify='right', style=('magenta' if ANSI else ''))
        table.add_column("Message")
        table.add_column("Status")
        statuses = {
            True: rich_text.Text('Success', style=('green' if ANSI else '')),
            False: rich_text.Text('Fail', style=('red' if ANSI else '')),
            None: rich_text.Text('In-progress', style=('yellow' if ANSI else '')),
        }
        rows = []
        items = []
        Text = rich_text.Text
        for pipe in pipes:
            status = statuses[results_dict.get(pipe, (None, ""))[0]]
            message = results_dict.get(pipe, (None, f"Syncing pipe '{pipe}'..."))[1]
            row = (str(pipe), message, status)
            item = Text(str(pipe) + "\n" + message + "\n") + status
            items.append(item)
            rows.append(row)
            table.add_row(str(pipe), message, status)
        clear_screen(debug=debug)
        #  print_options(items, header='Synced Pipes')
        get_console().print(table)

    def sync_pipe(p):
        """
        Wrapper function for the Pool.
        """
        try:
            return_tuple = p.sync(
                blocking = (not unblock),
                force = force,
                debug = debug,
                min_seconds = min_seconds,
                workers = workers,
                #  _progress = _progress,
                #  _task = _task,
                **kw
            )
        except Exception as e:
            import traceback
            traceback.print_exception(type(e), e, e.__traceback__)
            print("Error: " + str(e))
            return_tuple = (False, f"Failed to sync pipe '{p}' with exception:" + "\n" + str(e))

        locks['results_dict'].acquire()
        nonlocal results_dict
        results_dict[p] = return_tuple
        locks['results_dict'].release()

        _print_progress_table()
        #  print_tuple(return_tuple, _progress=_progress)
        _checkpoint(_progress=_progress, _task=_task)
        if _progress is not None:
            locks['remaining_count'].acquire()
            nonlocal remaining_count
            remaining_count -= 1
            _progress.update(_task, description=_task_label(remaining_count))
            locks['remaining_count'].release()
        return return_tuple

    pool = get_pool_executor(workers=workers)
    cm = contextlib.nullcontext() if pool is None else pool
    with cm:
        for pipe in pipes:
            _print_progress_table()
            pool.submit(sync_pipe, pipe)
        
    #  clear_screen(debug=debug)
    _print_progress_table()

    results = list(results_dict)
    #  results = pool.map(sync_pipe, pipes) if pool is not None else [sync_pipe(p) for p in pipes]

    if results is None:
        warn(f"Failed to fetch results from syncing pipes.")
        succeeded_pipes = []
        failed_pipes = pipes
        results_dict = {
            p: (False, f"Could not fetch sync result for pipe '{p}'.") for p in pipes
        }
    else:
        ### Determine which pipes failed to sync.
        results_dict = { p: r for p, r in zip(pipes, results) }
        try:
            succeeded_pipes, failed_pipes = [], []
            for pipe, result in results_dict.items():
                (succeeded_pipes if result[0] else failed_pipes).append(pipe)
        except TypeError:
            succeeded_pipes = []
            failed_pipes = [p for p in pipes]


    #  if len(failed_pipes) > 0:
        #  print("\n" + f"Failed to sync pipes:")
        #  for p in failed_pipes:
            #  print(f"  - {p}")

    #  if len(succeeded_pipes) > 0:
        #  success_msg = "\nSuccessfully synced pipes:"
        #  if unblock:
            #  success_msg = "\nSuccessfully spawned threads for pipes:"
        #  print(success_msg)
        #  for p in succeeded_pipes:
            #  print(f"  - {p}")

    if debug:
        from meerschaum.utils.formatting import pprint
        dprint("\n" + f"Return values for each pipe:")
        pprint(results_dict)

    return succeeded_pipes, failed_pipes

def _sync_pipes(
        loop: bool = False,
        min_seconds: int = 1,
        shell: bool = False,
        debug: bool = False,
        **kw: Any
    ) -> SuccessTuple:
    """
    Fetch and sync new data for pipes.
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn, info
    from meerschaum.utils.formatting._shell import progress
    import contextlib
    import time, sys
    run = True
    msg = ""
    interrupt_warning_msg = "Syncing was interrupted due to a keyboard interrupt."
    cooldown = 2 * (min_seconds + 1)
    success = []
    while run:
        _progress = progress() if shell else None
        cm = _progress if _progress is not None else contextlib.nullcontext()

        lap_begin = time.perf_counter()

        try:
            with cm:
                success, fail = _pipes_lap(
                    min_seconds = min_seconds,
                    _progress = _progress,
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
            success, fail = None, None
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
        lap_end = time.perf_counter()
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
