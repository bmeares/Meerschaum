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
        workers: Optional[int] = None,
        debug: bool = None,
        unblock: bool = False,
        force: bool = False,
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
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.threading import Lock, RLock, Thread, Event
    from meerschaum.utils.misc import print_options, get_cols_lines
    from meerschaum.utils.pool import get_pool_executor, get_pool
    import queue
    import multiprocessing
    import contextlib
    import time, os
    rich_table, rich_text, rich_box = attempt_import(
        'rich.table', 'rich.text', 'rich.box',
    )
    locks = {'remaining_count': Lock(), 'results_dict': Lock(), 'pipes_threads': Lock(),}
    pipes = get_pipes(
        as_list = True,
        method = 'registered',
        debug = debug,
        **kw
    )
    remaining_count = len(pipes)
    conns = pipes[0].instance_connector.engine.pool.size() if pipes else 0
    cores = multiprocessing.cpu_count()
    pipes_queue = queue.Queue(remaining_count)
    [pipes_queue.put_nowait(pipe) for pipe in pipes]
    stop_event = Event()
    results_dict = {}
    pipes_threads = {}

    def _task_label(count: int):
        return f"[cyan]Syncing {count} pipe{'s' if count != 1 else ''}..."

    _task = (
        _progress.add_task(_task_label(remaining_count), start=True, total=remaining_count)
    ) if _progress is not None else None


    def worker_fn():
        nonlocal results_dict, pipes_queue
        while not stop_event.is_set():
            try:
                pipe = pipes_queue.get_nowait()
            except queue.Empty:
                return
            return_tuple = sync_pipe(pipe)
            locks['results_dict'].acquire()
            results_dict[pipe] = return_tuple
            locks['results_dict'].release()

            print_tuple(return_tuple, _progress=_progress)
            _checkpoint(_progress=_progress, _task=_task)
            if _progress is not None:
                locks['remaining_count'].acquire()
                nonlocal remaining_count
                remaining_count -= 1
                locks['remaining_count'].release()
            pipes_queue.task_done()

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
                **kw
            )
        except Exception as e:
            import traceback
            traceback.print_exception(type(e), e, e.__traceback__)
            print("Error: " + str(e))
            return_tuple = (False, f"Failed to sync pipe '{p}' with exception:" + "\n" + str(e))

        return return_tuple

    if workers is None:
        workers = min(cores, (conns if conns != 0 else cores))
    if workers > conns and conns != 0:
        warn(
            f"Using more workers ({workers}) than the available pool of database connections "
            + "may lead to concurrency issues.\n    You can change the pool size with "
            + "`edit config system` under the keys connectors:sql:create_engine:pool_size,\n    "
            + "and a size of 0 will not limit the number of connections.",
            stack = False,
        )
    worker_threads = [Thread(target=worker_fn) for _ in range(min(workers, len(pipes)))]
    for worker_thread in worker_threads:
        worker_thread.daemon = True
        worker_thread.start()

    try:
        while any([t.is_alive() for t in worker_threads]):
            time.sleep(0.1)
    except KeyboardInterrupt:
        stop_event.set()
        raise

    pipes_queue.join()
    for worker_thread in worker_threads:
        worker_thread.join()

    results = [results_dict[pipe] for pipe in pipes] if len(results_dict) == len(pipes) else None

    if results is None:
        warn(f"Failed to fetch results from syncing pipes.")
        succeeded_pipes = []
        failed_pipes = pipes
        results_dict = {
            p: (results_dict.get(p, (False, f"Could not fetch sync result for pipe '{p}'.")))
            for p in pipes
        }
    else:
        ### Determine which pipes failed to sync.
        try:
            succeeded_pipes, failed_pipes = [], []
            for pipe, result in results_dict.items():
                (succeeded_pipes if result[0] else failed_pipes).append(pipe)
        except TypeError:
            succeeded_pipes = []
            failed_pipes = [p for p in pipes]

    return succeeded_pipes, failed_pipes, results_dict

def _sync_pipes(
        loop: bool = False,
        min_seconds: int = 1,
        unblock: bool = False,
        shell: bool = False,
        debug: bool = False,
        **kw: Any
    ) -> SuccessTuple:
    """
    Fetch and sync new data for pipes.
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn, info
    from meerschaum.utils.formatting._shell import progress, live
    from meerschaum.utils.formatting._shell import clear_screen, flush_with_newlines
    from meerschaum.utils.misc import print_options
    import contextlib
    import time, sys
    import asyncio
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
                success, fail, results_dict = _pipes_lap(
                    min_seconds = min_seconds,
                    _progress = _progress,
                    unblock = unblock,
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

        if success is not None and not loop:
            clear_screen(debug=debug)
        if fail is not None and len(fail) > 0:
            print_options(
                [str(p) + "\n" + results_dict[p][1] + "\n" for p in fail],
                header ="Failed to sync pipes:"
            )

        if success is not None and len(success) > 0:
            success_msg = "Successfully synced pipes:"
            if unblock:
                success_msg = "Successfully spawned threads for pipes:"
            print_options([str(p) + "\n" for p in success], header=success_msg)

        if debug:
            from meerschaum.utils.formatting import pprint
            dprint("\n" + f"Return values for each pipe:")
            pprint(results_dict)

        msg = (
            f"It took {round(lap_end - lap_begin, 2)} seconds to sync " +
            f"{len(success) + len(fail)} pipe" +
                ("s" if (len(success) + len(fail)) != 1 else "") + "\n" +
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
