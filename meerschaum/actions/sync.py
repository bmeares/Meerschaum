#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Synchronize pipes' data with sources.

NOTE: `sync` required a SQL connection and is not intended for client use
"""

from __future__ import annotations
from datetime import timedelta
from meerschaum.utils.typing import SuccessTuple, Any, List, Optional, Tuple, Union

def sync(
        action: Optional[List[str]] = None,
        **kw: Any
    ) -> SuccessTuple:
    """
    Fetch and sync data for pipes.

    Usage:
        - `--loop`
            - Sync indefinitely.
        - `--min-seconds 10`
            - Wait 10 seconds between laps.
        - `--async`, `--unblock``
            - Spin up background threads for each pipe.
        - `--debug`
            - Print verbose messages.
    """
    from meerschaum.actions import choose_subaction
    options = {
        'pipes': _sync_pipes,
    }
    return choose_subaction(action, options, **kw)


def _pipes_lap(
        workers: Optional[int] = None,
        debug: bool = None,
        unblock: bool = False,
        force: bool = False,
        min_seconds: int = 1,
        verify: bool = False,
        deduplicate: bool = False,
        bounded: Optional[bool] = None,
        chunk_interval: Union[timedelta, int, None] = None,
        mrsm_instance: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        nopretty: bool = False,
        _progress: Optional['rich.progress.Progress'] = None,
        **kw: Any
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
    from meerschaum.connectors.parse import parse_instance_keys
    from meerschaum import Plugin
    import queue
    import multiprocessing
    import contextlib
    import time, os, copy
    from meerschaum.utils.packages import venv_exec
    from meerschaum.utils.process import poll_process
    import json
    import sys
    dill = attempt_import('dill')

    rich_table, rich_text, rich_box = attempt_import(
        'rich.table', 'rich.text', 'rich.box',
        lazy = False,
    )
    all_kw = copy.deepcopy(kw)
    all_kw.update({
        'workers': workers,
        'debug': debug,
        'unblock': unblock,
        'force': force,
        'min_seconds': min_seconds,
        'timeout_seconds': timeout_seconds,
        'mrsm_instance': mrsm_instance,
        'verify': verify,
        'deduplicate': deduplicate,
        'bounded': bounded,
        'chunk_interval': chunk_interval,
    })
    locks = {'remaining_count': Lock(), 'results_dict': Lock(), 'pipes_threads': Lock(),}
    pipes = get_pipes(
        as_list=True, method='registered', debug=debug, mrsm_instance=mrsm_instance, **kw
    )
    remaining_count = len(pipes)
    instance_connector = parse_instance_keys(mrsm_instance, debug=debug)
    conns = (
        instance_connector.engine.pool.size() if instance_connector.type == 'sql'
        else len(pipes)
    )
    cores = multiprocessing.cpu_count()
    pipes_queue = queue.Queue(remaining_count)
    for pipe in pipes:
        pipes_queue.put_nowait(pipe)
    stop_event = Event()
    results_dict = {}

    ### Cap the number workers to the pool size or 1 if working in-memory.
    if workers is None:
        workers = (
            1 if (
                instance_connector.type == 'sql'
                and
                instance_connector.__dict__.get('database', None) == ':memory:'
            ) else min(cores, (conns if conns != 0 else cores))
        )
    if workers > conns and conns != 0 and instance_connector.type == 'sql':
        warn(
            f"Using more workers ({workers}) than the available pool of database connections "
            + "may lead to concurrency issues.\n    You can change the pool size with "
            + "`edit config system` under the keys connectors:sql:create_engine:pool_size,\n    "
            + "and a size of 0 will not limit the number of connections.",
            stack = False,
        )


    def _task_label(count: int):
        return f"[cyan]Syncing {count} pipe{'s' if count != 1 else ''}..."

    _task = (
        _progress.add_task(_task_label(len(pipes)), start=True, total=len(pipes))
    ) if _progress is not None else None


    def worker_fn():
        while not stop_event.is_set():
            try:
                pipe = pipes_queue.get_nowait()
            except queue.Empty:
                return
            return_tuple = sync_pipe(pipe)
            results_dict[pipe] = return_tuple

            if not nopretty:
                success, msg = return_tuple
                msg = (
                    f"Finished syncing {pipe}:\n" if success
                    else f"Error while syncing {pipe}:\n"
                ) + msg + '\n'
                print_tuple(
                    (success, msg),
                    calm = True,
                    _progress = _progress,
                )
            _checkpoint(_progress=_progress, _task=_task)
            if _progress is not None:
                nonlocal remaining_count
                with locks['remaining_count']:
                    remaining_count -= 1
            pipes_queue.task_done()

    sync_function_source = dill.source.getsource(_wrap_sync_pipe)
    fence_begin, fence_end = '<MRSM_RESULT>', '</MRSM_RESULT>'

    def sync_pipe(p):
        """
        Wrapper function for handling exceptions.
        """
        ### If no timeout is specified, handle syncing in the current thread.
        if timeout_seconds is None:
            return _wrap_sync_pipe(p, **all_kw)
        _success_tuple = False, "Nothing returned."
        def write_line(line):
            nonlocal _success_tuple
            decoded = line.decode('utf-8')
            begin_index, end_index = decoded.find(fence_begin), decoded.find(fence_end)

            ### Found the beginning of the return value.
            ### Don't write the parsed success tuple message.
            if begin_index >= 0:
                _success_tuple = tuple(json.loads(
                    decoded[begin_index + len(fence_begin):end_index]
                ))
                return
            sys.stdout.buffer.write(line)

        def timeout_handler(p, *args, **kw):
            success, msg = False, (
                f"Failed to sync {p} within {timeout_seconds} second"
                + ('s' if timeout_seconds != 1 else '') + '.'
            )
            write_line((fence_begin + json.dumps((success, msg)) + fence_end).encode('utf-8'))

        src = (
            "from __future__ import annotations\n"
            + "from datetime import timedelta\n"
            + "from typing import Optional, Union\n"
            + "\n\n"
            + sync_function_source
            + '\n\n'
            + "import meerschaum as mrsm\n"
            + "import json\n"
            + f"pipe = mrsm.Pipe(**json.loads({json.dumps(json.dumps(p.meta))}))\n"
            + f"""print(
                    '{fence_begin}'
                    + json.dumps(
                        _wrap_sync_pipe(
                            pipe,
                            **json.loads({json.dumps(json.dumps(all_kw))})
                        )
                    )
                    + '{fence_end}'
            )"""
        )
        if debug:
            dprint(src)

        proc = venv_exec(src, venv=None, as_proc=True, debug=debug)
        poll_process(
            proc,
            write_line,
            timeout_seconds,
            timeout_handler,
            (p,)
        )
        return _success_tuple

    worker_threads = [Thread(target=worker_fn) for _ in range(min(workers, len(pipes)))]
    for worker_thread in worker_threads:
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
    return results_dict


def _sync_pipes(
        loop: bool = False,
        min_seconds: int = 1,
        unblock: bool = False,
        verify: bool = False,
        deduplicate: bool = False,
        bounded: Optional[bool] = None,
        chunk_interval: Union[timedelta, int, None] = None,
        shell: bool = False,
        nopretty: bool = False,
        debug: bool = False,
        **kw: Any
    ) -> SuccessTuple:
    """
    Fetch and sync new data for pipes.

    Usage:
        - `--loop`
            - Sync indefinitely.
        - `--min-seconds 10`
            - Wait 10 seconds between laps.
        - `--async`, `--unblock``
            - Spin up background threads for each pipe.
        - `--debug`
            - Print verbose messages.
    """
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import warn, info
    from meerschaum.utils.formatting import UNICODE
    from meerschaum.utils.formatting._shell import progress, live
    from meerschaum.utils.formatting._shell import clear_screen, flush_with_newlines
    from meerschaum.utils.formatting import print_pipes_results
    import contextlib
    import time
    import sys
    import json
    import asyncio
    run = True
    msg = ""
    interrupt_warning_msg = "Syncing was interrupted due to a keyboard interrupt."
    cooldown = 2 * (min_seconds + 1)
    underline = '\u2015' if UNICODE else '-'
    success_pipes, failure_pipes = [], []
    while run:
        _progress = progress() if shell else None
        cm = _progress if _progress is not None else contextlib.nullcontext()

        lap_begin = time.perf_counter()

        try:
            results_dict = {}
            with cm:
                results_dict = _pipes_lap(
                    min_seconds = min_seconds,
                    _progress = _progress,
                    verify = verify,
                    deduplicate = deduplicate,
                    bounded = bounded,
                    chunk_interval = chunk_interval,
                    unblock = unblock,
                    debug = debug,
                    nopretty = nopretty,
                    **kw
                )
                success_pipes = [
                    pipe
                    for pipe, (_success, _msg) in results_dict.items()
                    if _success
                ]
                failure_pipes = [
                    pipe
                    for pipe, (_success, _msg) in results_dict.items()
                    if not _success
                ]
        except Exception as e:
            import traceback
            traceback.print_exc()
            warn(
                f"Failed to sync all pipes. Waiting for {cooldown} seconds, then trying again.",
                stack = False
            )
            results_dict = {}
            success_pipes, failure_pipes = None, None
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
        cooldown = 2 * (min_seconds + 1)
        lap_end = time.perf_counter()
        print()

        if success_pipes is not None and not loop and shell and not nopretty:
            clear_screen(debug=debug)

        success_msg = (
            "Successfully spawned threads for pipes:"
            if unblock
            else f"Successfully synced pipes:"
        )
        fail_msg = f"Failed to sync pipes:"
        if results_dict:
            print_pipes_results(
                results_dict,
                success_header = success_msg,
                failure_header = fail_msg,
                nopretty = nopretty,
            )

        msg = (
            f"It took {round(lap_end - lap_begin, 2)} seconds to sync " +
            f"{len(success_pipes) + len(failure_pipes)} pipe" +
                ("s" if (len(success_pipes) + len(failure_pipes)) != 1 else "") + "\n" +
            f"    ({len(success_pipes)} succeeded, {len(failure_pipes)} failed)."
        ) if success_pipes is not None else "Syncing was aborted."
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
    return (len(success_pipes) > 0 if success_pipes is not None else False), msg


def _wrap_sync_pipe(
        pipe,
        unblock: bool = False,
        force: bool = False,
        debug: bool = False,
        min_seconds: int = 1,
        workers = None,
        verify: bool = False,
        deduplicate: bool = False,
        bounded: Optional[bool] = None,
        chunk_interval: Union[timedelta, int, None] = None,
        **kw
    ):
    """
    Wrapper function for handling exceptions.
    """
    from meerschaum.connectors import get_connector_plugin
    from meerschaum.utils.venv import Venv
    try:
        with Venv(get_connector_plugin(pipe.connector), debug=debug):
            if not verify and not deduplicate:
                sync_method = pipe.sync
            elif not verify and deduplicate:
                sync_method = pipe.deduplicate
            else:
                sync_method = pipe.verify
                kw['deduplicate'] = deduplicate
            return_tuple = sync_method(
                blocking = (not unblock),
                force = force,
                debug = debug,
                min_seconds = min_seconds,
                workers = workers,
                bounded = bounded,
                chunk_interval = chunk_interval,
                **{k: v for k, v in kw.items() if k != 'blocking'}
            )
    except Exception as e:
        import traceback
        traceback.print_exception(type(e), e, e.__traceback__)
        print("Error: " + str(e))
        return_tuple = (False, f"Failed to sync {pipe} with exception:" + "\n" + str(e))

    return return_tuple



### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
sync.__doc__ += _choices_docstring('sync')
