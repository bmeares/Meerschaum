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
    from meerschaum.utils.misc import choose_subaction
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
    all_kw.update({'workers': workers, 'debug': debug, 'unblock': unblock, 'force': force,
        'min_seconds': min_seconds, 'timeout_seconds': timeout_seconds,
        'mrsm_instance': mrsm_instance,})
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

        src = sync_function_source + '\n\n' + (
            "import meerschaum as mrsm\n"
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
            proc, write_line, timeout_seconds,
            timeout_handler, (p,)
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
    for pipe in pipes:
        try:
            if pipe.connector.type == 'plugin':
                Plugin(pipe.connector.label).deactivate_venv(force=True, debug=debug)
        except Exception as e:
            pass

    results = [results_dict[pipe] for pipe in pipes] if len(results_dict) == len(pipes) else None

    if results is None:
        warn(f"Failed to fetch results from syncing pipes.")
        succeeded_pipes = []
        failed_pipes = pipes
        results_dict = {
            p: (results_dict.get(p, (False, f"Could not fetch sync result for {p}.")))
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
    from meerschaum.utils.misc import print_options
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
                    nopretty = nopretty,
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
            results_dict = {}
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


        def get_options_to_print(
                pipes_list: List['meerschaum.Pipe'],
                include_msg: bool = True
            ) -> List[str]:
            """
            Format the output strings.
            """
            default_tuple = False, "No message returned."
            options = []
            for pipe in pipes_list:
                result = results_dict.get(pipe, default_tuple)
                if not isinstance(result, tuple):
                    result = default_tuple

                option = (
                    str(pipe)
                    + '\n'
                    + ((underline * len(str(pipe))) if include_msg else '')
                    + '\n'
                    + (str(result[1]) if include_msg else '')
                    + '\n\n'
                ) if not nopretty else (
                    json.dumps({
                        'pipe': pipe.meta,
                        'result': result,
                    })
                )
                options.append(option)
            
            return options


        if success is not None and not loop and shell and not nopretty:
            clear_screen(debug=debug)

        if success is not None and len(success) > 0:
            success_msg = "Successfully synced pipes:"
            if unblock:
                success_msg = "Successfully spawned threads for pipes:"
            print_options(
                get_options_to_print(success),
                header = success_msg,
                nopretty = nopretty,
            )

        if fail is not None and len(fail) > 0:
            print_options(
                get_options_to_print(fail),
                header = 'Failed to sync pipes:',
                nopretty = nopretty,
            )

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


def _wrap_sync_pipe(
        pipe,
        unblock: bool = False,
        force: bool = False,
        debug: bool = False,
        min_seconds: int = 1,
        workers = None,
        **kw
    ):
    """
    Wrapper function for handling exceptions.
    """
    from meerschaum.utils.venv import Venv
    try:
        venv = getattr(pipe.connector, '_plugin', None)
    except Exception as e:
        venv = 'mrsm'
    try:
        with Venv(venv, debug=debug):
            return_tuple = pipe.sync(
                blocking = (not unblock),
                force = force,
                debug = debug,
                min_seconds = min_seconds,
                workers = workers,
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
from meerschaum.utils.misc import choices_docstring as _choices_docstring
sync.__doc__ += _choices_docstring('sync')
