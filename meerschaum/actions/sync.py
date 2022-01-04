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
        min_seconds: int = 1,
        mrsm_instance: Optional[str] = None,
        timeout_seconds: int = 300,
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
    import queue
    import multiprocessing
    import contextlib
    import time, os
    rich_table, rich_text, rich_box = attempt_import(
        'rich.table', 'rich.text', 'rich.box',
    )
    all_kw = kw.copy()
    all_kw.update({'workers': workers, 'debug': debug, 'unblock': unblock, 'force': force,
        'min_seconds': min_seconds, 'timeout_seconds': timeout_seconds,
        'mrsm_instance': mrsm_instance, '_progress': _progress,})
    pipes = get_pipes(
        as_list = True,
        method = 'registered',
        debug = debug,
        mrsm_instance = mrsm_instance,
        **kw
    )
    instance_connector = parse_instance_keys(mrsm_instance, debug=debug)
    conns = (
        instance_connector.engine.pool.size() if instance_connector.type == 'sql'
        else len(pipes)
    )
    cores = multiprocessing.cpu_count()
    ### Cap the number workers to the pool size or 1 if working in-memory.
    if workers is None:
        workers = (
            1 if instance_connector.type == 'sql' and instance_connector.database == ':memory:'
            else min(cores, (conns if conns != 0 else cores))
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


    def worker_fn(
            results_dict,
            pipes_queue,
            stop_event,
            results_dict_lock,
            remaining_count_lock,
            timeout_seconds,
            remaining_count,
        ):
        #  nonlocal results_dict, pipes_queue
        print('inside worker')
        while not stop_event.is_set():
            print(pipes_queue)
            try:
                pipe = pipes_queue.get_nowait()
                print(pipe)
            except queue.Empty:
                print('queue is empty')
                return
            try:
                from meerschaum.utils.threading import Thread
                _syncing_thread = Thread(target=sync_pipe, error_callback=foo, args=(pipe,))
                _syncing_thread.start()
                return_tuple = _syncing_thread.join(timeout=timeout_seconds)
                if return_tuple is None:
                    return_tuple = False, (
                        "Failed to sync pipe '{pipe}' within {timeout_seconds} second"
                        + ('s' if timeout_seconds != 1 else '')
                    )
            except Exception as e:
                return_tuple = False, str(e)
                print(e)
            #  return_tuple = sync_pipe(pipe)
            results_dict_lock.acquire()
            results_dict[pipe] = return_tuple
            results_dict_lock.release()

            print_tuple(return_tuple, _progress=_progress)
            _checkpoint(_progress=_progress, _task=_task)
            if _progress is not None:
                remaining_count_lock.acquire()
                #  nonlocal remaining_count
                remaining_count.value -= 1
                remaining_count_lock.release()
            pipes_queue.task_done()

    def sync_pipe(p, **kw):
        """
        Wrapper function for handling exceptions.
        """
        try:
            return_tuple = p.sync(
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
            return_tuple = (False, f"Failed to sync pipe '{p}' with exception:" + "\n" + str(e))

        return return_tuple

    #  worker_threads = [Thread(target=worker_fn) for _ in range(min(workers, len(pipes)))]
    #  for worker_thread in worker_threads:
        #  worker_thread.start()

    from meerschaum.utils.packages import venv_exec
    from meerschaum.utils.process import poll_process
    import json
    import sys
    dill = attempt_import('dill')
    def write_line(line):
        sys.stdout.buffer.write(line)
    
    sync_function_source = dill.source.getsource(_wrap_sync_pipe)
    for p in pipes:
        src = sync_function_source + '\n\n' + (
            "import meerschaum as mrsm\n"
            + "import json\n"
            + f"pipe = mrsm.Pipe(**json.loads({json.dumps(json.dumps(p.meta))}))\n"
            + f"_wrap_sync_pipe(pipe, **json.loads({json.dumps(json.dumps(all_kw))}))"
        )
        if debug:
            dprint(src)
        proc = venv_exec(src, venv=None, as_proc=True, debug=debug)
        poll_process(proc, write_line, timeout_seconds)

    return pipes, [], {pipes[0]: (False, "Testing")}

    manager = multiprocessing.Manager()
    with manager:
        results_dict = manager.dict()
        #  pipes_queue = multiprocessing.JoinableQueue(remaining_count.value)
        remaining_count = manager.Value('i', len(pipes))
        pipes_queue = manager.Queue(remaining_count.value)
        [pipes_queue.put_nowait(pipe) for pipe in pipes]
        stop_event = manager.Event()
        locks = {
            'results_dict': manager.Lock(),
            'remaining_count': manager.Lock(),
        }

        worker_procs = [
            multiprocessing.Process(
                target = worker_fn,
                args = (
                    results_dict,
                    pipes_queue,
                    stop_event,
                    locks['results_dict'],
                    locks['remaining_count'],
                    timeout_seconds,
                    remaining_count,
                )
            ) for _ in range(min(workers, len(pipes)))
        ]
        for worker_proc in worker_procs:
            worker_proc.start()

    try:
        while any([w.is_alive() for w in worker_procs]):
            time.sleep(0.1)
    except KeyboardInterrupt:
        stop_event.set()
        raise

    print('joining...')
    pipes_queue.join()
    #  print('joined queue.')
    for worker_proc in worker_procs:
        worker_proc.join()
    print('Done joining.')

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

        if success is not None and not loop:
            clear_screen(debug=debug)
        if fail is not None and len(fail) > 0:
            print_options(
                [str(p) + "\n"
                    + (
                        results_dict[p][1] if isinstance(results_dict.get(p, None), tuple)
                        else "No message was returned."
                    ) + "\n" for p in fail],
                header = "Failed to sync pipes:"
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
    try:
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
        return_tuple = (False, f"Failed to sync pipe '{pipe}' with exception:" + "\n" + str(e))

    return return_tuple



### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
sync.__doc__ += _choices_docstring('sync')
