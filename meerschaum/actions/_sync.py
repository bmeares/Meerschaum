#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Synchronize Pipes' data with sources

NOTE: `sync` required a SQL connection and is not intended for client use
"""

def sync(
        action : list = [''],
        **kw        
    ) -> tuple:
    """
    Sync elements
    """
    from meerschaum.utils.misc import choose_subaction
    options = {
        'pipes'   : _sync_pipes,
    }
    return choose_subaction(action, options, **kw)

def _pipes_lap(
        workers : int = None,
        debug : bool = None,
        unblock : bool = False,
        #  min_seconds : int = 0,
        **kw
    ) -> tuple:
    """
    Do a lap of syncing Pipes
    """
    from meerschaum import get_pipes
    from meerschaum.utils.debug import dprint
    import time
    pipes = get_pipes(
        as_list=True,
        method = 'registered',
        debug = debug,
        **kw
    )

    ### enforce minimum cooldown
    #  sync_times_dict = dict()

    def sync_pipe(p):
        """
        Wrapper function for the Pool
        """
        ### check if enough seconds have passed
        #  last_sync_time = None
        #  if p in sync_times_dict:
            #  last_sync_time = sync_times_dict[p]
        #  too_soon = False
        #  now = time.time()
        #  if last_sync_time:
            #  too_soon = ((diff := (now - last_sync_time)) >= min_seconds)

        #  if too_soon:
            #  return False, f"Too soon for Pipe '{p}'. There are {round(diff, 2)} seconds until next sync."

        #  sync_times_dict[p] = time.time()
        #  print(sync_times_dict)
        #  return p.sync(debug=debug)
        from meerschaum.utils.warnings import warn
        try:
            return_tuple = p.sync(blocking=(not unblock), debug=debug)
        except Exception as e:
            warn(str(e), stacklevel=4)
            return_tuple = (False, f"Failed to sync Pipe '{p}' with exception:" + "\n" + str(e))
        
        return return_tuple

    from multiprocessing import cpu_count
    from multiprocessing.pool import ThreadPool as Pool
    import multiprocessing
    if workers is None:
        workers = cpu_count()

    pool = Pool(processes=workers)

    results = pool.map_async(sync_pipe, pipes)
    results = results.get()
    if results is None:
        warn(f"Failed to fetch results from syncing Pipes.")
        succeeded_pipes = []
        failed_pipes = pipes
        results_dict = dict([(p, (False, f"Could not fetch sync result for Pipe '{p}'")) for p in pipes])
    else:
        ### determine which Pipes failed to sync
        pipe_indices = [i for p, i in enumerate(pipes)]
        succeeded_pipes = [pipe_indices[i] for i, r in enumerate(results) if r[0]]
        failed_pipes = [pipe_indices[i] for i, r in enumerate(results) if not r[0]]
        results_dict = dict([(p, r) for p, r in zip(pipe_indices, results)])

    if len(failed_pipes) > 0:
        print("\n" + f"Failed to sync Pipes:")
        for p in failed_pipes: print(f"  - {p}")

    if len(succeeded_pipes) > 0:
        print("\n" + f"Successfully synced Pipes:")
        for p in succeeded_pipes:
            print(f"  - {p}")

    if debug:
        import pprintpp
        dprint("\n" + f"Return values for each Pipe:")
        pprintpp.pprint(results_dict)

    return succeeded_pipes, failed_pipes

def _sync_pipes(
        loop : bool = False,
        min_seconds : int = 1,
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Fetch new data for Pipes
    """
    from meerschaum.utils.debug import dprint
    import time
    run = True
    msg = ""
    cooldown = 2 * (min_seconds + 1)
    while run:
        lap_begin = time.time()
        try:
            success, fail = _pipes_lap(
                debug=debug,
                **kw
            )
        except Exception as e:
            from meerschaum.utils.warnings import warn
            print(e)
            warn(f"Failed to sync all pipes. Waiting for {cooldown} seconds, then trying again.", stacklevel=3)
            time.sleep(cooldown)
            cooldown = int(cooldown * 1.5)
            continue
        cooldown = 2 * (min_seconds + 1)
        lap_end = time.time()
        msg = (
            "\n" + f"It took {round(lap_end - lap_begin, 2)} seconds to sync {len(success) + len(fail)} pipes" + "\n" + 
            f"  ({len(success)} succeeded, {len(fail)} failed)."
        )
        print(msg)
        if min_seconds > 0 and loop:
            print("\n" + f"Sleeping for {min_seconds} second" + ("s" if abs(min_seconds) != 1 else ""))
            time.sleep(min_seconds)
        run = loop
    return True, msg

### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
sync.__doc__ += _choices_docstring('sync')

