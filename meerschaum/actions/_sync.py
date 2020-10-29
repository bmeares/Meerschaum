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
        **kw
    ) -> tuple:
    """
    Do a lap of syncing Pipes
    """
    from meerschaum import get_pipes
    pipes = get_pipes(
        as_list=True,
        method = 'registered',
        debug = debug,
        **kw
    )

    def sync_pipe(p):
        """
        Wrapper function for the Pool
        """
        return p.sync(debug=debug)

    from multiprocessing import cpu_count
    from multiprocessing.pool import ThreadPool as Pool
    import multiprocessing
    if workers is None:
        workers = cpu_count()

    pool = Pool(processes=workers)

    results = pool.map(sync_pipe, pipes)

    ### determine which Pipes failed to sync
    pipe_indices = [i for p, i in enumerate(pipes)]
    succeeded_pipes = [pipe_indices[i] for i, r in enumerate(results) if r[0]]
    failed_pipes = [pipe_indices[i] for i, r in enumerate(results) if not r[0]]

    print(f"Failed to sync Pipes:")
    for p in failed_pipes:
        print(f"  - {p}")
    print('')

    print(f"Successfully synced Pipes:")
    for p in succeeded_pipes:
        print(f"  - {p}")

    if debug:
        from meerschaum import get_connector
        import pprintpp

    return succeeded_pipes, failed_pipes

def _sync_pipes(
        loop : bool = False,
        debug : bool = False,
        **kw
    ) -> tuple:
    """
    Fetch new data for Pipes
    """
    from meerschaum.utils.debug import dprint
    import time
    run = True
    while run:
        lap_begin = time.time()
        success, fail = _pipes_lap(
            debug=debug,
            **kw
        )
        lap_end = time.time()
        print(
            f"It took {round(lap_end - lap_begin, 2)} seconds to sync {len(success) + len(fail)} pipes\n" +
            f"  ({len(success)} succeeded, {len(fail)} failed)."
        )
        run = loop
    return True, "Success"

### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
sync.__doc__ += _choices_docstring('sync')

