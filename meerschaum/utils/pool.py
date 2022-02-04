#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Global pools that are joined on exit.
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Callable, List, Any
from meerschaum.utils.threading import Lock, RLock
import signal

pools = None
_locks = {
    'pools': Lock(),
}

def _initializer():
    """Ignore keyboard interrupt in workers."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def get_pool(
        pool_class_name: str = 'ThreadPool',
        workers: Optional[int] = None,
        initializer: Optional[Callable[[None], None]] = None,
        initargs: Optional[List[Any]] = None,
    ):
    """If the requested pool does not exist, instantiate it here.
    Pools are joined and closed on exit.

    Parameters
    ----------
    pool_class_name: str :
         (Default value = 'ThreadPool')
    workers: Optional[int] :
         (Default value = None)
    initializer: Optional[Callable[[None] :
        
    None]] :
         (Default value = None)
    initargs: Optional[List[Any]] :
         (Default value = None)

    Returns
    -------

    """
    global pools
    _locks['pools'].acquire()
    if pools is None:
        pools = dict()

    def build_pool(workers):
        global pools
        from meerschaum.utils.warnings import warn
        from meerschaum.utils.packages import attempt_import
        try:
            Pool = getattr(
                attempt_import('multiprocessing.pool', warn=False, venv=None, lazy=False),
                pool_class_name
            )
        except Exception as e:
            warn(e, stacklevel=3)
            Pool = getattr(
                attempt_import('multiprocessing.pool', warn=False, venv=None, lazy=False),
                'ThreadPool'
            )

        if workers is None:
            from multiprocessing import cpu_count
            workers = cpu_count()
        try:
            pools[pool_class_name] = Pool(workers, initializer=initializer, initargs=initargs)
        except Exception as e:
            print(e)
            pools[pool_class_name] = None

    if pool_class_name not in pools or pools.get(pool_class_name, None) is None:
        build_pool(workers)

    if (
        pools[pool_class_name] is not None
        and pools[pool_class_name]._state not in ('RUN', 0)
    ):
        try:
            pools[pool_class_name].close()
        except Exception as e:
            pass
        del pools[pool_class_name]
        build_pool(workers)

    _locks['pools'].release()
    return pools[pool_class_name]

def get_pools():
    """ """
    global pools
    if pools is None:
        _locks['pools'].acquire()
        pools = dict()
        _locks['pools'].release()
    return pools


def get_pool_executor(workers: Optional[int] = None):
    """

    Parameters
    ----------
    workers: Optional[int] :
         (Default value = None)

    Returns
    -------
    type
        

    """
    try:
        from multiprocessing import cpu_count
        from concurrent.futures import ThreadPoolExecutor
        workers = cpu_count() if workers is None else workers
    except Exception as e:
        ThreadPoolExecutor = None

    return ThreadPoolExecutor(max_workers=workers) if ThreadPoolExecutor is not None else None

