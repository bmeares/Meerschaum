#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Global pools that are joined on exit
"""

pools = None

def get_pool(pool_class_name : str = 'ThreadPool', workers : int = None):
    """
    If the requested pool does not exist, instantiate it here.
    Pools are joined and closed on exit.
    """
    global pools
    if pools is None:
        pools = dict()

    def build_pool(workers):
        global pools
        from meerschaum.utils.packages import attempt_import
        try:
            Pool = getattr(attempt_import('multiprocessing.pool', warn=False, venv=None, lazy=False), pool_class_name)
        except:
            Pool = getattr(attempt_import('multiprocessing.pool', warn=False, venv=None, lazy=False), 'ThreadPool')

        if workers is None:
            from multiprocessing import cpu_count
            workers = cpu_count()
        pools[pool_class_name] = Pool(workers)

    if pool_class_name not in pools:
        build_pool(workers)

    if pools[pool_class_name]._state not in ('RUN', 0):
        try:
            pools[pool_class_name].close()
        except:
            pass
        del pools[pool_class_name]
        build_pool(workers)

    return pools[pool_class_name]

def get_pools():
    """
    Return the global pools dictionary
    """
    global pools
    if pools is None: pools = dict()
    return pools
