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

    if workers is None:
        from multiprocessing import cpu_count
        workers = cpu_count()

    if pool_class_name not in pools:
        from meerschaum.utils.misc import attempt_import
        Pool = getattr(attempt_import('multiprocessing.pool'), pool_class_name)
        pools[pool_class_name] = Pool(workers)

    return pools[pool_class_name]

def get_pools():
    """
    Return the global pools dictionary
    """
    global pools
    if pools is None: pools = dict()
    return pools
