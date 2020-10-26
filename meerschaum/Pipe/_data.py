#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for getting Pipe data
"""

def get_data(
        self,
        begin : 'datetime.datetime' = None,
        end : 'datetime.datetime' = None,
        refresh : bool = False,
        debug : bool = False
    ) -> 'pd.DataFrame':
    """
    Get data from the instance connector
    """
    if refresh or True: ### TODO remove `or True`
        self._data = self.instance_connector.get_pipe_data(
            pipe = self,
            begin = begin,
            end = end,
            debug = debug
        )
    ### TODO caching / sync logic
    return self._data

def get_backtrack_data(
        self,
        begin : 'datetime.datetime' = None,
        backtrack_minutes : int = 0,
        debug : bool = False
    ) -> 'pd.DataFrame':
    """
    Get the most recent data from the instance connector
    """
    return self.instance_connector.get_backtrack_data(
        pipe = self,
        begin = begin,
        backtrack_minutes = backtrack_minutes,
        debug = debug
    )

@property
def data(
        self
    ) -> 'pd.DataFrame':
    return self.get_data()
