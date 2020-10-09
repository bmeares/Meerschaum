#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for fetching new data into the Pipe
"""

def fetch(
        self,
        debug : bool = False
    ) -> 'pd.DataFrame or None':
    """
    Fetch a Pipe's latest data from its connector
    and only add newest data into Pipe's parent table
    
    (e.g. Pipe 'connector_metric_location' dumps into 'connector_metric')

    returns : pd.DataFrame of newest unseen data
    """
    ### TODO diff the existing data with new data
    if 'fetch' in self.attributes['parameters']:
        return self.connector.fetch(
            self,
            begin = self.sync_time,
            debug = debug
        )
