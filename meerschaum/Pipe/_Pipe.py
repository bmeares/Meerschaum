#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Pipes are the primary data access objects in the Meerschaum system
"""


class Pipe:
    def __init__(
        self,
        location_key : str,
        metric_key : str,
        connector_keys : str = 'sql:main',
        debug : bool = False
    ):
        """
        location_key : standard Meerschaum location key
        metric_key : standard Meerschaum metric key
        connector_str : keys to get Meerschaum connector
            e.g. 'sql:main'
        """
        self.location_key = location_key
        self.metric_key = metric_key
        self.connector_keys = connector_keys

        ### aggregations
        self._aggregations = dict()

    @property
    def connector(self):
        if '_connector' not in self.__dict__:
            from meerschaum.utils.misc import parse_connector_keys
            if (conn := parse_connector_keys(self.connector_keys)):
                self._connector = conn
            else:
                return None
        return self._connector
