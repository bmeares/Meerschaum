#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Pipes are the primary data access objects in the Meerschaum system
"""

class Pipe:
    from ._fetch import fetch
    from ._register import register
    def __init__(
        self,
        connector_keys : str,
        metric_key : str,
        location_key : str = None,
        source : str = 'sql',
        debug : bool = False
    ):
        """
        connector_str : keys to get Meerschaum connector
            e.g. 'sql:main'
        metric_key : standard Meerschaum metric key
        location_key : standard Meerschaum location key
        """
        self.connector_keys = connector_keys
        self.metric_key = metric_key
        self.location_key = location_key
        
        from meerschaum.api.models import MetaPipe
        self.meta = MetaPipe(
            connector_keys = connector_keys,
            metric_key = metric_key,
            location_key = location_key
        )

        ### TODO aggregations?
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

    @property
    def id(self):
        if '_id' not in self.__dict__:
            from meerschaum import get_connector
            meta_connector = get_connector('sql', 'meta')
            q = """
            SELECT pipe_id
            FROM pipes
            WHERE connector_keys = '{self.connector_keys}'
                AND metric_key = '{self.metric_key}'
                AND location_key """ + 
            self._id = meta_connector.value("SELECT pipe_id FROM pipes WHERE conn")
    
    def __str__(self):
        name = f"{self.connector_keys.replace(':', '_')}_{self.metric_key}"
        if self.location_key is not None:
            name += f"_{self.location_key}"
        return name

    def __repr__(self):
        return str(self)
