#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Pipes are the primary data access objects in the Meerschaum system
"""

class Pipe:
    from ._fetch import fetch
    from ._register import register
    from ._attributes import attributes, parameters, columns
    from ._show import show
    from ._edit import edit
    from ._sync import sync, get_sync_time, get_backtrack_data, exists

    def __init__(
        self,
        connector_keys : str,
        metric_key : str,
        location_key : str = None,
        parameters : dict = None,
        source : str = 'sql:main',
        debug : bool = False
    ):
        """
        connector_keys : str
            keys to get Meerschaum connector
            e.g. 'sql:main'
        
        metric_key : str
            standard Meerschaum metric key
        
        location_key : str
            standard Meerschaum location key

        parameters : dict : {}
            parameters dictionary to give the Pipe.
            This dictionary is NOT stored in memory but rather is used for registration purposes.
        
        source : str : 'sql:main'
            connector_keys for the source connector
        """
        self.connector_keys = connector_keys
        self.metric_key = metric_key
        self.location_key = location_key

        ### only set parameters if values are provided
        if parameters is not None:
            self._parameters = parameters
        
        ### NOTE: The parameters dictionary is {} by default.
        ###       A Pipe may be registered without parameters, then edited,
        ###       or a Pipe may be registered with parameters set in-memory first.
        from meerschaum.api.models import MetaPipe
        self.meta = MetaPipe(
            connector_keys = connector_keys,
            metric_key = metric_key,
            location_key = location_key,
            parameters = parameters
        )

        ### TODO aggregations?
        #  self._aggregations = dict()

        ### TODO implement source. This one will be tough bc of datetime parsing (need regex magic),
        ###      but the groundwork is there, just have to implement it in the API.

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
            q = f"""
            SELECT pipe_id
            FROM pipes
            WHERE connector_keys = '{self.connector_keys}'
                AND metric_key = '{self.metric_key}'
                AND location_key """ + ("IS NULL" if self.location_key is None else f"= '{self.location_key}'")
            self._id = meta_connector.value(q)
        return self._id

    @property
    def sync_time(self):
        if '_sync_time' not in self.__dict__:
            self._sync_time = self.get_sync_time()

        if self._sync_time is None:
            del self._sync_time
            return None

        return self._sync_time

    def __str__(self):
        """
        The Pipe's SQL table name. Converts the ':' in the connector_keys to an '_'.
        """
        name = f"{self.connector_keys.replace(':', '_')}_{self.metric_key}"
        if self.location_key is not None:
            name += f"_{self.location_key}"
        return name

    def __repr__(self):
        return str(self)
