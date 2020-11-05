#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Pipes are the primary data access objects in the Meerschaum system
"""
default_instance_labels = {
    'api' : 'main',
    'sql' : 'main',
}


class Pipe:
    from ._fetch import fetch
    from ._data import data, get_data, get_backtrack_data
    from ._register import register
    from ._attributes import attributes, parameters, columns, get_columns
    from ._show import show
    from ._edit import edit
    from ._sync import sync, get_sync_time, exists
    from ._delete import delete

    def __init__(
        self,
        connector_keys : str,
        metric_key : str,
        location_key : str = None,
        parameters : dict = None,
        mrsm_instance : str = None,
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
        
        mrsm_instance : str : None
            connector_keys for the Meerschaum instance connector (SQL or API connector)
        """
        if location_key == '[None]': location_key = None
        self.connector_keys = connector_keys
        self.metric_key = metric_key
        self.location_key = location_key

        ### only set parameters if values are provided
        if parameters is not None:
            self._parameters = parameters
        
        ### NOTE: The parameters dictionary is {} by default.
        ###       A Pipe may be registered without parameters, then edited,
        ###       or a Pipe may be registered with parameters set in-memory first.
        from meerschaum.config import get_config
        if mrsm_instance is None: mrsm_instance = get_config('meerschaum', 'instance', patch=True)
        if not isinstance(mrsm_instance, str):
            self._instance_connector = mrsm_instance
            self.instance_keys = mrsm_instance.type + ':' + mrsm_instance.label
        else: ### NOTE: must be SQL or API Connector for this work
            self.instance_keys = mrsm_instance

        ### TODO aggregations?
        #  self._aggregations = dict()


    @property
    def meta(self):
        """
        Simulate the MetaPipe model without importing FastAPI
        """
        refresh = False
        if '_meta' not in self.__dict__: refresh = True
        elif self.parameters != self._meta['parameters']: refresh = True
            
        if refresh:
            parameters = self.parameters
            if parameters is None: parameters = dict()
            self._meta = {
                'connector_keys' : self.connector_keys,
                'metric_key'     : self.metric_key,
                'location_key'   : self.location_key,
                'parameters'     : parameters,
            }
        return self._meta

    @property
    def instance_connector(self):
        if '_instance_connector' not in self.__dict__:
            from meerschaum.utils.misc import parse_instance_keys
            conn = parse_instance_keys(self.instance_keys)
            if conn:
                self._instance_connector = conn
            else:
                return None
        return self._instance_connector

    @property
    def connector(self):
        if '_connector' not in self.__dict__:
            from meerschaum.utils.misc import parse_instance_keys
            conn = parse_instance_keys(self.connector_keys)
            if conn:
                self._connector = conn
            else:
                return None
        return self._connector

    @property
    def id(self):
        if not ('_id' in self.__dict__ and self._id):
            self._id = self.instance_connector.get_pipe_id(self)
        return self._id

    @property
    def sync_time(self):
        if '_sync_time' not in self.__dict__:
            self._sync_time = self.get_sync_time()

        if self._sync_time is None:
            del self._sync_time
            return None

        return self._sync_time

    #  def json(self, *args, **kw):
        #  """
        #  Serialize Pipes into JSON
        #  """
        #  #  import json
        #  return str(self)

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
