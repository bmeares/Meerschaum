#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Pipes are the primary metaphor of the Meerschaum system.
You can interact with pipe data via `meerschaum.Pipe` objects.

If you are working with multiple pipes, it is highly recommended that you instead use
`meerschaum.utils.get_pipes` (available as `meerschaum.get_pipes`)
to create a dictionary of Pipe objects.

```
>>> from meerschaum import get_pipes
>>> pipes = get_pipes()
```

# Examples
For the below examples to work, `sql:remote_server` must be defined (check with `edit config`)
with correct credentials, as well as a network connection and valid permissions.

## Manually Adding Data
---

```
>>> from meerschaum import Pipe
>>> pipe = Pipe('csv', 'energy')
>>> 
>>> ### Columns only need to be defined if you're creating a new pipe.
>>> pipe.columns = { 'datetime' : 'time', 'id' : 'station_id' }
>>> 
>>> ### Create a Pandas DataFrame somehow,
>>> ### or you can use a dictionary of lists instead.
>>> df = pd.read_csv('data.csv')
>>> 
>>> pipe.sync(df)
```

## Registering a Remote Pipe
---

```
>>> from meerschaum import Pipe
>>> pipe = Pipe('sql:remote_server', 'energy')
>>> 
>>> pipe.attributes = {
...     'fetch' : {
...         'definition' : 'SELECT * FROM energy_table',
...     },
... }
>>> 
>>> ### Columns are a subset of attributes, so define columns
>>> ### after defining attributes.
>>> pipe.columns = { 'datetime' : 'time', 'id' : 'station_id' }
>>> 
>>> pipe.sync()
```

"""

from __future__ import annotations
from meerschaum.utils.typing import Optional, Dict, Any, Union, InstanceConnector

class Pipe:
    """
    Access Meerschaum pipes via Pipe objects.

    Pipes are identified by the following:
        1. Connector keys (e.g. `sql:main`)
        2. Metric key (e.g. `weather`)
        3. Location (optional; e.g. `None`)

    A pipe's connector keys correspond to a data source, and when the pipe is synced,
    its `fetch` definition is evaluated and executed to produce new data.

    Alternatively, new data may be directly synced via `pipe.sync()`:

    ```
    >>> from meerschaum import Pipe
    >>> pipe = Pipe('csv', 'weather')
    >>> 
    >>> import pandas as pd
    >>> df = pd.read_csv('weather.csv')
    >>> pipe.sync(df)
    ```
    """

    from ._fetch import fetch
    from ._data import get_data, get_backtrack_data, get_rowcount
    from ._register import register
    from ._attributes import (
        attributes,
        parameters,
        columns,
        get_columns,
        get_columns_types,
        get_id,
        id,
    )
    from ._show import show
    from ._edit import edit, edit_definition
    from ._sync import sync, get_sync_time, exists
    from ._delete import delete
    from ._drop import drop
    from ._bootstrap import bootstrap

    def __init__(
        self,
        connector_keys : str,
        metric_key : str,
        location_key : Optional[str] = None,
        parameters : Optional[Dict[str, Any]] = None,
        columns : Optional[Dict[str, str]] = None,
        mrsm_instance : Optional[Union[str, InstanceConnector]] = None,
        instance : Optional[Union[str, InstanceConnector]] = None,
        debug : bool = False
    ):
        """
        :param connector_keys:
            Keys for the pipe's source connector.
            E.g. 'sql:main'

        :param metric_key:
            Label for the pipe's contents.
            E.g. 'weather'

        :param location_key:
            Label for the pipe's location.
            Defaults to None.

        :param parameters:
            Optionally set a pipe's parameters from the constructor,
            e.g. columns and other attributes.
            Defaults to None.

        :param columns:
            Subset of parameters for ease of use.
            Defaults to None.

        :param mrsm_instance:
            Connector keys for the Meerschaum instance where the pipe resides.
            Defaults to the preconfigured default instance.

        :param instance:
            Alias for `mrsm_instance`. If `mrsm_instance` is supplied, this value is ignored.
        """
        if location_key in ('[None]', 'None'):
            location_key = None
        self.connector_keys = connector_keys
        self.metric_key = metric_key
        self.location_key = location_key

        ### only set parameters if values are provided
        if parameters is not None:
            self._parameters = parameters

        if columns is not None:
            if self.__dict__.get('_parameters', None) is None:
                self._parameters = {}
            self._parameters['columns'] = columns

        ### NOTE: The parameters dictionary is {} by default.
        ###       A Pipe may be registered without parameters, then edited,
        ###       or a Pipe may be registered with parameters set in-memory first.
        from meerschaum.config import get_config
        _mrsm_instance = mrsm_instance if mrsm_instance is not None else instance
        if _mrsm_instance is None:
            _mrsm_instance = get_config('meerschaum', 'instance', patch=True)
        if not isinstance(_mrsm_instance, str):
            self._instance_connector = _mrsm_instance
            self.instance_keys = str(_mrsm_instance)
        else: ### NOTE: must be SQL or API Connector for this work
            self.instance_keys = _mrsm_instance

    @property
    def meta(self):
        """
        Simulate the MetaPipe model without importing FastAPI.
        """
        refresh = False
        if '_meta' not in self.__dict__:
            refresh = True
        elif self.parameters != self.__dict__['_meta']['parameters']:
            refresh = True

        if refresh:
            parameters = self.parameters
            if parameters is None:
                parameters = dict()
            self._meta = {
                'connector_keys' : self.connector_keys,
                'metric_key'     : self.metric_key,
                'location_key'   : self.location_key,
                'parameters'     : parameters,
            }
        return self._meta

    @property
    def instance_connector(self):
        """
        Return the connector for the instance on which the pipe resides.
        """
        if '_instance_connector' not in self.__dict__:
            from meerschaum.connectors.parse import parse_instance_keys
            conn = parse_instance_keys(self.instance_keys)
            if conn:
                self._instance_connector = conn
            else:
                return None
        return self._instance_connector

    @property
    def connector(self):
        """
        Return the pipe's data source connector.
        """
        if '_connector' not in self.__dict__:
            from meerschaum.connectors.parse import parse_instance_keys
            conn = parse_instance_keys(self.connector_keys)
            if conn:
                self._connector = conn
            else:
                return None
        return self._connector

    @property
    def sync_time(self):
        """
        Convenience function to get the pipe's latest datetime.
        """
        return self.get_sync_time()

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

    def __getstate__(self):
        """
        Define the state dictionary (pickling).
        """
        state = {
            'connector_keys' : self.connector_keys,
            'metric_key' : self.metric_key,
            'location_key' : self.location_key,
            'parameters' : self.parameters,
            'mrsm_instance' :  self.instance_keys,
        }
        return state

    def __setstate__(self, _state : dict):
        """
        Read the state (unpickling).
        """
        self.__init__(**_state)
