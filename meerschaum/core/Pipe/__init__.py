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
>>> ### Columns only need to be defined if you're creating a new pipe.
>>> pipe = Pipe('csv', 'energy', columns={'datetime': 'time', 'id': 'station_id'})
>>> 
>>> ### Create a Pandas DataFrame somehow,
>>> ### or you can use a dictionary of lists instead.
>>> df = pd.read_csv('data.csv')
>>> pipe.sync(df)
```

## Registering a Remote Pipe
---

```
>>> from meerschaum import Pipe
>>> pipe = Pipe('sql:remote_server', 'energy', parameters={
...     'fetch': {
...         'definition': 'SELECT * FROM energy_table',
...     },
...     'columns': {'datetime': 'time', 'id': 'station_id'}
... })
>>> 
>>> pipe.sync()
```

"""

from __future__ import annotations
import copy
from meerschaum.utils.typing import Optional, Dict, Any, Union, InstanceConnector, List
from meerschaum.utils.formatting._pipes import pipe_repr
from meerschaum.config import get_config

class Pipe:
    """
    Access Meerschaum pipes via Pipe objects.
    
    Pipes are identified by the following:

    1. Connector keys (e.g. `'sql:main'`)
    2. Metric key (e.g. `'weather'`)
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
        dtypes,
        get_columns,
        get_columns_types,
        get_indices,
        tags,
        get_id,
        id,
        get_val_column,
        parents,
        children,
        target,
        _target_legacy,
        guess_datetime,
    )
    from ._show import show
    from ._edit import edit, edit_definition, update
    from ._sync import sync, get_sync_time, exists, filter_existing
    from ._delete import delete
    from ._drop import drop
    from ._clear import clear
    from ._bootstrap import bootstrap
    from ._dtypes import enforce_dtypes, infer_dtypes

    def __init__(
        self,
        connector: str = '',
        metric: str = '',
        location: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        columns: Optional[Dict[str, str]] = None,
        tags: Optional[List[str]] = None,
        target: Optional[str] = None,
        dtypes: Optional[Dict[str, str]] = None,
        instance: Optional[Union[str, InstanceConnector]] = None,
        temporary: bool = False,
        mrsm_instance: Optional[Union[str, InstanceConnector]] = None,
        cache: bool = False,
        debug: bool = False,
        connector_keys: Optional[str] = None,
        metric_key: Optional[str] = None,
        location_key: Optional[str] = None,
    ):
        """
        Parameters
        ----------
        connector: str
            Keys for the pipe's source connector, e.g. `'sql:main'`.

        metric: str
            Label for the pipe's contents, e.g. `'weather'`.

        location: str, default None
            Label for the pipe's location. Defaults to `None`.

        parameters: Optional[Dict[str, Any]], default None
            Optionally set a pipe's parameters from the constructor,
            e.g. columns and other attributes.
            You can edit these parameters with `edit pipes`.

        columns: Optional[Dict[str, str]], default None
            Set the `columns` dictionary of `parameters`.
            If `parameters` is also provided, this dictionary is added under the `'columns'` key.

        tags: Optional[List[str]], default None
            A list of strings to be added under the `'tags'` key of `parameters`.
            You can select pipes with certain tags using `--tags`.

        dtypes: Optional[Dict[str, str]], default None
            Set the `dtypes` dictionary of `parameters`.
            If `parameters` is also provided, this dictionary is added under the `'dtypes'` key.

        mrsm_instance: Optional[Union[str, InstanceConnector]], default None
            Connector for the Meerschaum instance where the pipe resides.
            Defaults to the preconfigured default instance (`'sql:main'`).

        instance: Optional[Union[str, InstanceConnector]], default None
            Alias for `mrsm_instance`. If `mrsm_instance` is supplied, this value is ignored.

        temporary: bool, default False
            If `True`, prevent instance tables (pipes, users, plugins) from being created.

        cache: bool, default False
            If `True`, cache fetched data into a local database file.
            Defaults to `False`.
        """
        from meerschaum.utils.warnings import error, warn
        if (not connector and not connector_keys) or (not metric and not metric_key):
            error(
                "Please provide strings for the connector and metric\n    "
                + "(first two positional arguments)."
            )

        ### Fall back to legacy `location_key` just in case.
        if not location:
            location = location_key

        if not connector:
            connector = connector_keys

        if not metric:
            metric = metric_key

        if location in ('[None]', 'None'):
            location = None

        from meerschaum.config.static import STATIC_CONFIG
        negation_prefix = STATIC_CONFIG['system']['fetch_pipes_keys']['negation_prefix']
        for k in (connector, metric, location, *(tags or [])):
            if str(k).startswith(negation_prefix):
                error(f"A pipe's keys and tags cannot start with the prefix '{negation_prefix}'.")

        self.connector_keys = str(connector)
        self.connector_key = self.connector_keys ### Alias
        self.metric_key = metric
        self.location_key = location
        self.temporary = temporary

        self._attributes = {
            'connector_keys': self.connector_keys,
            'metric_key': self.metric_key,
            'location_key': self.location_key,
            'parameters': {},
        }

        ### only set parameters if values are provided
        if isinstance(parameters, dict):
            self._attributes['parameters'] = parameters
        else:
            if parameters is not None:
                warn(f"The provided parameters are of invalid type '{type(parameters)}'.")
            self._attributes['parameters'] = {}

        if isinstance(columns, dict):
            self._attributes['parameters']['columns'] = columns
        elif columns is not None:
            warn(f"The provided columns are of invalid type '{type(columns)}'.")

        if isinstance(tags, (list, tuple)):
            self._attributes['parameters']['tags'] = tags
        elif tags is not None:
            warn(f"The provided tags are of invalid type '{type(tags)}'.")

        if isinstance(target, str):
            self._attributes['parameters']['target'] = target
        elif target is not None:
            warn(f"The provided target is of invalid type '{type(target)}'.")

        if isinstance(dtypes, dict):
            self._attributes['parameters']['dtypes'] = dtypes
        elif dtypes is not None:
            warn(f"The provided dtypes are of invalid type '{type(dtypes)}'.")

        ### NOTE: The parameters dictionary is {} by default.
        ###       A Pipe may be registered without parameters, then edited,
        ###       or a Pipe may be registered with parameters set in-memory first.
        #  from meerschaum.config import get_config
        _mrsm_instance = mrsm_instance if mrsm_instance is not None else instance
        if _mrsm_instance is None:
            _mrsm_instance = get_config('meerschaum', 'instance', patch=True)

        if not isinstance(_mrsm_instance, str):
            self._instance_connector = _mrsm_instance
            self.instance_keys = str(_mrsm_instance)
        else: ### NOTE: must be SQL or API Connector for this work
            self.instance_keys = _mrsm_instance

        self._cache = cache and get_config('system', 'experimental', 'cache')


    @property
    def meta(self):
        """Simulate the MetaPipe model without importing FastAPI."""
        if '_meta' not in self.__dict__:
            self._meta = {
                'connector_keys' : self.connector_keys,
                'metric_key'     : self.metric_key,
                'location_key'   : self.location_key,
                'instance'       : self.instance_keys,
            }
        return self._meta


    @property
    def instance_connector(self) -> Union[InstanceConnector, None]:
        """
        The connector to where this pipe resides.
        May either be of type `meerschaum.connectors.sql.SQLConnector` or
        `meerschaum.connectors.api.APIConnector`.
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
    def connector(self) -> Union[meerschaum.connectors.Connector, None]:
        """
        The connector to the data source.
        May be of type `'sql'`, `'api`', `'mqtt'`, or `'plugin'`.
        """
        if '_connector' not in self.__dict__:
            from meerschaum.connectors.parse import parse_instance_keys
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                try:
                    conn = parse_instance_keys(self.connector_keys)
                except Exception as e:
                    conn = None
            if conn:
                self._connector = conn
            else:
                return None
        return self._connector


    @property
    def cache_connector(self) -> Union[meerschaum.connectors.sql.SQLConnector, None]:
        """
        If the pipe was created with `cache=True`, return the connector to the pipe's
        SQLite database for caching.
        """
        if not self._cache:
            return None

        if '_cache_connector' not in self.__dict__:
            from meerschaum.connectors import get_connector
            from meerschaum.config._paths import DUCKDB_RESOURCES_PATH, SQLITE_RESOURCES_PATH
            _resources_path = SQLITE_RESOURCES_PATH
            self._cache_connector = get_connector(
                'sql', '_cache_' + str(self),
                flavor='sqlite',
                database=str(_resources_path / ('_cache_' + str(self) + '.db')),
            )

        return self._cache_connector


    @property
    def cache_pipe(self) -> Union['meerschaum.Pipe', None]:
        """
        If the pipe was created with `cache=True`, return another `meerschaum.Pipe` used to
        manage the local data.
        """
        if self.cache_connector is None:
            return None
        if '_cache_pipe' not in self.__dict__:
            from meerschaum.config._patch import apply_patch_to_config
            from meerschaum.utils.sql import sql_item_name
            _parameters = copy.deepcopy(self.parameters)
            _fetch_patch = {
                'fetch': ({
                    'definition': (
                        f"SELECT * FROM {sql_item_name(str(self), self.instance_connector.flavor)}"
                    ),
                }) if self.instance_connector.type == 'sql' else ({
                    'connector_keys': self.connector_keys,
                    'metric_key': self.metric_key,
                    'location_key': self.location_key,
                })
            }
            _parameters = apply_patch_to_config(_parameters, _fetch_patch)
            self._cache_pipe = Pipe(
                self.instance_keys,
                (self.connector_keys + '_' + self.metric_key + '_cache'),
                self.location_key,
                mrsm_instance = self.cache_connector,
                parameters = _parameters,
                cache = False,
                temporary = True,
            )

        return self._cache_pipe


    @property
    def sync_time(self) -> Union['datetime.datetime', None]:
        """
        Convenience function to get the pipe's latest datetime.
        Use `meerschaum.Pipe.get_sync_time()` instead.
        """
        return self.get_sync_time()

    def __str__(self, ansi: bool=False):
        return pipe_repr(self, ansi=ansi)


    def __eq__(self, other):
        try:
            return (
                isinstance(self, type(other))
                and self.connector_keys == other.connector_keys
                and self.metric_key == other.metric_key
                and self.location_key == other.location_key
                and self.instance_keys == other.instance_keys
            )
        except Exception as e:
            return False

    def __hash__(self):
        ### Using an esoteric separator to avoid collisions.
        sep = "[\"']"
        return hash(
            str(self.connector_keys) + sep
            + str(self.metric_key) + sep
            + str(self.location_key) + sep
            + str(self.instance_keys) + sep
        )

    def __repr__(self, **kw) -> str:
        return pipe_repr(self, **kw)

    def __getstate__(self) -> Dict[str, Any]:
        """
        Define the state dictionary (pickling).
        """
        return {
            'connector_keys': self.connector_keys,
            'metric_key': self.metric_key,
            'location_key': self.location_key,
            'parameters': self.parameters,
            'mrsm_instance': self.instance_keys,
        }

    def __setstate__(self, _state: Dict[str, Any]):
        """
        Read the state (unpickling).
        """
        connector_keys = _state.pop('connector_keys')
        metric_key = _state.pop('metric_key')
        location_key = _state.pop('location_key')
        self.__init__(connector_keys, metric_key, location_key, **_state)
