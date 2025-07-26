#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define logic for caching pipes' attributes.
"""

from __future__ import annotations

import os
import pickle
import json
import pathlib
from datetime import datetime
from typing import Any, Dict, Optional, Union

import meerschaum as mrsm
from meerschaum.utils.warnings import warn, dprint


def _get_in_memory_key(cache_key: str) -> str:
    """
    Return the in-memory version of a cache key.
    """
    return (
        ('_' + cache_key)
        if not cache_key.startswith('_')
        else cache_key
    )

def _get_cache_connector(self) -> 'Union[None, ValkeyConnector]':
    """
    Return the cache connector if required.
    """
    enable_valkey_cache = mrsm.get_config('system', 'experimental', 'valkey_session_cache')
    if not enable_valkey_cache:
        return None

    if self.cache_connector_keys is None:
        return None

    if not self.cache_connector_keys.startswith('valkey:'):
        warn(f"Invalid cache connector keys: '{self.cache_connector_keys}'")
        return None

    return mrsm.get_connector(self.cache_connector_keys)


def _cache_value(
    self,
    cache_key: str,
    value: Any,
    debug: bool = False,
) -> None:
    """
    Cache a value in-memory and (if `Pipe.cache` is `True`) on-disk.
    """
    in_memory_key = _get_in_memory_key(cache_key)
    self.__dict__[in_memory_key] = value
    write_success, write_msg = (
        self._write_cache_file(cache_key, value)
        if self.cache
        else (True, "Success")
    )
    if not write_success and debug:
        dprint(f"Failed to cache '{cache_key}' to disk.")


def _get_cached_value(
    self,
    cache_key: str,
    debug: bool = False,
) -> Any:
    """
    Attempt to retrieve a cached value from in-memory on on-disk.
    """
    if debug:
        dprint(f"Attempting to read cache key: '{cache_key}'")

    in_memory_key = _get_in_memory_key(cache_key)
    if in_memory_key in self.__dict__:
        return self.__dict__[in_memory_key]

    return self._read_cache_file(cache_key, debug=debug)


def _invalidate_cache(
    self,
    hard: bool = False,
    debug: bool = False,
) -> mrsm.SuccessTuple:
    """
    Invalidate temporary in-memory cache.
    Note this does not affect in on-disk cache created when `cache=True`.

    Parameters
    ----------
    hard: bool, default False
        If `True`, clear all temporary cache.
        Otherwise only clear soft cache.

    Returns
    -------
    A `SuccessTuple` to indicate success.
    """
    if debug:
        dprint(f"Invalidating {'some' if not hard else 'all'} cache for {self}.")

    self._exists = None
    self._sync_ts = None

    if not hard:
        return True, "Success"

    _ = self.__dict__.pop('_parameters', None)
    _ = self.__dict__.pop('_precision', None)
    self._attributes_sync_time = None
    if not self.__dict__.get('_static', False):
        self._columns_types_timestamp = None
        self._columns_types = None
    return True, "Success"


def _get_cache_dir_path(self, create_if_not_exists: bool = True) -> pathlib.Path:
    """
    Return the path to the cache directory.
    """
    if '_cache_dir_path' in self.__dict__:
        return self._cache_dir_path

    from meerschaum.config.paths import PIPES_CACHE_RESOURCES_PATH
    cache_dir_path = (
        PIPES_CACHE_RESOURCES_PATH
        / self.connector_keys
        / self.metric_key
        / str(self.location_key)
    )
    if create_if_not_exists and not cache_dir_path.exists():
        try:
            cache_dir_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            warn(f"Enocuntered an issue when creating local pipe metadata cache:\n{e}")

    self._cache_dir_path = cache_dir_path
    return cache_dir_path


def _write_cache_file(
    self,
    cache_key: str,
    obj_to_write: Any,
    debug: bool = False,
) -> mrsm.SuccessTuple:
    """
    Write a pickle-able object to a cache file.
    """
    from meerschaum.utils.dtypes import get_current_timestamp, json_serialize_value
    now = get_current_timestamp()
    cache_dir_path = self._get_cache_dir_path()
    file_path = cache_dir_path / (cache_key + '.pkl')
    meta_file_path = cache_dir_path / (cache_key + '.meta.json')

    if debug:
        dprint(f"Writing cache file '{file_path}'.")

    metadata = {
        'created': now,
    }

    try:
        with open(file_path, 'wb+') as f:
            pickle.dump(obj_to_write, f)
        with open(meta_file_path, 'w+', encoding='utf-8') as f:
            json.dump(metadata, f, default=json_serialize_value)
    except Exception as e:
        if debug:
            dprint(f"Failed to write cache file:\n{e}")
        return False, f"Failed to write cache file:\n{e}"

    return True, "Success"


def _read_cache_file(
    self,
    cache_key: str,
    debug: bool = False,
) -> Any:
    """
    Read a cache file and return the pickled object.
    Returns `None` if the cache file does not exist or is expired.
    """
    from meerschaum.utils.dtypes import get_current_timestamp
    now = get_current_timestamp()
    cache_dir_path = self._get_cache_dir_path()
    file_path = cache_dir_path / (cache_key + '.pkl')
    meta_file_path = cache_dir_path / (cache_key + '.meta.json')
    local_cache_timeout_seconds = mrsm.get_config(
        'pipes', 'attributes', 'local_cache_timeout_seconds'
    )

    if debug:
        dprint(f"Reading cache file '{file_path}'.")

    if not meta_file_path.exists() or not file_path.exists():
        return None

    try:
        with open(meta_file_path, 'r', encoding='utf-8') as f:
            metadata = json.load(f)
    except Exception as e:
        if debug:
            dprint(f"Failed to read cache metadata file '{meta_file_path}':\n{e}")
        return None

    created_str = metadata.get('created', None)
    created = datetime.fromisoformat(created_str) if created_str else None
    if not created:
        if debug:
            dprint(f"Could not read cache `created` timestamp for '{meta_file_path}'.")
        return None

    try:
        with open(file_path, 'rb') as f:
            obj = pickle.load(f)
    except Exception as e:
        if debug:
            dprint(f"Failed to read cache file:\n{e}")

        return None

    return obj


def _load_cache_files(self, debug: bool = False) -> mrsm.SuccessTuple:
    """
    Load all the existing pickle cache files.
    """
    if not self.cache:
        return True, f"Skip checking for cache for {self}."

    cache_dir_path = self._get_cache_dir_path() 
    cache_keys = [
        filename[:(-1 * len('.pkl'))]
        for filename in os.listdir(cache_dir_path)
        if filename.endswith('.pkl')
    ]
    if not cache_keys:
        if debug:
            dprint(f"No local cache found for {self}.")
        return True, "No cache to load."

    if debug:
        dprint(
            f"Will load {len(cache_keys)} cache file"
            + ('s' if len(cache_keys) != 1 else '')
            + f' into {self}.'
        )

    cache_objs = {
        cache_key: self._read_cache_file(cache_key, debug=debug)
        for cache_key in cache_keys
    }
    cache_patch = {
        in_memory_key: obj
        for cache_key, obj in cache_objs.items()
        if (
            obj is not None
            and (in_memory_key := _get_in_memory_key(cache_key)) not in self.__dict__
        )
    }
    if debug:
        dprint("Loading cache keys:")
        mrsm.pprint(cache_patch)

    self.__dict__.update(cache_patch)
    return True, "Success"


def _clear_cache_key(
    self,
    cache_key: str,
    debug: bool = False,
) -> None:
    """
    Clear a cached value from in-memory and on-disk.
    """
    in_memory_key = _get_in_memory_key(cache_key)
    _ = self.__dict__.pop(in_memory_key, None)

    cache_dir_path = self._get_cache_dir_path()
    file_path = cache_dir_path / (cache_key + '.pkl')
    meta_file_path = cache_dir_path / (cache_key + '.meta.json')

    try:
        file_path.unlink()
    except Exception as e:
        if debug:
            dprint(f"Failed to delete cache file '{file_path}':\n{e}")

    try:
        meta_file_path.unlink()
    except Exception as e:
        if debug:
            dprint(f"Failed to delete meta cache file '{meta_file_path}':{e}")
