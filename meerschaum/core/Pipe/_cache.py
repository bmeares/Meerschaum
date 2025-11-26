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
import shutil
from datetime import datetime, timedelta
from typing import Any, Union, List

import meerschaum as mrsm
from meerschaum.utils.warnings import warn, dprint


def _get_in_memory_key(cache_key: str) -> str:
    """
    Return the in-memory version of a cache key.
    """
    return (
        f"_{cache_key}"
        if not str(cache_key).startswith('_')
        else str(cache_key)
    )


def _get_cache_conn_cache_key(pipe: mrsm.Pipe, cache_key: str) -> str:
    """
    Return the cache key to use in the cache connector.
    """
    ck = pipe.connector_keys.replace(':', '_')
    mk = pipe.metric_key
    lk = str(pipe.location_key)
    return f'.cache:pipes:{ck}:{mk}:{lk}:{cache_key}'


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
    memory_only: bool = False,
    debug: bool = False,
) -> None:
    """
    Cache a value in-memory and (if `Pipe.cache` is `True`) on-disk or to the cache connector.
    """
    if value is None:
        if debug:
            dprint(f"Skip caching '{cache_key}': received value of `None`")
        return

    in_memory_key = _get_in_memory_key(cache_key)
    self.__dict__[in_memory_key] = value
    if memory_only:
        return

    write_success, write_msg = (
        self._write_cache_key(cache_key, value)
        if self.cache
        else (True, "Success")
    )
    if not write_success and debug:
        dprint(f"Failed to cache '{cache_key}':\n{write_msg}")


def _get_cached_value(
    self,
    cache_key: str,
    debug: bool = False,
) -> Any:
    """
    Attempt to retrieve a cached value from in-memory on on-disk.
    """
    in_memory_key = _get_in_memory_key(cache_key)
    if in_memory_key in self.__dict__:
        if debug:
            dprint(f"Return cached key '{cache_key}' from memory.")
        return self.__dict__[in_memory_key]

    if debug:
        dprint(f"Reading cache key '{cache_key}'...")
    return self._read_cache_key(cache_key, debug=debug)


def _invalidate_cache(
    self,
    hard: bool = False,
    debug: bool = False,
) -> mrsm.SuccessTuple:
    """
    Invalidate temporary cache.

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

    self._clear_cache_key('_exists', debug=debug)
    self._clear_cache_key('sync_ts', debug=debug)

    if not hard:
        return True, "Success"

    cache_conn = self._get_cache_connector()
    cache_dir_path = self._get_cache_dir_path()
    cache_keys = self._get_cache_keys(debug=debug)
    for cache_key in cache_keys:
        if cache_keys == 'attributes':
            continue
        self._clear_cache_key(cache_key, debug=debug)

    if cache_conn is None:
        try:
            if cache_dir_path.exists():
                shutil.rmtree(cache_dir_path)
            _ = self.__dict__.pop('_checked_if_cache_dir_exists', None)
        except Exception:
            pass

    return True, "Success"


def _get_cache_dir_path(self, create_if_not_exists: bool = False) -> pathlib.Path:
    """
    Return the path to the cache directory.
    """
    from meerschaum.config.paths import PIPES_CACHE_RESOURCES_PATH, ROOT_DIR_PATH
    cache_dir_path = (
        PIPES_CACHE_RESOURCES_PATH
        / self.instance_keys
        / self.connector_keys
        / self.metric_key
        / str(self.location_key)
    )
    if create_if_not_exists and not cache_dir_path.exists():
        try:
            cache_dir_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            warn(f"Encountered an issue when creating local pipe metadata cache:\n{e}")

    return cache_dir_path


def _write_cache_key(
    self,
    cache_key: str,
    obj_to_write: Any,
    debug: bool = False,
) -> mrsm.SuccessTuple:
    """
    Pickle and write the object to cache.
    """
    cache_connector = self._get_cache_connector()
    if cache_connector is None:
        return self._write_cache_file(cache_key, obj_to_write, debug=debug)

    return self._write_cache_conn_key(cache_key, obj_to_write, debug=debug)


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
    _checked_if_cache_dir_exists = self.__dict__.get('_checked_if_cache_dir_exists', None)
    cache_dir_path = self._get_cache_dir_path(
        create_if_not_exists=(not _checked_if_cache_dir_exists),
    )
    if not _checked_if_cache_dir_exists:
        self._checked_if_cache_dir_exists = True

    file_path = cache_dir_path / (cache_key + '.pkl')
    meta_file_path = cache_dir_path / (cache_key + '.meta.json')
    metadata = {
        'created': now,
    }

    if debug:
        dprint(f"Writing cache file '{file_path}'.")

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


def _write_cache_conn_key(
    self,
    cache_key: str,
    obj_to_write: Any,
    debug: bool = False,
) -> mrsm.SuccessTuple:
    """
    Write the object to the cache connector.
    """
    cache_connector = self._get_cache_connector()
    if cache_connector is None:
        return False, f"No cache connector is set for {self}."

    cache_conn_cache_key = _get_cache_conn_cache_key(self, cache_key)
    local_cache_timeout_seconds = int(mrsm.get_config(
        'pipes', 'attributes', 'local_cache_timeout_seconds'
    ))
    obj_bytes = pickle.dumps(obj_to_write)
    if debug:
        dprint(f"Setting '{cache_conn_cache_key}' on '{cache_connector}'.")

    success = cache_connector.set(
        cache_conn_cache_key,
        obj_bytes,
        ex=local_cache_timeout_seconds,
    )
    if not success:
        return False, f"Failed to set '{cache_conn_cache_key}' on '{cache_connector}'."

    return True, "Success"


def _read_cache_key(
    self,
    cache_key: str,
    debug: bool = False,
) -> Any:
    """
    Read the cache file if the cache connector is None, otherwise read from Valkey.
    """
    cache_connector = self._get_cache_connector()
    if cache_connector is None:
        return self._read_cache_file(cache_key, debug=debug)

    return self._read_cache_conn_key(cache_key, debug=debug)


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

    if not meta_file_path.exists() or not file_path.exists():
        return None

    try:
        if debug:
            dprint(f"Reading cache file '{file_path}'.")

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

    is_expired = (now - created) >= timedelta(seconds=local_cache_timeout_seconds)
    if is_expired:
        self._clear_cache_file(cache_key, debug=debug)
        return None

    try:
        with open(file_path, 'rb') as f:
            obj = pickle.load(f)
    except Exception as e:
        if debug:
            dprint(f"Failed to read cache file:\n{e}")

        return None

    return obj


def _read_cache_conn_key(
    self,
    cache_key: str,
    debug: bool = False,
) -> Any:
    """
    Read a cache key from the cache connector.
    """
    cache_connector = self._get_cache_connector()
    if cache_connector is None:
        return None

    cache_conn_cache_key = _get_cache_conn_cache_key(self, cache_key)
    try:
        obj_bytes = cache_connector.get(cache_conn_cache_key, decode=False)
        if obj_bytes is None:
            return None
        obj = pickle.loads(obj_bytes)
    except Exception as e:
        warn(f"Failed to load '{cache_conn_cache_key}' from '{cache_connector}':\n{e}")
        return None

    return obj


def _load_cache_keys(self, debug: bool = False) -> mrsm.SuccessTuple:
    """
    Discover and load existing cache keys.
    """
    if not self.cache:
        return True, f"Skip checking for cache for {self}."

    cache_connector = self._get_cache_connector()
    if cache_connector is None:
        return self._load_cache_files(debug=debug)

    return self._load_cache_conn_keys(debug=debug)


def _load_cache_files(self, debug: bool = False) -> mrsm.SuccessTuple:
    """
    Load all the existing pickle cache files.
    """
    if not self.cache:
        return True, f"Skip checking for cache for {self}."

    cache_dir_path = self._get_cache_dir_path(create_if_not_exists=True)
    if not cache_dir_path.exists():
        return True, f"No cache directory for {self}."

    cache_keys = self._get_cache_file_keys(debug=debug)
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
        dprint(f"Loading cache keys into {self}:")
        mrsm.pprint(cache_patch)

    self.__dict__.update(cache_patch)
    return True, "Success"


def _load_cache_conn_keys(self, debug: bool = False) -> mrsm.SuccessTuple:
    """
    Discover and load cache keys from the cache connector.
    """
    if not self.cache:
        return True, f"Skip checking for cache for {self}."

    cache_connector = self._get_cache_connector()
    if cache_connector is None:
        return False, f"No cache connector is set for {self}."

    keys = self._get_cache_conn_keys(debug=debug)
    try:
        cache_keys_bytes = {
            key.split(':')[-1]: cache_connector.get(key, decode=False)
            for key in keys
        }
    except Exception as e:
        return False, f"Failed to retrieve cache keys for {self} from '{cache_connector}':\n{e}"

    try:
        cache_keys_objs = {
            cache_key: pickle.loads(obj_bytes)
            for cache_key, obj_bytes in cache_keys_bytes.items()
        }
    except Exception as e:
        return False, f"Failed to de-pickle cache bytes from '{self}':\n{e}"

    cache_patch = {
        in_memory_key: obj
        for cache_key, obj in cache_keys_objs.items()
        if (
            obj is not None
            and (in_memory_key := _get_in_memory_key(cache_key)) not in self.__dict__
        )
    }
    if debug:
        dprint("Loading cache keys into {self}:")
        mrsm.pprint(cache_patch)

    self.__dict__.update(cache_patch)
    return True, "Success"


def _get_cache_keys(self, debug: bool = False) -> List[str]:
    """
    Return a list of existing cache keys.
    """
    cache_connector = self._get_cache_connector()
    if cache_connector is None:
        return self._get_cache_file_keys(debug=debug)

    return self._get_cache_conn_keys(debug=debug)


def _get_cache_file_keys(self, debug: bool = False) -> List[str]:
    """
    Return the cache keys from disk.
    """
    cache_dir_path = self._get_cache_dir_path() 
    if not cache_dir_path.exists():
        if debug:
            dprint(f"Cache path '{cache_dir_path}' does not exist; no keys to return.")
        return []

    if debug:
        dprint(f"Listing cache files from '{cache_dir_path}'.")

    return [
        filename[:(-1 * len('.pkl'))]
        for filename in os.listdir(cache_dir_path)
        if filename.endswith('.pkl')
    ]


def _get_cache_conn_keys(self, debug: bool = False) -> List[str]:
    """
    Return the cache keys from the cache connector.
    """
    cache_connector = self._get_cache_connector()
    if cache_connector is None:
        return []

    keys_prefix = _get_cache_conn_cache_key(self, '')

    try:
        return [
            key.decode('utf-8').split(':')[-1]
            for key in cache_connector.client.keys(keys_prefix + '*')
        ]
    except Exception as e:
        warn(f"Failed to get cache keys for {self} from '{cache_connector}':\n{e}")
        return []


def _clear_cache_key(
    self,
    cache_key: str,
    debug: bool = False,
) -> None:
    """
    Clear a cached value from in-memory and on-disk / from Valkey.
    """
    in_memory_key = _get_in_memory_key(cache_key)
    _ = self.__dict__.pop(in_memory_key, None)

    cache_connector = self._get_cache_connector()
    if cache_connector is None:
        self._clear_cache_file(cache_key, debug=debug)
    else:
        self._clear_cache_conn_key(cache_key, debug=debug)


def _clear_cache_file(
    self,
    cache_key: str,
    debug: bool = False,
) -> None:
    """
    Clear a cached value from on-disk.
    """
    cache_dir_path = self._get_cache_dir_path()
    file_path = cache_dir_path / (cache_key + '.pkl')
    meta_file_path = cache_dir_path / (cache_key + '.meta.json')

    try:
        if file_path.exists():
            file_path.unlink()
    except Exception as e:
        if debug:
            dprint(f"Failed to delete cache file '{file_path}':\n{e}")

    try:
        if meta_file_path.exists():
            meta_file_path.unlink()
    except Exception as e:
        if debug:
            dprint(f"Failed to delete meta cache file '{meta_file_path}':{e}")


def _clear_cache_conn_key(
    self,
    cache_key: str,
    debug: bool = False,
) -> None:
    """
    Clear a cached value from Valkey.
    """
    cache_connector = self._get_cache_connector()
    if cache_connector is None:
        return

    cache_conn_cache_key = _get_cache_conn_cache_key(self, cache_key)
    try:
        cache_connector.client.unlink(cache_conn_cache_key)
    except Exception as e:
        warn(f"Failed to clear cache key '{cache_key}' from '{cache_connector}':\n{e}")
