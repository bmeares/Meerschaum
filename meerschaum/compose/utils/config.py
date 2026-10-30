#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Read the compose configuration (`meerschaum-compose`, through `_core`).
"""

import os
import pathlib
import json

import meerschaum as mrsm
from meerschaum.utils.typing import Optional, Union, Dict, Any, List
from meerschaum.utils.warnings import warn, info
from meerschaum.utils.misc import items_str
from meerschaum.utils.dtypes import json_serialize_value

COMPOSE_KEYS = [
    'root_dir',
    'plugins_dir',
    'plugins',
    'sync',
    'config',
    'connectors',
    'instance',
    'environment',
    'project_name',
    'pipes',
    'pipelines',
    'jobs',
    'isolation',
    'daemon',
]
DEFAULT_COMPOSE_FILE_CANDIDATES = ['mrsm-compose.yaml', 'mrsm-compose.yml']
CONFIG_METADATA: Dict[str, Any] = {}


def infer_compose_file_path(file: Optional[pathlib.Path] = None) -> Union[pathlib.Path, None]:
    """
    Return the file path to a valid compose file or `None` if nothing exists.

    Parameters
    ----------
    file: Optional[pathlib.Path], default None
        If a specific path is provided, only check if it exists.
        Otherwise go through the candidates and see if any exist.

    Returns
    -------
    The file path to a compose file if it exists, else `None`.
    """
    if file is not None:
        return file.resolve() if file.exists() else None
    found_candidates = []
    for candidate_name in DEFAULT_COMPOSE_FILE_CANDIDATES:
        candidate_path = pathlib.Path(candidate_name).resolve()
        if candidate_path.exists():
            found_candidates.append(candidate_path)
    if len(found_candidates) > 1:
        warn(f"Found multiple YAML files. Compose will use this file:\n{found_candidates[0]}.")
    elif len(found_candidates) == 0:
        return None
    return found_candidates[0].resolve()


def _plain(value: Any) -> Any:
    """The configuration as plain JSON-like data (paths become strings) for `_core`."""
    return json.loads(json.dumps(value, default=str))


def _emit(messages: List[Any]) -> None:
    """Print the messages `_core` collected (`warn(..., stack=False)` or `info()`)."""
    for kind, text in messages:
        if kind == 'warn':
            warn(text, stack=False)
        else:
            info(text)


def _as_paths(compose_config: Dict[str, Any]) -> Dict[str, Any]:
    """`__file__`, `root_dir` and `plugins_dir` are `pathlib.Path`s in the dictionary."""
    for key in ('__file__', 'root_dir'):
        if isinstance(compose_config.get(key, None), str):
            compose_config[key] = pathlib.Path(compose_config[key])
    if isinstance(compose_config.get('plugins_dir', None), list):
        compose_config['plugins_dir'] = [
            pathlib.Path(path) if isinstance(path, str) else path
            for path in compose_config['plugins_dir']
        ]
    return compose_config


def read_compose_config(
    compose_file_path: pathlib.Path,
    env_file: Optional[pathlib.Path] = None,
    isolated: bool = False,
    debug: bool = False,
) -> Union[Dict[str, Any], None]:
    """
    Read the compose file and only include the top-level keys from `COMPOSE_KEYS`.

    Parameters
    ----------
    compose_file_path: pathlib.Path
        The file path to the compose file.
        This file must be verified to exist.

    env_file: Optional[pathlib.Path], default None
        Use a specific environment file.
        Defaults to `./.env`.

    Returns
    -------
    The contents of the compose YAML file as a dictionary.
    """
    from meerschaum import _core
    config, env, messages = _core.compose_read_config(
        str(compose_file_path),
        str(env_file) if env_file is not None else None,
        isolated,
    )
    ### Undefined variables are blanked in the process environment, as v4 did.
    for key, value in env.items():
        if key not in os.environ:
            os.environ[key] = value
    _emit(messages)
    return _as_paths(config)


def ensure_dir_keys(compose_config: Dict[str, Any]) -> None:
    """
    Add the keys `root_dir` and `plugins_dir`.
    """
    compose_config['root_dir'] = get_dir_paths(compose_config, 'root')[0]
    compose_config['plugins_dir'] = get_dir_paths(compose_config, 'plugins')


def replace_config_paths(
    config: Dict[str, Any],
    path: pathlib.Path,
    pattern: str,
) -> Dict[str, Any]:
    """
    Recursively replace the given pattern string with the path string.
    """
    if isinstance(config, dict):
        for key, value in config.items():
            config[key] = replace_config_paths(value, path, pattern)
    elif isinstance(config, list):
        for i, item in enumerate(config):
            config[i] = replace_config_paths(item, path, pattern)
    elif isinstance(config, str):
        return config.replace(pattern, path.as_posix())

    return config


def get_dir_paths(compose_config: Dict[str, Any], dir_name: str) -> List[pathlib.Path]:
    """
    Return the absolute paths for the configured plugins directory.
    Throw a warning if multiple values are configured.

    Parameters
    ----------
    compose_config: pathlib.Path
        The compose configuration dictionary.

    dir_name: str
        The name of the directory to be resolved.
        Values include 'root' and 'plugins'.

    Returns
    -------
    The absolute path to the configured directory.
    """
    from meerschaum import _core
    paths, messages = _core.compose_dir_paths(_plain(compose_config), dir_name)
    _emit(messages)
    return [pathlib.Path(path) for path in paths]


def get_env_dict(compose_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return the environment dictionary every command of the project runs under.
    """
    from meerschaum import _core
    return _core.compose_env_dict(_plain(compose_config))


def write_patch(compose_config: Dict[str, Any], debug: bool = False) -> None:
    """
    Write the patch files to the configured patch directory.
    """
    from meerschaum.config._edit import write_config
    root_dir_path = compose_config['root_dir']
    patch_dir_path = root_dir_path / 'permanent_patch_config'
    patch_dir_path.mkdir(exist_ok=True)
    write_config(compose_config.get('config', {}), patch_dir_path, debug=debug)


def init_root(compose_config: Dict[str, Any], debug: bool = False) -> bool:
    """
    Initialize the Meerschaum root directory.
    """
    from meerschaum.compose.utils import run_mrsm_command
    from meerschaum.compose.utils.plugins import get_installed_plugins
    root_dir_path = compose_config['root_dir']
    fresh = False
    if not root_dir_path.exists():
        fresh = True
        root_dir_path.mkdir(exist_ok=True)
        info(
            "Initializing Meerschaum root directory:\n    "
            f"{root_dir_path}\n    "
            "This should only take a few seconds..."
        )

    success, message = run_mrsm_command(
        ['show', 'version', '--no-daemon'],
        compose_config,
        capture_output=True,
        debug=debug,
    )

    if fresh:
        if get_installed_plugins(compose_config, debug=debug):
            info("Installing required packages for plugins...")
            run_mrsm_command(
                ['install', 'required'],
                compose_config,
                capture_output=False,
                debug=debug,
            )

    ### Update the cache after building the in-memory config.
    if config_has_changed(compose_config):
        write_config_cache(compose_config)

    return success


def init_env(
    compose_file_path: pathlib.Path,
    env_file: Optional[pathlib.Path] = None
) -> None:
    """
    Initialize the local environment from the dotfile.

    Parameters
    ----------
    env_file: Optional[pathlib.Path], default None
        The path to the the environment dotfile.
        Infer `.env` if `env_file` is `None`.
    """
    from meerschaum import _core
    env, messages = _core.compose_init_env(
        str(compose_file_path),
        str(env_file) if env_file is not None else None,
    )
    for key, value in env.items():
        os.environ.setdefault(key, value)
    _emit(messages)


def get_config_cache_path(compose_config: Dict[str, Any]) -> pathlib.Path:
    """
    Return the file path to the config cache file.
    """
    root_dir_path = compose_config['root_dir']
    return root_dir_path / '.compose-cache.pkl'


def write_config_cache(compose_config: Dict[str, Any]) -> None:
    """
    Write the current compose configuration to a cache file.
    """
    config_cache_path = get_config_cache_path(compose_config)

    config_cache_path.write_text(hash_config(compose_config), encoding='utf-8')


def read_config_cache(compose_config: Dict[str, Any]) -> Union[int, None]:
    """
    Read and return the cached config metadata.
    If no cache exists, return None.
    """
    config_cache_path = get_config_cache_path(compose_config)
    if not config_cache_path.exists():
        return None
    try:
        config_cache = config_cache_path.read_text(encoding='utf-8').strip()
    except (OSError, UnicodeError):
        return None
    return (
        config_cache
        if len(config_cache) == 64 and all(c in '0123456789abcdef' for c in config_cache)
        else None
    )


def config_has_changed(compose_config: Dict[str, Any]) -> bool:
    """
    Check if the in-memory configuration is the same as the last cached version.
    """
    metadata_key = str(get_config_cache_path(compose_config).resolve())
    hashed_config = hash_config(compose_config)
    metadata = CONFIG_METADATA.get(metadata_key, None)
    if metadata is not None and metadata[0] == hashed_config:
        return metadata[1]
    config_cache = read_config_cache(compose_config)
    has_changed = (config_cache != hashed_config)
    CONFIG_METADATA[metadata_key] = (hashed_config, has_changed)
    return has_changed


def hash_config(compose_config: Dict[str, Any]) -> str:
    """
    Compute the hash value for the configuration dictionary.
    """
    import hashlib
    return hashlib.sha256(
        bytes(
            json.dumps(compose_config, sort_keys=True, default=str),
            encoding = 'utf-8',
        )
    ).hexdigest()
