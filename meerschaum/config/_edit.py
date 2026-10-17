#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Functions for editing the configuration file
"""

from __future__ import annotations

import pathlib
from meerschaum.utils.typing import Optional, Any, SuccessTuple, Dict, List, Union
from meerschaum import _core


def edit_config(
    keys: Optional[List[str]] = None,
    params: Optional[Dict[str, Any]] = None,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """Edit the configuration files."""
    import meerschaum.config.paths as paths
    from meerschaum.config import get_config, _config
    from meerschaum.config._read_config import get_keyfile_path, read_config, revert_symlinks_config
    from meerschaum.config._backup import backup_config_key, restore_config_backup
    from meerschaum._internal.static import STATIC_CONFIG
    from meerschaum.utils.packages import reload_meerschaum
    from meerschaum.utils.misc import edit_file
    from meerschaum.utils.warnings import warn, info
    from meerschaum.utils.prompt import prompt, yes_no

    if keys is None:
        keys = []

    symlinks_key = STATIC_CONFIG['config']['symlinks_key']
    def _edit_key(key: str):
        new_key_config = None
        ### Snapshot the pristine file before any edits or default-merging so it
        ### can be rolled back if the edit corrupts it.
        backup_path = backup_config_key(key, debug=debug)
        while True:
            ### If defined in default, create the config file.
            config = _config()
            symlinks_key_config = config.get(symlinks_key, {}).get(key, {})
            key_config = revert_symlinks_config({
                key: config.pop(key, {}),
                symlinks_key: {key: symlinks_key_config},
            })
            keyfile_path = get_keyfile_path(key, create_new=True)
            get_config(key, write_missing=True, warn=False)

            edit_file(get_keyfile_path(key, create_new=True))

            ### TODO: verify that the file is valid. Retry if not.
            try:
                new_key_config = read_config(
                    paths.CONFIG_DIR_PATH,
                    [key],
                    write_missing=False,
                    raise_parsing_errors=True,
                )
            except Exception:
                if key_config:
                    _config()[key] = key_config
                warn(f"Could not parse key '{key}'.", stack=False)
                if backup_path is not None and yes_no(
                    f"Restore '{key}' from the backup taken before editing?",
                    default='n',
                ):
                    restore_success, restore_msg = restore_config_backup(
                        key, backup_path=backup_path, reload=False, debug=debug,
                    )
                    if restore_success:
                        info(restore_msg)
                        new_key_config = read_config(
                            paths.CONFIG_DIR_PATH,
                            [key],
                            write_missing=False,
                            raise_parsing_errors=False,
                        )
                        break
                    warn(restore_msg, stack=False)
                _ = prompt(f"Press [Enter] to edit '{keyfile_path}', [CTRL+C] to exit.")
                continue

            if new_key_config:
                break

    try:
        for k in keys:
            _edit_key(k)
    except KeyboardInterrupt:
        return False, ""

    reload_meerschaum(debug=debug)
    return (True, "Success")


def write_config(
    config_dict: Optional[Dict[str, Any]] = None,
    directory: Union[str, pathlib.Path, None] = None,
    debug: bool = False,
    **kw: Any
) -> bool:
    """Write YAML and JSON files to the configuration directory.

    Parameters
    ----------
    config_dict: Optional[Dict[str, Any]], default None
        A dictionary of keys to dictionaries of configuration.
        Each key corresponds to a .yaml or .json config file.
        Writing config to a directory with different keys
        does not affect existing keys in that directory.
        If not provided, use the currently loaded config dictionary.

    directory: Union[str, pathlib.Path, None], default None
        The directory to which the keys are written.
        If not provided, use the default config path (`~/.config/meerschaum/config/`).

    Returns
    -------
    A bool indicating success.

    """
    _rust = _core.write_config
    return _rust(config_dict, directory=directory, debug=debug)

def general_write_yaml_config(
    files: Optional[Dict[pathlib.Path, Dict[str, Any]]] = None,
    debug: bool = False
):
    """
    Write configuration dictionaries to file paths with optional headers.

    Parameters
    ----------
    files: Optional[Dict[str, pathlib.Path]], default None
        Dictionary of paths -> dict or tuple of format (dict, header).
        If item is a tuple, the header will be written at the top of the file.
    """

    from meerschaum.utils.debug import dprint

    if files is None:
        files = {}

    for fp, value in files.items():
        config = value
        header = None
        if isinstance(value, tuple):
            config, header = value
        path = pathlib.Path(fp)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch(exist_ok=True)
        with open(path, 'w+', encoding='utf-8') as f:
            if header is not None:
                if debug:
                    dprint(f"Header detected, writing to {path}...")
                f.write(header)
            if isinstance(config, str):
                if debug:
                    dprint(f"Config is a string. Writing to {path}...")
                f.write(config)
            elif isinstance(config, dict):
                if debug:
                    dprint(f"Config is a dict. Writing to {path}...")
                from meerschaum.utils.yaml import yaml
                yaml.dump(config, stream=f, sort_keys=False)

    return True

def general_edit_config(
    action: Optional[List[str]] = None,
    files: Optional[Dict[str, Union[str, pathlib.Path]]] = None,
    default: Optional[str] = None,
    debug: bool = False
) -> SuccessTuple:
    """Prompt the user to edit any config files."""
    if default is None:
        raise Exception("Provide a default choice for which file to edit")
    from meerschaum.utils.misc import edit_file

    if files is None:
        files = {}
    if action is None:
        action = []
    file_to_edit = files[default]
    if len(action) > 1 and action[1] in files:
        file_to_edit = files[action[1]]

    edit_file(file_to_edit)

    return True, "Success"

def write_default_config(
    debug: bool = False,
    **kw
):
    """Write the default configuration files."""
    import meerschaum.config.paths as paths
    from meerschaum.config._default import default_config
    return write_config(default_config, directory=paths.DEFAULT_CONFIG_DIR_PATH)
