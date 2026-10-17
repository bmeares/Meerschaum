#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
"""
Import the config yaml file
"""

from __future__ import annotations
import pathlib

from meerschaum.utils.typing import Optional, Dict, Any, List, Tuple, Union
from meerschaum._internal.static import STATIC_CONFIG
from meerschaum import _core


def read_config(
    directory: Union[pathlib.Path, str, None] = None,
    keys: Optional[List[str]] = None,
    write_missing: bool = True,
    substitute: bool = True,
    with_filenames: bool = False,
    raise_parsing_errors: bool = False,
) -> Union[Dict[str, Any], Tuple[Dict[str, Any], List[str]]]:
    """
    Read the configuration directory.

    Parameters
    ----------
    directory: Union[pathlib.Path, str, None], default None
        The directory with configuration files (.json and .yaml).

    keys: Optional[List[str]], default None
        Which configuration files to read.

    write_missing: bool, default True
        If a keyfile does not exist but is defined in the default configuration,
        write the file to disk.

    substitute: bool, default True
        Replace `MRSM{}` syntax with configuration values.

    with_filename: bool, default False
        If `True`, return a tuple of the configuration dictionary with a list of read filenames.

    raise_parsing_errors: bool, default False
        If `True`, re-raise parsing exceptions.

    Examples
    --------
    >>> read_config(keys=['meerschaum'], with_filename=True)
    >>> ({...}, ['meerschaum.yaml'])
    """
    _rust = _core.read_config
    return _rust(
        directory=directory,
        keys=keys,
        write_missing=write_missing,
        substitute=substitute,
        with_filenames=with_filenames,
        raise_parsing_errors=raise_parsing_errors,
    )


def search_and_substitute_config(
    config: Dict[str, Any],
    leading_key: str = "MRSM",
    delimiter: str = ":",
    begin_key: str = "{",
    end_key: str = "}",
    literal_key: str = '!',
    keep_symlinks: bool = True,
) -> Dict[str, Any]:
    """
    Search the config for Meerschaum substitution syntax and substite with value of keys.

    Parameters
    ----------
    config: Dict[str, Any]
        The Meerschaum configuration dictionary to search through.

    leading_key: str, default 'MRSM'
        The string with which to start the search.

    begin_key: str, default '{'
        The string to start the keys list.

    end_key: str, default '}'
        The string to end the keys list.

    literal_key: str, default '!'
        The string to force an literal interpretation of a value.
        When the string is isolated, a literal interpreation is assumed and the surrounding
        quotes are replaced.
        
        E.g. Suppose a:b:c produces a dictionary {'d': 1}.
        - 'MRSM{a:b:c}'    => {'d': 1}        : isolated
        - ' MRSM{a:b:c} '  => ' "{\'d\': 1}"' : not isolated
        - ' MRSM{!a:b:c} ' => ' {"d": 1}'     : literal

    keep_symlinks: bool, default True
        If True, include the symlinks under the top-level key '_symlinks' (never written to a file).
        Defaults to True.
        
        Example:

        ```
        MRSM{meerschaum:connectors:main:host} => cf['meerschaum']['connectors']['main']['host']
        ``` 

    Returns
    -------
    The configuration dictionary with `MRSM{}` symlinks replaced with
    the values from the current configuration.
    """
    _rust = _core.search_and_substitute_config
    return _rust(
        config,
        leading_key=leading_key,
        delimiter=delimiter,
        begin_key=begin_key,
        end_key=end_key,
        literal_key=literal_key,
        keep_symlinks=keep_symlinks,
    )


def revert_symlinks_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Given a configuration dictionary, re-apply the values from the
    accompanying `_symlinks` dictionary.

    Parameters
    ----------
    config: Dict[str, Any]
        The configuration dictionary containing a `_symlinks` dictionary.

    Returns
    -------
    A configuration dictionary with `_symlinks` re-applied.
    """
    _rust = _core.revert_symlinks_config
    return _rust(config)


def get_possible_keys() -> List[str]:
    """
    Return a list of possible top-level keys.
    """
    import os
    import meerschaum.config.paths as paths
    from meerschaum.config._default import default_config
    keys = set()
    for key in default_config:
        keys.add(key)
    for filename in os.listdir(paths.CONFIG_DIR_PATH):
        keys.add('.'.join(filename.split('.')[:-1]))
    return sorted(list(keys))


def get_keyfile_path(
    key: str,
    create_new: bool = False,
    directory: Union[pathlib.Path, str, None] = None,
) -> Union[pathlib.Path, None]:
    """Determine a key's file path."""
    _rust = _core.get_keyfile_path
    return _rust(key, create_new=create_new, directory=directory)
