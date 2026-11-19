#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Poll database and API connections.
"""

from meerschaum.utils.typing import InstanceConnector, Union, Optional, Dict, Any

def retry_connect(
    connector: Union[InstanceConnector, None] = None,
    max_retries: int = 50,
    retry_wait: int = 3,
    workers: int = 1,
    warn: bool = True,
    print_on_connect: bool = False,
    enforce_chaining: bool = True,
    enforce_login: bool = True,
    debug: bool = False,
) -> bool:
    """
    Keep trying to connect to the database.

    Parameters
    ----------
    connector: Union[InstanceConnector, None], default None
        The connector to the instance.

    max_retries: int, default 40
        How many time to try connecting.

    retry_wait: int, default 3
        The number of seconds between retries.

    workers: int, default 1
        How many worker thread connections to make.

    warn: bool, default True
        If `True`, print a warning in case the connection fails.

    print_on_connect: bool, default False
        If `True`, print a message when a successful connection is established.

    enforce_chaining: bool, default True
        If `False`, ignore the configured chaining option.

    enforce_login: bool, default True
        If `False`, ignore an invalid login.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    Whether a connection could be made.

    Notes
    -----
    `sql` (every flavor with a native driver), `valkey` and `api` connectors are retried in
    this process by the Rust core, with no child process, temporary file or Python package.
    Any other connector (a plugin's, or one without a usable driver) is retried in a child
    Python process.

    """
    target = _native_target(connector, debug=debug)
    if target is not None:
        from meerschaum import _core
        return bool(_core.retry_connect_native(
            target,
            str(connector),
            int(max_retries),
            float(retry_wait),
            str(retry_wait),
            int(workers or 0),
            bool(warn),
            bool(print_on_connect),
            bool(enforce_chaining),
            bool(enforce_login),
            bool(warn) and connector.__dict__.get('_warn', None) is not False,
            bool(debug),
        ))

    import json
    import os
    import tempfile
    from meerschaum.utils.venv import venv_exec

    kw = {
        'connector_meta': connector.meta,
        'max_retries': max_retries,
        'retry_wait': retry_wait,
        'workers': workers,
        'warn': warn,
        'print_on_connect': print_on_connect,
        'enforce_chaining': enforce_chaining,
        'enforce_login': enforce_login,
        'debug': debug,
    }

    ### The connector's attributes (its password among them) travel in an owner-only file the
    ### child reads and removes, never on its command line, where `ps` would show them (5.1).
    fd, kw_path = tempfile.mkstemp(prefix='mrsm-retry-', suffix='.json')
    try:
        with os.fdopen(fd, 'w', encoding='utf-8') as kw_file:
            json.dump(kw, kw_file)
        code = (
            "import sys, json, os\n"
            ### The child imports the helper (no source text, no `dill`: the base wheel has no
            ### runtime dependencies).
            + "from meerschaum.connectors.poll import _wrap_retry_connect\n\n"
            + f"_kw_path = {kw_path!r}\n"
            + "with open(_kw_path, 'r', encoding='utf-8') as _kw_file:\n"
            + "    kw = json.load(_kw_file)\n"
            + "os.remove(_kw_path)\n"
            + "success = _wrap_retry_connect(**kw)\n"
            + "sys.exit((0 if success else 1))"
        )
        return venv_exec(code, venv=None, debug=debug)
    finally:
        try:
            os.remove(kw_path)
        except FileNotFoundError:
            pass


def _native_target(connector: Any, debug: bool = False) -> Optional[Any]:
    """
    The `meerschaum._core` connector the Rust retry loop runs against, or `None` when the
    connector is not one the core implements (a plugin's connector) or its driver cannot be
    built here (a flavor whose native library is missing); the reason is a debug message.
    """
    def _fallback(reason: str) -> None:
        if debug:
            from meerschaum.utils.debug import dprint
            dprint(f"`retry_connect()` keeps to a child process for '{connector}': {reason}.")

    connector_type = getattr(connector, 'type', None)
    try:
        from meerschaum import _core
        if not hasattr(_core, 'retry_connect_native'):
            _fallback("the extension has no native retry loop")
            return None
        if connector_type == 'sql':
            from meerschaum.connectors.sql import SQLConnector
            if isinstance(connector, SQLConnector):
                ### Without the default-schema lookup, which would connect (and fail) first.
                return connector._core_connector(discover_schema=False)
        elif connector_type == 'valkey':
            from meerschaum.connectors.valkey import ValkeyConnector
            if isinstance(connector, ValkeyConnector):
                return connector._rust_client()
        elif connector_type == 'api':
            from meerschaum.connectors.api import APIConnector
            if isinstance(connector, APIConnector):
                return connector._rust_client()
    except Exception as e:
        _fallback(str(e))
        return None
    _fallback("the core does not implement this connector")
    return None


def _wrap_retry_connect(
    connector_meta: Dict[str, Any],
    max_retries: int = 50,
    retry_wait: int = 3,
    workers: int = 1,
    print_on_connect: bool = False,
    warn: bool = True,
    enforce_chaining: bool = True,
    enforce_login: bool = True,
    debug: bool = False,
) -> bool:
    """
    Keep trying to connect to the database.

    Parameters
    ----------
    connector_keys: Optional[str], default None
        The keys of the connector to the instance.

    max_retries: int, default 40
        How many time to try connecting.

    retry_wait: int, default 3
        The number of seconds between retries.

    workers: int, default 1
        How many worker thread connections to make.

    warn: bool, default True
        If `True`, print a warning in case the connection fails.

    print_on_connect: bool, default False
        If `True`, print a message when a successful connection is established.

    enforce_chaining: bool, default True
        If `False`, ignore the configured chaining option.

    enforce_login: bool, default True
        If `False`, ignore an invalid login.

    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    Whether a connection could be made.

    """
    from meerschaum.utils.warnings import warn as _warn, error, info
    from meerschaum.utils.debug import dprint
    from meerschaum.connectors import instance_types, get_connector
    from meerschaum.connectors.parse import parse_instance_keys
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.misc import timed_input
    from functools import partial
    import platform
    import time

    connector = get_connector(**connector_meta)

    if not hasattr(connector, 'test_connection'):
        return True

    retries = 0
    connected, chaining_status = False, None
    while retries < max_retries:
        if debug:
            dprint(f"Trying to connect to '{connector}'...")
            dprint(f"Attempt ({retries + 1} / {max_retries})")

        if connector.type not in ('sql', 'api'):
            try:
                connected = connector.test_connection()
            except Exception as e:
                if warn:
                    print(e)
                connected = False

        elif connector.type == 'sql':

            def _connect(_connector):
                ### `SELECT 1` through the Rust connector (no SQLAlchemy engine).
                if not _connector.test_connection(max_retries=1, warn=warn):
                    raise Exception("Failed to connect.")

            try:
                _connect(connector)
                connected = True
            except Exception as e:
                if warn:
                    print(e)
                connected = False

        elif connector.type == 'api':
            ### If the remote instance does not allow chaining, don't even try logging in.
            if not isinstance(chaining_status, bool):
                chaining_status = connector.get_chaining_status(debug=debug)
                if chaining_status is None:
                    connected = None
                elif chaining_status is False:
                    if enforce_chaining:
                        if warn:
                            _warn(
                                f"Meerschaum instance '{connector}' does not allow chaining " +
                                "and cannot be used as the parent for this instance.",
                                stack=False,
                            )
                        return False

                    ### Allow is the option to ignore chaining status.
                    chaining_status = True

            if chaining_status:
                connected = (
                    connector.login(warn=warn, debug=debug)[0]
                ) if enforce_login else True

                if not connected and warn:
                    _warn(f"Unable to login to '{connector}'!", stack=False)

        if connected:
            if print_on_connect:
                info(f"Connected to '{connector}'.")
            return True

        if warn:
            _warn(
                f"Connection to '{connector}' failed.\n    "
                + f"Press [Enter] to retry or wait {retry_wait} seconds.",
                stack = False
            )
            if workers and workers > 1:
                info(
                    f"To quit, press CTRL-C, then 'q' + [Enter] for each worker"
                    + f" ({workers})."
                )
            info(f"Failed connection attempt ({retries + 1} / {max_retries})")

        try:
            if retry_wait > 0:
                if platform.system() != 'Windows':
                    text = timed_input(retry_wait)
                    if text in ('q', 'quit', 'pass', 'exit', 'stop'):
                        return None
                else:
                    time.sleep(retry_wait)
        except KeyboardInterrupt:
            return None
        retries += 1

    return False
