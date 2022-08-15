#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Poll database and API connections.
"""

from meerschaum.utils.typing import InstanceConnector, Union
from meerschaum.utils.misc import timed_input

def retry_connect(
        connector: Union[InstanceConnector, None] = None,
        max_retries: int = 40,
        retry_wait: int = 3,
        workers: int = 1,
        warn: bool = True,
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
    from meerschaum import get_connector
    from meerschaum.utils.packages import attempt_import
    from meerschaum.utils.sql import test_queries
    from functools import partial
    import time

    ### Get default connector if None is provided.
    if connector is None:
        connector = get_connector()

    if connector.type not in ('sql', 'api'):
        return None

    retries = 0
    connected, chaining_status = False, None
    while retries < max_retries:
        if debug:
            dprint(f"Trying to connect to '{connector}'...")
            dprint(f"Attempt ({retries + 1} / {max_retries})")

        if connector.type == 'sql':

            def _connect(_connector):
                ### Test queries like `SELECT 1`.
                connect_query = test_queries.get(connector.flavor, test_queries['default'])
                if _connector.exec(connect_query) is None:
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
                                stack = False
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
            if debug:
                dprint("Connection established!")
            return True

        if warn:
            _warn(
                f"Connection failed. Press [Enter] to retry or wait {retry_wait} seconds.",
                stack = False
            )
            info(
                f"To quit, press CTRL-C, then 'q' + Enter for each worker" +
                (f" ({workers})." if workers is not None else ".")
            )
        try:
            if retry_wait > 0:
                text = timed_input(retry_wait)
                if text in ('q', 'quit', 'pass', 'exit', 'stop'):
                    return None
        except KeyboardInterrupt:
            return None
        retries += 1

    return False

