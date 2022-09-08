#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register a Pipe object
"""

from meerschaum.utils.typing import SuccessTuple

def register(
        self,
        debug: bool = False
    ) -> SuccessTuple:
    """
    Register a new Pipe along with its attributes.

    Parameters
    ----------
    debug: bool, default False
        Verbosity toggle.

    Returns
    -------
    A `SuccessTuple` of success, message.

    """
    from meerschaum.connectors import custom_types
    from meerschaum.utils.formatting import get_console
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        try:
            _conn = self.connector
        except Exception as e:
            _conn = None

    if (
        _conn is not None
        and
        (_conn.type == 'plugin' or _conn.type in custom_types)
        and
        getattr(_conn, 'register', None) is not None
    ):
        try:
            params = self.connector.register(self)
        except Exception as e:
            get_console().print_exception()
            params = None
        params = {} if params is None else params
        if not isinstance(params, dict):
            from meerschaum.utils.warnings import warn
            warn(
                f"Invalid parameters returned from `register()` in connector {self.connector}:\n"
                + f"{params}"
            )
        else:
            self.parameters = params

    if not self.parameters:
        cols = self.columns if self.columns else {'datetime': None, 'id': None}
        self.parameters = {
            'columns': cols,
        }

    return self.instance_connector.register_pipe(self, debug=debug)
