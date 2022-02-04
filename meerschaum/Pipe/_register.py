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
    ):
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
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        try:
            _conn = self.connector
        except Exception as e:
            _conn = None

    if _conn is not None and _conn.type == 'plugin' and _conn.register is not None:
        params = self.connector.register(self)
        params = {} if params is None else params
        if not isinstance(params, dict):
            from meerschaum.utils.warnings import warn
            warn(
                f"Invalid parameters returned from `register()` in plugin {self.connector}:\n"
                + f"{params}"
            )
        else:
            self.parameters = params

    if not self.parameters:
        self.parameters = {
            'columns': self.columns,
        }

    return self.instance_connector.register_pipe(self, debug=debug)

