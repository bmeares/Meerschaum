#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Register a Pipe object
"""

from meerschaum.utils.typing import SuccessTuple, Any

def register(
        self,
        debug: bool = False,
        **kw: Any
    ) -> SuccessTuple:
    """
    Register a new Pipe along with its attributes.

    Parameters
    ----------
    debug: bool, default False
        Verbosity toggle.

    kw: Any
        Keyword arguments to pass to `instance_connector.register_pipe()`.

    Returns
    -------
    A `SuccessTuple` of success, message.
    """
    if self.temporary:
        return False, "Cannot register pipes created with `temporary=True` (read-only)."

    from meerschaum.utils.formatting import get_console
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin, custom_types
    from meerschaum.config._patch import apply_patch_to_config

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
            with Venv(get_connector_plugin(_conn), debug=debug):
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
            self.parameters = apply_patch_to_config(params, self.parameters)

    if not self.parameters:
        cols = self.columns if self.columns else {'datetime': None, 'id': None}
        self.parameters = {
            'columns': cols,
        }

    with Venv(get_connector_plugin(self.instance_connector)):
        return self.instance_connector.register_pipe(self, debug=debug, **kw)
