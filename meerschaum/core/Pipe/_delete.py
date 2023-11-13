#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Delete a Pipe's contents and registration
"""

from meerschaum.utils.typing import SuccessTuple

def delete(
        self,
        drop: bool = True,
        debug: bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Call the Pipe's instance connector's `delete_pipe()` method.

    Parameters
    ----------
    drop: bool, default True
        If `True`, drop the pipes' target table.

    debug : bool, default False
        Verbosity toggle.

    Returns
    -------
    A `SuccessTuple` of success (`bool`), message (`str`).

    """
    import os, pathlib
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin

    if self.temporary:
        return (
            False,
            "Cannot delete pipes created with `temporary=True` (read-only). "
            + "You may want to call `pipe.drop()` instead."
        )

    if self.cache_pipe is not None:
        _drop_cache_tuple = self.cache_pipe.drop(debug=debug, **kw)
        if not _drop_cache_tuple[0]:
            warn(_drop_cache_tuple[1])
        if getattr(self.cache_connector, 'flavor', None) == 'sqlite':
            _cache_db_path = pathlib.Path(self.cache_connector.database)
            try:
                os.remove(_cache_db_path)
            except Exception as e:
                warn(f"Could not delete cache file '{_cache_db_path}' for {self}:\n{e}")

    if drop:
        drop_success, drop_msg = self.drop(debug=debug)
        if not drop_success:
            warn(f"Failed to drop {self}:\n{drop_msg}")

    with Venv(get_connector_plugin(self.instance_connector)):
        result = self.instance_connector.delete_pipe(self, debug=debug, **kw)

    if not isinstance(result, tuple):
        return False, f"Received an unexpected result from '{self.instance_connector}': {result}"

    if result[0]:
        to_delete = ['_id']
        for member in to_delete:
            if member in self.__dict__:
                del self.__dict__[member]
    return result
