#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Delete a Pipe's contents and registration
"""

from meerschaum.utils.typing import SuccessTuple

def delete(
        self,
        debug: bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Call the Pipe's instance connector's `delete_pipe()` method.

    Parameters
    ----------
    debug : bool, default False:
        Verbosity toggle.

    Returns
    -------
    A `SuccessTuple` of success (`bool`), message (`str`).

    """
    import os, pathlib
    from meerschaum.utils.warnings import warn
    if self.cache_pipe is not None:
        _delete_cache_tuple = self.cache_pipe.delete(debug=debug, **kw)
        if not _delete_cache_tuple[0]:
            warn(_delete_cache_tuple[1])
        _cache_db_path = pathlib.Path(self.cache_connector.database)
        try:
            os.remove(_cache_db_path)
        except Exception as e:
            warn(f"Could not delete cache file '{_cache_db_path}' for {self}:\n{e}")
    result = self.instance_connector.delete_pipe(self, debug=debug, **kw)
    if not isinstance(result, tuple):
        return False, f"Received unexpected result from '{self.instance_connector}': {result}"
    if result[0]:
        to_delete = ['_id', '_attributes', '_columns', '_tags', '_data']
        for member in to_delete:
            if member in self.__dict__:
                del self.__dict__[member]
    return result
