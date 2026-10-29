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
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin

    if self.temporary:
        if self.cache:
            invalidate_success, invalidate_msg = self._invalidate_cache(hard=True, debug=debug)
            if not invalidate_success:
                return invalidate_success, invalidate_msg

        return (
            False,
            "Cannot delete pipes created with `temporary=True` (read-only). "
            + "You may want to call `pipe.drop()` instead."
        )

    ### Deleting drops the table first: a `drop` or `delete` lock refuses it before anything
    ### changes, whatever the instance.
    refusal = self._lock_refusal('delete_and_drop' if drop else 'delete', always=True, debug=debug)
    if refusal:
        return False, refusal

    if drop:
        drop_success, drop_msg = self.drop(debug=debug)
        if not drop_success:
            warn(f"Failed to drop {self}:\n{drop_msg}")

    with Venv(get_connector_plugin(self.instance_connector)):
        result = self.instance_connector.delete_pipe(self, debug=debug, **kw)

    if not isinstance(result, tuple):
        return False, f"Received an unexpected result from '{self.instance_connector}': {result}"

    if result[0]:
        self._invalidate_cache(hard=True, debug=debug)
        self._clear_cache_key('_id', debug=debug)
        from meerschaum.core.Pipeline._engine import forget_engine_state
        forget_engine_state(self, 'delete_pipe')

    return result
