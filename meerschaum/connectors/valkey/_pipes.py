#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define pipes methods for `ValkeyConnector`.
"""

import meerschaum as mrsm
from meerschaum.utils.typing import SuccessTuple, Any, Union, Optional, Dict

PIPES_TABLE: str = 'pipes'
PIPES_COUNTER: str = 'pipes:counter'


def register_pipe(
    self,
    pipe: mrsm.Pipe,
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Insert the pipe's attributes into the internal `pipes` table.

    Parameters
    ----------
    pipe: mrsm.Pipe
        The pipe to be registered.

    Returns
    -------
    A `SuccessTuple` of the result.
    """
    attributes = {
        'connector_keys': str(pipe.connector_keys),
        'metric_key': str(pipe.connector_key),
        'location_key': str(pipe.location_key),
        'parameters': pipe._attributes.get('parameters', {}),
    }

    try:
        pipe_id = self.client.incr(PIPES_COUNTER)
        num_rows = self.push_docs(
            [{'pipe_id': pipe_id, **attributes}],
            PIPES_TABLE,
            datetime_column='pipe_id',
            debug=debug,
        )
        if num_rows == 0:
            return True, f"{pipe} is already registered."

    except Exception as e:
        return False, f"Failed to register {pipe}:\n{e}"

    return True, "Success"
