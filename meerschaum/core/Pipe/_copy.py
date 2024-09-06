#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Define methods for copying pipes.
"""

from datetime import datetime, timedelta

import meerschaum as mrsm
from meerschaum.utils.typing import SuccessTuple, Any, Optional, Dict, Union


def copy_to(
    self,
    instance_keys: str,
    sync: bool = True,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    chunk_interval: Union[timedelta, int, None] = None,
    debug: bool = False,
    **kwargs: Any
) -> SuccessTuple:
    """
    Copy a pipe to another instance.

    Parameters
    ----------
    instance_keys: str
        The instance to which to copy this pipe.

    sync: bool, default True
        If `True`, sync the source pipe's documents 

    begin: Union[datetime, int, None], default None
        Beginning datetime value to pass to `Pipe.get_data()`.

    end: Union[datetime, int, None], default None
        End datetime value to pass to `Pipe.get_data()`.

    params: Optional[Dict[str, Any]], default None
        Parameters filter to pass to `Pipe.get_data()`.

    chunk_interval: Union[timedelta, int, None], default None
        The size of chunks to retrieve from `Pipe.get_data()` for syncing.

    kwargs: Any
        Additional flags to pass to `Pipe.get_data()` and `Pipe.sync()`, e.g. `workers`.

    Returns
    -------
    A SuccessTuple indicating success.
    """
    if str(instance_keys) == self.instance_keys:
        return False, f"Cannot copy {self} to instance '{instance_keys}'."

    new_pipe = mrsm.Pipe(
        self.connector_keys,
        self.metric_key,
        self.location_key,
        parameters=self.parameters.copy(),
        instance=instance_keys,
    )

    new_pipe_is_registered = new_pipe.get_id() is not None

    metadata_method = new_pipe.edit if new_pipe_is_registered else new_pipe.register
    metadata_success, metadata_msg = metadata_method(debug=debug)
    if not metadata_success:
        return metadata_success, metadata_msg

    if not self.exists(debug=debug):
        return True, f"{self} does not exist; nothing to sync."

    original_as_iterator = kwargs.get('as_iterator', None)
    kwargs['as_iterator'] = True

    chunk_generator = self.get_data(
        begin=begin,
        end=end,
        params=params,
        chunk_interval=chunk_interval,
        debug=debug,
        **kwargs
    )

    if original_as_iterator is None:
        _ = kwargs.pop('as_iterator', None)
    else:
        kwargs['as_iterator'] = original_as_iterator

    sync_success, sync_msg = new_pipe.sync(
        chunk_generator,
        begin=begin,
        end=end,
        params=params,
        debug=debug,
        **kwargs
    )
    msg = (
        f"Successfully synced {new_pipe}:\n{sync_msg}"
        if sync_success
        else f"Failed to sync {new_pipe}:\n{sync_msg}"
    )
    return sync_success, msg
