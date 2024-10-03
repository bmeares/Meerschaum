#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Delete duplicate rows within a pipe's table.
"""

from __future__ import annotations
from datetime import datetime, timedelta
from meerschaum.utils.typing import SuccessTuple, Any, Optional, Dict, Tuple, Union


def deduplicate(
    self,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    chunk_interval: Union[datetime, int, None] = None,
    bounded: Optional[bool] = None,
    workers: Optional[int] = None,
    debug: bool = False,
    _use_instance_method: bool = True,
    **kwargs: Any
) -> SuccessTuple:
    """
    Call the Pipe's instance connector's `delete_duplicates` method to delete duplicate rows.

    Parameters
    ----------
    begin: Union[datetime, int, None], default None:
        If provided, only deduplicate rows newer than this datetime value.

    end: Union[datetime, int, None], default None:
        If provided, only deduplicate rows older than this datetime column (not including end).

    params: Optional[Dict[str, Any]], default None
        Restrict deduplication to this filter (for multiplexed data streams).
        See `meerschaum.utils.sql.build_where`.

    chunk_interval: Union[timedelta, int, None], default None
        If provided, use this for the chunk bounds.
        Defaults to the value set in `pipe.parameters['chunk_minutes']` (1440).

    bounded: Optional[bool], default None
        Only check outside the oldest and newest sync times if bounded is explicitly `False`.

    workers: Optional[int], default None
        If the instance connector is thread-safe, limit concurrenct syncs to this many threads.

    debug: bool, default False:
        Verbositity toggle.

    kwargs: Any
        All other keyword arguments are passed to
        `pipe.sync()`, `pipe.clear()`, and `pipe.get_data().

    Returns
    -------
    A `SuccessTuple` corresponding to whether all of the chunks were successfully deduplicated.
    """
    from meerschaum.utils.warnings import warn, info
    from meerschaum.utils.misc import interval_str, items_str
    from meerschaum.utils.venv import Venv
    from meerschaum.connectors import get_connector_plugin
    from meerschaum.utils.pool import get_pool

    if self.cache_pipe is not None:
        success, msg = self.cache_pipe.deduplicate(
            begin = begin,
            end = end,
            params = params,
            bounded = bounded,
            debug = debug,
            _use_instance_method = _use_instance_method,
            **kwargs
        )
        if not success:
            warn(msg)

    workers = self.get_num_workers(workers=workers)
    pool = get_pool(workers=workers)

    if _use_instance_method:
        with Venv(get_connector_plugin(self.instance_connector)):
            if hasattr(self.instance_connector, 'deduplicate_pipe'):
                return self.instance_connector.deduplicate_pipe(
                    self,
                    begin = begin,
                    end = end,
                    params = params,
                    bounded = bounded,
                    debug = debug,
                    **kwargs
                )

    ### Only unbound if explicitly False.
    if bounded is None:
        bounded = True
    chunk_interval = self.get_chunk_interval(chunk_interval, debug=debug)

    bound_time = self.get_bound_time(debug=debug)
    if bounded and begin is None:
        begin = (
            bound_time
            if bound_time is not None
            else self.get_sync_time(newest=False, debug=debug)
        )
    if bounded and end is None:
        end = self.get_sync_time(newest=True, debug=debug)

    if bounded and end is not None:
        end += (
            timedelta(minutes=1)
            if isinstance(end, datetime)
            else 1
        )

    chunk_bounds = self.get_chunk_bounds(
        bounded = bounded,
        begin = begin,
        end = end,
        chunk_interval = chunk_interval,
        debug = debug,
    )

    indices = [col for col in self.columns.values() if col]
    if not indices:
        return False, f"Cannot deduplicate without index columns."
    dt_col = self.columns.get('datetime', None)

    def process_chunk_bounds(bounds) -> Tuple[
            Tuple[
                Union[datetime, int, None],
                Union[datetime, int, None]
            ],
            SuccessTuple
        ]:
        ### Only selecting the index values here to keep bandwidth down.
        chunk_begin, chunk_end = bounds
        chunk_df = self.get_data(
            select_columns = indices, 
            begin = chunk_begin,
            end = chunk_end,
            params = params,
            debug = debug,
        )
        if chunk_df is None:
            return bounds, (True, "")
        existing_chunk_len = len(chunk_df)
        deduped_chunk_df = chunk_df.drop_duplicates(keep='last')
        deduped_chunk_len = len(deduped_chunk_df)

        if existing_chunk_len == deduped_chunk_len:
            return bounds, (True, "")

        chunk_msg_header = f"\n{chunk_begin} - {chunk_end}"
        chunk_msg_body = ""

        full_chunk = self.get_data(
            begin=chunk_begin,
            end=chunk_end,
            params=params,
            debug=debug,
        )
        if full_chunk is None or len(full_chunk) == 0:
            return bounds, (True, f"{chunk_msg_header}\nChunk is empty, skipping...")

        chunk_indices = [ix for ix in indices if ix in full_chunk.columns]
        if not chunk_indices:
            return bounds, (False, f"None of {items_str(indices)} were present in chunk.")
        try:
            full_chunk = full_chunk.drop_duplicates(
                subset=chunk_indices,
                keep='last'
            ).reset_index(
                drop=True,
            )
        except Exception as e:
            return (
                bounds,
                (False, f"Failed to deduplicate chunk on {items_str(chunk_indices)}:\n({e})")
            )

        clear_success, clear_msg = self.clear(
            begin=chunk_begin,
            end=chunk_end,
            params=params,
            debug=debug,
        )
        if not clear_success:
            chunk_msg_body += f"Failed to clear chunk while deduplicating:\n{clear_msg}\n"
            warn(chunk_msg_body)

        sync_success, sync_msg = self.sync(full_chunk, debug=debug)
        if not sync_success:
            chunk_msg_body += f"Failed to sync chunk while deduplicating:\n{sync_msg}\n"

        ### Finally check if the deduplication worked.
        chunk_rowcount = self.get_rowcount(
            begin=chunk_begin,
            end=chunk_end,
            params=params,
            debug=debug,
        )
        if chunk_rowcount != deduped_chunk_len:
            return bounds, (
                False, (
                    chunk_msg_header + "\n"
                    + chunk_msg_body + ("\n" if chunk_msg_body else '')
                    + "Chunk rowcounts still differ ("
                    + f"{chunk_rowcount} rowcount vs {deduped_chunk_len} chunk length)."
                )
            )

        return bounds, (
            True, (
                chunk_msg_header + "\n"
                + chunk_msg_body + ("\n" if chunk_msg_body else '')
                + f"Deduplicated chunk from {existing_chunk_len} to {chunk_rowcount} rows."
            )
        )

    info(
        f"Deduplicating {len(chunk_bounds)} chunk"
        + ('s' if len(chunk_bounds) != 1 else '')
        + f" ({'un' if not bounded else ''}bounded)"
        + f" of size '{interval_str(chunk_interval)}'"
        + f" on {self}."
    )
    bounds_success_tuples = dict(pool.map(process_chunk_bounds, chunk_bounds))
    bounds_successes = {
        bounds: success_tuple
        for bounds, success_tuple in bounds_success_tuples.items()
        if success_tuple[0]
    }
    bounds_failures = {
        bounds: success_tuple
        for bounds, success_tuple in bounds_success_tuples.items()
        if not success_tuple[0]
    }

    ### No need to retry if everything failed.
    if len(bounds_failures) > 0 and len(bounds_successes) == 0:
        return (
            False,
            (
                f"Failed to deduplicate {len(bounds_failures)} chunk"
                + ('s' if len(bounds_failures) != 1 else '')
                + ".\n"
                + "\n".join([msg for _, (_, msg) in bounds_failures.items() if msg])
            )
        )

    retry_bounds = [bounds for bounds in bounds_failures]
    if not retry_bounds:
        return (
            True,
            (
                f"Successfully deduplicated {len(bounds_successes)} chunk"
                + ('s' if len(bounds_successes) != 1 else '')
                + ".\n"
                + "\n".join([msg for _, (_, msg) in bounds_successes.items() if msg])
            ).rstrip('\n')
        )

    info(f"Retrying {len(retry_bounds)} chunks for {self}...")
    retry_bounds_success_tuples = dict(pool.map(process_chunk_bounds, retry_bounds))
    retry_bounds_successes = {
        bounds: success_tuple
        for bounds, success_tuple in bounds_success_tuples.items()
        if success_tuple[0]
    }
    retry_bounds_failures = {
        bounds: success_tuple
        for bounds, success_tuple in bounds_success_tuples.items()
        if not success_tuple[0]
    }

    bounds_successes.update(retry_bounds_successes)
    if not retry_bounds_failures:
        return (
            True,
            (
                f"Successfully deduplicated {len(bounds_successes)} chunk"
                + ('s' if len(bounds_successes) != 1 else '')
                + f"({len(retry_bounds_successes)} retried):\n"
                + "\n".join([msg for _, (_, msg) in bounds_successes.items() if msg])
            ).rstrip('\n')
        )

    return (
        False,
        (
            f"Failed to deduplicate {len(bounds_failures)} chunk"
            + ('s' if len(retry_bounds_failures) != 1 else '')
            + ".\n"
            + "\n".join([msg for _, (_, msg) in retry_bounds_failures.items() if msg])
        ).rstrip('\n')
    )
