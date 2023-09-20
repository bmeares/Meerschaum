#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Verify the contents of a pipe by resyncing its interval.
"""

from datetime import datetime
from meerschaum.utils.typing import SuccessTuple, Any, Optional, Union, Tuple, List, Dict
from meerschaum.utils.warnings import warn, info
from meerschaum.utils.debug import dprint

def verify(
        self,
        begin: Union[datetime, int, None] = None,
        end: Union[datetime, int, None] = None,
        workers: Optional[int] = None,
        debug: bool = False,
        **kwargs: Any
    ) -> SuccessTuple:
    """
    Verify the contents of the pipe by resyncing its interval.

    Parameters
    ----------
    begin: Union[datetime, int, None], default None
        If specified, only verify rows greater than or equal to this value.

    end: Union[datetime, int, None], default None
        If specified, only verify rows less than this value.

    workers: Optional[int], default None
        If provided, limit the verification to this many threads.
        Use a value of `1` to sync chunks in series.

    debug: bool, default False
        Verbosity toggle.

    kwargs: Any
        All keyword arguments are passed to `pipe.sync()`.

    Returns
    -------
    A SuccessTuple indicating whether the pipe was successfully resynced.
    """
    from meerschaum.utils.pool import get_pool
    workers = self.get_num_workers(workers)
    sync_less_than_begin = begin is None
    sync_greater_than_end = end is None

    if begin is None:
        begin = self.get_sync_time(newest=False, debug=debug)
    if end is None:
        end = self.get_sync_time(newest=True, debug=debug)

    cannot_determine_bounds = (
        begin is None
        or
        end is None
        or
        not self.exists(debug=debug)
    )

    if cannot_determine_bounds:
        return self.sync(
            begin = begin,
            end = end,
            workers = workers,
            debug = debug,
            **kwargs
        )
    
    ### Now that we've determined the bounds, prepend this to the final message.
    message_header = f"{begin} - {end}"

    ### Edge case: --begin and --end are the same.
    if not sync_less_than_begin and not sync_greater_than_end and begin == end:
        return True, f"Begin and end are equal ('{begin}'); nothing to do."

    ### Set the chunk interval under `pipe.parameters['chunk_minutes']`.
    chunk_interval = self.get_chunk_interval(debug=debug)

    ### Build a list of tuples containing the chunk boundaries
    ### so that we can sync multiple chunks in parallel.
    ### Run `verify pipes --workers 1` to sync chunks in series.
    chunk_bounds = []
    begin_cursor = begin
    while begin_cursor < end:
        end_cursor = begin_cursor + chunk_interval
        chunk_bounds.append((begin_cursor, end_cursor))
        begin_cursor = end_cursor

    ### The chunk interval might be too large.
    if not chunk_bounds and end > begin:
        chunk_bounds = [(begin, end)]

    ### Truncate the last chunk to the end timestamp.
    if chunk_bounds[-1][1] > end:
        chunk_bounds[-1] = (chunk_bounds[-1][0], end)

    ### Pop the last chunk if its bounds are equal.
    if chunk_bounds[-1][0] == chunk_bounds[-1][1]:
        chunk_bounds = chunk_bounds[:-1]

    if sync_less_than_begin:
        chunk_bounds = [(None, begin)] + chunk_bounds
    if sync_greater_than_end:
        chunk_bounds = chunk_bounds + [(end, None)]

    ### Last check: return if no chunk can be determined.
    if not chunk_bounds:
        return True, f"Could not determine chunks between '{begin}' and '{end}'; nothing to do."

    info(
        f"Syncing {len(chunk_bounds)} chunk" + ('s' if len(chunk_bounds) != 1 else '')
        + f" of size '{chunk_interval}'"
        + f" between '{begin}' and '{end}'."
    )

    pool = get_pool(workers=workers)

    def process_chunk_bounds(
            chunk_begin_and_end: Tuple[
                Union[int, datetime],
                Union[int, datetime]
            ]
        ):
        chunk_begin, chunk_end = chunk_begin_and_end
        return chunk_begin_and_end, self.sync(
            begin = chunk_begin,
            end = chunk_end,
            workers = workers,
            debug = debug,
            **kwargs
        )

    ### Dictionary of the form bounds -> success_tuple, e.g.:
    ### {
    ###    (2023-01-01, 2023-01-02): (True, "Success")
    ### }
    bounds_success_tuples = dict(pool.map(process_chunk_bounds, chunk_bounds))
    bounds_success_bools = {bounds: tup[0] for bounds, tup in bounds_success_tuples.items()}

    if all(bounds_success_bools.values()):
        return True, get_chunks_success_message(bounds_success_tuples, header=message_header)

    chunk_bounds_to_resync = [
        bounds
        for bounds, success in zip(chunk_bounds, chunk_success_bools)
        if not success
    ]
    bounds_to_print = [
        f"{bounds[0]} - {bounds[1]}"
        for bounds in chunk_bounds_to_resync
    ]
    warn(
        f"Will resync the following failed chunks:\n    "
        + '\n    '.join(bounds_to_print),
        stack = False,
    )

    retry_bounds_success_tuples = dict(pool.map(process_chunk_bounds, chunk_bounds_to_resync))
    bounds_success_tuples.update(retry_bounds_success_tuples)
    retry_bounds_success_bools = {
        bounds: tup[0]
        for bounds, tup in retry_bounds_success_tuples.items()
    }

    if all(retry_bounds_success_bools.values()):
        message = get_chunks_success_message(bounds_success_tuples, header=message_header)
        return (
            True,
            (
                message
                + f"\nRetried {len(chunk_bounds_to_resync)} chunks."
            )
        )

    message = get_chunks_success_message(bounds_success_tuples, header=message_header)
    return False, message


def get_chunks_success_message(
        chunk_success_tuples: Dict[Tuple[Any, Any], SuccessTuple],
        header: str = '',
    ) -> str:
    """
    Sum together all of the inserts and updates from the chunks.

    Parameters
    ----------
    chunk_success_tuples: Dict[Tuple[Any, Any], SuccessTuple]
        A dictionary mapping chunk bounds to the resulting SuccessTuple.

    header: str
        An optional header to print before the message.

    Returns
    -------
    A success message.
    """
    from meerschaum.utils.formatting import extract_stats_from_message
    success_chunk_messages = [tup[1] for bounds, tup in chunk_success_tuples.items() if tup[0]]
    fail_chunk_bounds_tuples = {
        bounds: tup
        for bounds, tup in chunk_success_tuples.items()
        if not tup[0]
    }
    chunk_stats = [extract_stats_from_message(msg) for msg in success_chunk_messages]
    inserts = [stat['inserted'] for stat in chunk_stats]
    updates = [stat['updated'] for stat in chunk_stats]
    num_inserted = sum(inserts)
    num_updated = sum(updates)
    num_fails = len(fail_chunk_bounds_tuples)

    header = (header + "\n") if header else ""
    success_msg = (
        f"Successfully synced {len(chunk_success_tuples)} chunk"
        + ('s' if len(chunk_success_tuples) != 1 else '')
        + f'\n(inserted {num_inserted}, updated {num_updated} rows in total).'
    )
    fail_msg = (
        ''
        if num_fails == 0
        else (
            f"\n\nFailed to sync {num_fails} chunks:\n"
            + '\n'.join([
                f"{fail_begin} - {fail_end}\n{msg}\n"
                for (fail_begin, fail_end), (_, msg) in fail_chunk_bounds_tuples.items()
            ])
        )
    )

    return header + success_msg + fail_msg
