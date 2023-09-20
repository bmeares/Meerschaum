#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Verify the contents of a pipe by resyncing its interval.
"""

from datetime import datetime, timedelta
from meerschaum.utils.typing import SuccessTuple, Any, Optional, Union, Tuple, List, Dict
from meerschaum.utils.warnings import warn, info
from meerschaum.utils.debug import dprint

def verify(
        self,
        begin: Union[datetime, int, None] = None,
        end: Union[datetime, int, None] = None,
        params: Optional[Dict[str, Any]] = None,
        chunk_interval: Union[timedelta, int, None] = None,
        bounded: bool = False,
        deduplicate: bool = False,
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

    chunk_interval: Union[timedelta, int, None], default None
        If provided, use this as the size of the chunk boundaries.
        Default to the value set in `pipe.parameters['chunk_minutes']` (1440).

    bounded: bool, default False
        If `True`, do not verify older than the oldest existing datetime 
        or newer than the newest existing datetime (i.e. `begin=pipe.get_sync_time(newest=False)`
        and `end=pipe.get_sync_time() + timedelta(minutes=1)`.

    deduplicate: bool, default False
        If `True`, deduplicate the pipe's table after the verification syncs.

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
    sync_less_than_begin = not bounded and begin is None
    sync_greater_than_end = not bounded and end is None

    if begin is None:
        begin = self.get_sync_time(newest=False, debug=debug)
    if end is None:
        end = self.get_sync_time(newest=True, debug=debug)

    if bounded:
        end += (
            timedelta(minutes=1)
            if isinstance(end, datetime)
            else 1
        )

    cannot_determine_bounds = (
        begin is None
        or
        end is None
        or
        not self.exists(debug=debug)
    )

    if cannot_determine_bounds:
        sync_success, sync_msg = self.sync(
            begin = begin,
            end = end,
            params = params,
            workers = workers,
            debug = debug,
            **kwargs
        )
        if not sync_success:
            return sync_success, sync_msg
        if deduplicate:
            return self.deduplicate(
                begin = begin,
                end = end,
                params = params,
                workers = workers,
                debug = debug,
                **kwargs
            )
        return sync_success, sync_msg


    if not chunk_interval:
        chunk_interval = self.get_chunk_interval(debug=debug)
    chunk_bounds = self.get_chunk_bounds(
        chunk_interval = chunk_interval,
        bounded = bounded,
        debug = debug,
    )

    ### Consider it a success if no chunks need to be verified.
    if not chunk_bounds:
        if deduplicate:
            return self.deduplicate(
                begin = begin,
                end = end,
                params = params,
                workers = workers,
                debug = debug,
                **kwargs
            )
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
            params = params,
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

    message_header = f"{begin} - {end}"
    if all(bounds_success_bools.values()):
        msg = get_chunks_success_message(bounds_success_tuples, header=message_header)
        if deduplicate:
            deduplicate_success, deduplicate_msg = self.deduplicate(
                begin = begin,
                end = end,
                params = params,
                workers = workers,
                debug = debug,
                **kwargs
            )
            return deduplicate_success, msg + '\n\n' + deduplicate_msg
        return True, msg

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
        message = (
            get_chunks_success_message(bounds_success_tuples, header=message_header)
            + f"\nRetried {len(chunk_bounds_to_resync)} chunks."
        )
        if deduplicate:
            deduplicate_success, deduplicate_msg = self.deduplicate(
                begin = begin,
                end = end,
                params = params,
                workers = workers,
                debug = debug,
                **kwargs
            )
            return deduplicate_success, message + '\n\n' + deduplicate_msg
        return True, message

    message = get_chunks_success_message(bounds_success_tuples, header=message_header)
    if deduplicate:
        deduplicate_success, deduplicate_msg = self.deduplicate(
            begin = begin,
            end = end,
            params = params,
            workers = workers,
            debug = debug,
            **kwargs
        )
        return deduplicate_success, message + '\n\n' + deduplicate_msg
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
