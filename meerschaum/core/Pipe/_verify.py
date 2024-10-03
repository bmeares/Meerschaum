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
    bounded: Optional[bool] = None,
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

    bounded: Optional[bool], default None
        If `True`, do not verify older than the oldest sync time or newer than the newest.
        If `False`, verify unbounded syncs outside of the new and old sync times.
        The default behavior (`None`) is to bound only if a bound interval is set
        (e.g. `pipe.parameters['verify']['bound_days']`).

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
    from meerschaum.utils.misc import interval_str
    workers = self.get_num_workers(workers)

    ### Skip configured bounding in parameters
    ### if `bounded` is explicitly `False`.
    bound_time = (
        self.get_bound_time(debug=debug)
        if bounded is not False
        else None
    )
    if bounded is None:
        bounded = bound_time is not None

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

    sync_less_than_begin = not bounded and begin is None
    sync_greater_than_end = not bounded and end is None

    cannot_determine_bounds = not self.exists(debug=debug)

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


    chunk_interval = self.get_chunk_interval(chunk_interval, debug=debug)
    chunk_bounds = self.get_chunk_bounds(
        begin = begin,
        end = end,
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

    begin_to_print = (
        begin
        if begin is not None
        else (
            chunk_bounds[0][0]
            if bounded
            else chunk_bounds[0][1]
        )
    )
    end_to_print = (
        end
        if end is not None
        else (
            chunk_bounds[-1][1]
            if bounded
            else chunk_bounds[-1][0]
        )
    )

    info(
        f"Syncing {len(chunk_bounds)} chunk" + ('s' if len(chunk_bounds) != 1 else '')
        + f" ({'un' if not bounded else ''}bounded)"
        + f" of size '{interval_str(chunk_interval)}'"
        + f" between '{begin_to_print}' and '{end_to_print}'."
    )

    pool = get_pool(workers=workers)

    ### Dictionary of the form bounds -> success_tuple, e.g.:
    ### {
    ###    (2023-01-01, 2023-01-02): (True, "Success")
    ### }
    bounds_success_tuples = {}
    def process_chunk_bounds(
            chunk_begin_and_end: Tuple[
                Union[int, datetime],
                Union[int, datetime]
            ]
        ):
        if chunk_begin_and_end in bounds_success_tuples:
            return chunk_begin_and_end, bounds_success_tuples[chunk_begin_and_end]

        chunk_begin, chunk_end = chunk_begin_and_end
        return chunk_begin_and_end, self.sync(
            begin = chunk_begin,
            end = chunk_end,
            params = params,
            workers = workers,
            debug = debug,
            **kwargs
        )

    ### If we have more than one chunk, attempt to sync the first one and return if its fails.
    if len(chunk_bounds) > 1:
        first_chunk_bounds = chunk_bounds[0]
        (
            (first_begin, first_end),
            (first_success, first_msg)
        ) = process_chunk_bounds(first_chunk_bounds)
        if not first_success:
            return (
                first_success,
                f"\n{first_begin} - {first_end}\n"
                + f"Failed to sync first chunk:\n{first_msg}"
            )
        bounds_success_tuples[first_chunk_bounds] = (first_success, first_msg)

    bounds_success_tuples.update(dict(pool.map(process_chunk_bounds, chunk_bounds)))
    bounds_success_bools = {bounds: tup[0] for bounds, tup in bounds_success_tuples.items()}

    message_header = f"{begin_to_print} - {end_to_print}"
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
        for bounds, success in zip(chunk_bounds, bounds_success_bools)
        if not success
    ]
    bounds_to_print = [
        f"{bounds[0]} - {bounds[1]}"
        for bounds in chunk_bounds_to_resync
    ]
    if bounds_to_print:
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
    from meerschaum.utils.misc import items_str
    success_chunk_messages = [tup[1] for bounds, tup in chunk_success_tuples.items() if tup[0]]
    fail_chunk_bounds_tuples = {
        bounds: tup
        for bounds, tup in chunk_success_tuples.items()
        if not tup[0]
    }
    chunk_stats = [extract_stats_from_message(msg) for msg in success_chunk_messages]
    inserts = [stat['inserted'] for stat in chunk_stats]
    updates = [stat['updated'] for stat in chunk_stats]
    upserts = [stat['upserted'] for stat in chunk_stats]
    num_inserted = sum(inserts)
    num_updated = sum(updates)
    num_upserted = sum(upserts)
    num_fails = len(fail_chunk_bounds_tuples)

    header = (header + "\n") if header else ""
    stats_msg = items_str(
        (
            ([f'inserted {num_inserted}'] if num_inserted else [])
            + ([f'updated {num_updated}'] if num_updated else [])
            + ([f'upserted {num_upserted}'] if num_upserted else [])
        ) or ['synced 0'],
        quotes=False,
        and_=False,
    )

    success_msg = (
        f"Successfully synced {len(chunk_success_tuples)} chunk"
        + ('s' if len(chunk_success_tuples) != 1 else '')
        + '\n(' + stats_msg
        + ' rows in total).'
    )
    fail_msg = (
        ''
        if num_fails == 0
        else (
            f"\n\nFailed to sync {num_fails} chunk"
            + ('s' if num_fails != 1 else '') + ":\n"
            + '\n'.join([
                f"{fail_begin} - {fail_end}\n{msg}\n"
                for (fail_begin, fail_end), (_, msg) in fail_chunk_bounds_tuples.items()
            ])
        )
    )

    return header + success_msg + fail_msg


def get_bound_interval(self, debug: bool = False) -> Union[timedelta, int, None]:
    """
    Return the interval used to determine the bound time (limit for verification syncs).
    If the datetime axis is an integer, just return its value.

    Below are the supported keys for the bound interval:

        - `pipe.parameters['verify']['bound_minutes']`
        - `pipe.parameters['verify']['bound_hours']`
        - `pipe.parameters['verify']['bound_days']`
        - `pipe.parameters['verify']['bound_weeks']`
        - `pipe.parameters['verify']['bound_years']`
        - `pipe.parameters['verify']['bound_seconds']`

    If multiple keys are present, the first on this priority list will be used.

    Returns
    -------
    A `timedelta` or `int` value to be used to determine the bound time.
    """
    verify_params = self.parameters.get('verify', {})
    prefix = 'bound_'
    suffixes_to_check = ('minutes', 'hours', 'days', 'weeks', 'years', 'seconds')
    keys_to_search = {
        key: val
        for key, val in verify_params.items()
        if key.startswith(prefix)
    }
    bound_time_key, bound_time_value = None, None
    for key, value in keys_to_search.items():
        for suffix in suffixes_to_check:
            if key == prefix + suffix:
                bound_time_key = key
                bound_time_value = value
                break
        if bound_time_key is not None:
            break

    if bound_time_value is None:
        return bound_time_value

    dt_col = self.columns.get('datetime', None)
    if not dt_col:
        return bound_time_value

    dt_typ = self.dtypes.get(dt_col, 'datetime64[ns]')
    if 'int' in dt_typ.lower():
        return int(bound_time_value)

    interval_type = bound_time_key.replace(prefix, '')
    return timedelta(**{interval_type: bound_time_value})


def get_bound_time(self, debug: bool = False) -> Union[datetime, int, None]:
    """
    The bound time is the limit at which long-running verification syncs should stop.
    A value of `None` means verification syncs should be unbounded.

    Like deriving a backtrack time from `pipe.get_sync_time()`,
    the bound time is the sync time minus a large window (e.g. 366 days).

    Unbound verification syncs (i.e. `bound_time is None`)
    if the oldest sync time is less than the bound interval.

    Returns
    -------
    A `datetime` or `int` corresponding to the
    `begin` bound for verification and deduplication syncs.
    """ 
    bound_interval = self.get_bound_interval(debug=debug)
    if bound_interval is None:
        return None

    sync_time = self.get_sync_time(debug=debug)
    if sync_time is None:
        return None

    bound_time = sync_time - bound_interval
    oldest_sync_time = self.get_sync_time(newest=False, debug=debug)

    return (
        bound_time
        if bound_time > oldest_sync_time
        else None
    )
