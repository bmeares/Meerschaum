#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Verify the contents of a pipe by resyncing its interval.
"""

from datetime import datetime, timedelta
import time

import meerschaum as mrsm
from meerschaum.utils.typing import SuccessTuple, Any, Optional, Union, Tuple, Dict
from meerschaum.utils.warnings import warn, info
from meerschaum.config.static import STATIC_CONFIG


def verify(
    self,
    begin: Union[datetime, int, None] = None,
    end: Union[datetime, int, None] = None,
    params: Optional[Dict[str, Any]] = None,
    chunk_interval: Union[timedelta, int, None] = None,
    bounded: Optional[bool] = None,
    deduplicate: bool = False,
    workers: Optional[int] = None,
    batchsize: Optional[int] = None,
    skip_chunks_with_greater_rowcounts: bool = False,
    check_rowcounts_only: bool = False,
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

    batchsize: Optional[int], default None
        If provided, sync this many chunks in parallel.
        Defaults to `Pipe.get_num_workers()`.

    skip_chunks_with_greater_rowcounts: bool, default False
        If `True`, compare the rowcounts for a chunk and skip syncing if the pipe's
        chunk rowcount equals or exceeds the remote's rowcount.

    check_rowcounts_only: bool, default False
        If `True`, only compare rowcounts and print chunks which are out-of-sync.

    debug: bool, default False
        Verbosity toggle.

    kwargs: Any
        All keyword arguments are passed to `pipe.sync()`.

    Returns
    -------
    A SuccessTuple indicating whether the pipe was successfully resynced.
    """
    from meerschaum.utils.pool import get_pool
    from meerschaum.utils.formatting import make_header
    from meerschaum.utils.misc import interval_str
    workers = self.get_num_workers(workers)
    check_rowcounts = skip_chunks_with_greater_rowcounts or check_rowcounts_only

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
        if begin is None:
            remote_oldest_sync_time = self.get_sync_time(newest=False, remote=True, debug=debug)
            begin = remote_oldest_sync_time
    if bounded and end is None:
        end = self.get_sync_time(newest=True, debug=debug)
        if end is None:
            remote_newest_sync_time = self.get_sync_time(newest=True, remote=True, debug=debug)
            end = remote_newest_sync_time
        if end is not None:
            end += (
                timedelta(minutes=1)
                if hasattr(end, 'tzinfo')
                else 1
            )

    begin, end = self.parse_date_bounds(begin, end)
    cannot_determine_bounds = bounded and begin is None and end is None

    if cannot_determine_bounds and not check_rowcounts_only:
        warn(f"Cannot determine sync bounds for {self}. Syncing instead...", stack=False)
        sync_success, sync_msg = self.sync(
            begin=begin,
            end=end,
            params=params,
            workers=workers,
            debug=debug,
            **kwargs
        )
        if not sync_success:
            return sync_success, sync_msg

        if deduplicate:
            return self.deduplicate(
                begin=begin,
                end=end,
                params=params,
                workers=workers,
                debug=debug,
                **kwargs
            )
        return sync_success, sync_msg

    chunk_interval = self.get_chunk_interval(chunk_interval, debug=debug)
    chunk_bounds = self.get_chunk_bounds(
        begin=begin,
        end=end,
        chunk_interval=chunk_interval,
        bounded=bounded,
        debug=debug,
    )

    ### Consider it a success if no chunks need to be verified.
    if not chunk_bounds:
        if deduplicate:
            return self.deduplicate(
                begin=begin,
                end=end,
                params=params,
                workers=workers,
                debug=debug,
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
    message_header = f"{begin_to_print} - {end_to_print}"
    max_chunks_syncs = mrsm.get_config('pipes', 'verify', 'max_chunks_syncs')

    info(
        f"Verifying {self}:\n    "
        + ("Syncing" if not check_rowcounts_only else "Checking")
        + f" {len(chunk_bounds)} chunk"
        + ('s' if len(chunk_bounds) != 1 else '')
        + f" ({'un' if not bounded else ''}bounded)"
        + f" of size '{interval_str(chunk_interval)}'"
        + f" between '{begin_to_print}' and '{end_to_print}'.\n"
    )

    ### Dictionary of the form bounds -> success_tuple, e.g.:
    ### {
    ###    (2023-01-01, 2023-01-02): (True, "Success")
    ### }
    bounds_success_tuples = {}
    def process_chunk_bounds(
        chunk_begin_and_end: Tuple[
            Union[int, datetime],
            Union[int, datetime]
        ],
        _workers: Optional[int] = 1,
    ):
        if chunk_begin_and_end in bounds_success_tuples:
            return chunk_begin_and_end, bounds_success_tuples[chunk_begin_and_end]

        chunk_begin, chunk_end = chunk_begin_and_end
        do_sync = True
        chunk_success, chunk_msg = False, "Did not sync chunk."
        if check_rowcounts:
            existing_rowcount = self.get_rowcount(begin=chunk_begin, end=chunk_end, debug=debug)
            remote_rowcount = self.get_rowcount(
                begin=chunk_begin,
                end=chunk_end,
                remote=True,
                debug=debug,
            )
            checked_rows_str = (
                f"checked {existing_rowcount:,} row"
                + ("s" if existing_rowcount != 1 else '')
                + f" vs {remote_rowcount:,} remote"
            )
            if (
                existing_rowcount is not None
                and remote_rowcount is not None
                and existing_rowcount >= remote_rowcount
            ):
                do_sync = False
                chunk_success, chunk_msg = True, (
                    "Row-count is up-to-date "
                    f"({checked_rows_str})."
                )
            elif check_rowcounts_only:
                do_sync = False
                chunk_success, chunk_msg = True, (
                    f"Row-counts are out-of-sync ({checked_rows_str})."
                )

        num_syncs = 0
        while num_syncs < max_chunks_syncs:
            chunk_success, chunk_msg = self.sync(
                begin=chunk_begin,
                end=chunk_end,
                params=params,
                workers=_workers,
                debug=debug,
                **kwargs
            ) if do_sync else (chunk_success, chunk_msg)
            if chunk_success:
                break
            num_syncs += 1
            time.sleep(num_syncs**2)
        chunk_msg = chunk_msg.strip()
        if ' - ' not in chunk_msg:
            chunk_label = f"{chunk_begin} - {chunk_end}"
            chunk_msg = f'Verified chunk for {self}:\n{chunk_label}\n{chunk_msg}'
        mrsm.pprint((chunk_success, chunk_msg))

        return chunk_begin_and_end, (chunk_success, chunk_msg)

    ### If we have more than one chunk, attempt to sync the first one and return if its fails.
    if len(chunk_bounds) > 1:
        first_chunk_bounds = chunk_bounds[0]
        first_label = f"{first_chunk_bounds[0]} - {first_chunk_bounds[1]}"
        info(f"Verifying first chunk for {self}:\n    {first_label}")
        (
            (first_begin, first_end),
            (first_success, first_msg)
        ) = process_chunk_bounds(first_chunk_bounds, _workers=workers)
        if not first_success:
            return (
                first_success,
                f"\n{first_label}\n"
                + f"Failed to sync first chunk:\n{first_msg}"
            )
        bounds_success_tuples[first_chunk_bounds] = (first_success, first_msg)
        info(f"Completed first chunk for {self}:\n    {first_label}\n")

    pool = get_pool(workers=workers)
    batches = self.get_chunk_bounds_batches(chunk_bounds, batchsize=batchsize, workers=workers)

    def process_batch(
        batch_chunk_bounds: Tuple[
            Tuple[Union[datetime, int, None], Union[datetime, int, None]],
            ...
        ]
    ):
        _batch_begin = batch_chunk_bounds[0][0]
        _batch_end = batch_chunk_bounds[-1][-1]
        batch_message_header = f"{_batch_begin} - {_batch_end}"

        if check_rowcounts_only:
            info(f"Checking row-counts for batch bounds:\n    {batch_message_header}")
            _, (batch_init_success, batch_init_msg) = process_chunk_bounds(
                (_batch_begin, _batch_end)
            )
            mrsm.pprint((batch_init_success, batch_init_msg))
            if batch_init_success and 'up-to-date' in batch_init_msg:
                info("Entire batch is up-to-date.")
                return batch_init_success, batch_init_msg

        batch_bounds_success_tuples = dict(pool.map(process_chunk_bounds, batch_chunk_bounds))
        bounds_success_tuples.update(batch_bounds_success_tuples)
        batch_bounds_success_bools = {
            bounds: tup[0]
            for bounds, tup in batch_bounds_success_tuples.items()
        }

        if all(batch_bounds_success_bools.values()):
            msg = get_chunks_success_message(
                batch_bounds_success_tuples,
                header=batch_message_header,
                check_rowcounts_only=check_rowcounts_only,
            )
            if deduplicate:
                deduplicate_success, deduplicate_msg = self.deduplicate(
                    begin=_batch_begin,
                    end=_batch_end,
                    params=params,
                    workers=workers,
                    debug=debug,
                    **kwargs
                )
                return deduplicate_success, msg + '\n\n' + deduplicate_msg
            return True, msg

        batch_chunk_bounds_to_resync = [
            bounds
            for bounds, success in zip(batch_chunk_bounds, batch_bounds_success_bools)
            if not success
        ]
        batch_bounds_to_print = [
            f"{bounds[0]} - {bounds[1]}"
            for bounds in batch_chunk_bounds_to_resync
        ]
        if batch_bounds_to_print:
            warn(
                "Will resync the following failed chunks:\n    "
                + '\n    '.join(batch_bounds_to_print),
                stack=False,
            )

        retry_bounds_success_tuples = dict(pool.map(
            process_chunk_bounds,
            batch_chunk_bounds_to_resync
        ))
        batch_bounds_success_tuples.update(retry_bounds_success_tuples)
        bounds_success_tuples.update(retry_bounds_success_tuples)
        retry_bounds_success_bools = {
            bounds: tup[0]
            for bounds, tup in retry_bounds_success_tuples.items()
        }

        if all(retry_bounds_success_bools.values()):
            chunks_message = (
                get_chunks_success_message(
                    batch_bounds_success_tuples,
                    header=batch_message_header,
                    check_rowcounts_only=check_rowcounts_only,
                ) + f"\nRetried {len(batch_chunk_bounds_to_resync)} chunk" + (
                    's'
                    if len(batch_chunk_bounds_to_resync) != 1
                    else ''
                ) + "."
            )
            if deduplicate:
                deduplicate_success, deduplicate_msg = self.deduplicate(
                    begin=_batch_begin,
                    end=_batch_end,
                    params=params,
                    workers=workers,
                    debug=debug,
                    **kwargs
                )
                return deduplicate_success, chunks_message + '\n\n' + deduplicate_msg
            return True, chunks_message

        batch_chunks_message = get_chunks_success_message(
            batch_bounds_success_tuples,
            header=batch_message_header,
            check_rowcounts_only=check_rowcounts_only,
        )
        if deduplicate:
            deduplicate_success, deduplicate_msg = self.deduplicate(
                begin=begin,
                end=end,
                params=params,
                workers=workers,
                debug=debug,
                **kwargs
            )
            return deduplicate_success, batch_chunks_message + '\n\n' + deduplicate_msg
        return False, batch_chunks_message

    num_batches = len(batches)
    for batch_i, batch in enumerate(batches):
        batch_begin = batch[0][0]
        batch_end = batch[-1][-1]
        batch_counter_str = f"({(batch_i + 1):,}/{num_batches:,})"
        batch_label = f"batch {batch_counter_str}:\n{batch_begin} - {batch_end}"
        retry_failed_batch = True
        try:
            for_self = 'for ' + str(self)
            batch_label_str = batch_label.replace(':\n', ' ' + for_self + '...\n    ')
            info(f"Verifying {batch_label_str}\n")
            batch_success, batch_msg = process_batch(batch)
        except (KeyboardInterrupt, Exception) as e:
            batch_success = False
            batch_msg = str(e)
            retry_failed_batch = False

        batch_msg_to_print = (
            f"{make_header('Completed batch ' + batch_counter_str + ':')}\n{batch_msg}"
        )
        mrsm.pprint((batch_success, batch_msg_to_print))

        if not batch_success and retry_failed_batch:
            info(f"Retrying batch {batch_counter_str}...")
            retry_batch_success, retry_batch_msg = process_batch(batch)
            retry_batch_msg_to_print = (
                f"Retried {make_header('batch ' + batch_label)}\n{retry_batch_msg}"
            )
            mrsm.pprint((retry_batch_success, retry_batch_msg_to_print))

            batch_success = retry_batch_success
            batch_msg = retry_batch_msg

        if not batch_success:
            return False, f"Failed to verify {batch_label}:\n\n{batch_msg}"

    chunks_message = get_chunks_success_message(
        bounds_success_tuples,
        header=message_header,
        check_rowcounts_only=check_rowcounts_only,
    )
    return True, chunks_message



def get_chunks_success_message(
    chunk_success_tuples: Dict[Tuple[Any, Any], SuccessTuple],
    header: str = '',
    check_rowcounts_only: bool = False,
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
    checks = [stat['checked'] for stat in chunk_stats]
    out_of_sync_bounds_messages = {
        bounds: message
        for bounds, (success, message) in chunk_success_tuples.items()
        if 'out-of-sync' in message
    } if check_rowcounts_only else {}

    num_inserted = sum(inserts)
    num_updated = sum(updates)
    num_upserted = sum(upserts)
    num_checked = sum(checks)
    num_fails = len(fail_chunk_bounds_tuples)
    num_out_of_sync = len(out_of_sync_bounds_messages)

    header = (header + "\n") if header else ""
    stats_msg = items_str(
        (
            (
                ([f'inserted {num_inserted:,}'] if num_inserted else [])
                + ([f'updated {num_updated:,}'] if num_updated else [])
                + ([f'upserted {num_upserted:,}'] if num_upserted else [])
                + ([f'checked {num_checked:,}'] if num_checked else [])
            ) or ['synced 0']
        ),
        quotes=False,
        and_=False,
    )

    success_msg = (
        "Successfully "
        + ('synced' if not check_rowcounts_only else 'checked')
        + f" {len(chunk_success_tuples):,} chunk"
        + ('s' if len(chunk_success_tuples) != 1 else '')
        + '\n(' + stats_msg
        + ' rows in total).'
    )
    if check_rowcounts_only:
        success_msg += (
            f"\n\nFound {num_out_of_sync} chunk"
            + ('s' if num_out_of_sync != 1 else '')
            + ' to be out-of-sync'
            + ('.' if num_out_of_sync == 0 else ':\n\n  ')
            + '\n  '.join(
                [
                    f'{lbound} - {rbound}'
                    for lbound, rbound in out_of_sync_bounds_messages
                ]
            )
        )
    fail_msg = (
        ''
        if num_fails == 0
        else (
            f"\n\nFailed to sync {num_fails:,} chunk"
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

    dt_typ = self.dtypes.get(dt_col, 'datetime64[ns, UTC]')
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
    max_bound_time_days = STATIC_CONFIG['pipes']['max_bound_time_days']

    extreme_sync_times_delta = (
        hasattr(oldest_sync_time, 'tzinfo')
        and (sync_time - oldest_sync_time) >= timedelta(days=max_bound_time_days)
    )

    return (
        bound_time
        if bound_time > oldest_sync_time or extreme_sync_times_delta
        else None
    )
