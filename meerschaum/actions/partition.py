#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Repartition pipes' target tables to a new chunk (partition) width.
"""

from meerschaum.utils.typing import SuccessTuple, Any, List, Optional


def partition(action: Optional[List[str]] = None, **kw: Any) -> SuccessTuple:
    """
    Repartition pipes' target tables to a new chunk (partition) width.

    Only natively range-partitioned pipes are affected — those created on PostgreSQL / PostGIS,
    MySQL / MariaDB, or MSSQL with `hypertable: True` — plus TimescaleDB hypertables. Set the new
    width with `--chunk-minutes` (defaults to each pipe's `verify.chunk_minutes`, 30 days). On
    TimescaleDB this changes the chunk interval for FUTURE chunks (existing chunks are unchanged);
    on the other flavors the table is rebuilt at the new width (read, dropped, and re-synced).

    Usage:
    - partition pipes -m weather --chunk-minutes 10080
        - Rebuild the 'weather' pipes' tables to 7-day partitions.
    - partition pipes -i sql:main --chunk-minutes 1440 -y
        - Rebuild all partitioned pipes on `sql:main` to 1-day partitions, skipping the prompt.
    - partition pipes -t production
        - Repartition tagged pipes to each one's configured `verify.chunk_minutes`.

    """
    from meerschaum.actions import choose_subaction
    options = {
        'pipes': _partition_pipes,
    }
    return choose_subaction(action, options, **kw)


def _partition_pipes(
    action: Optional[List[str]] = None,
    chunk_minutes: Optional[int] = None,
    yes: bool = False,
    force: bool = False,
    noask: bool = False,
    nopretty: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Repartition the target tables for the selected pipes.

    Select pipes with the standard `-c` / `-m` / `-l` / `-i` / `-t` filters. Each selected pipe is
    repartitioned via `Pipe.repartition()`; non-partitioned pipes (and unsupported flavors like
    SQLite) report a failure and are skipped.

    Parameters
    ----------
    chunk_minutes: Optional[int], default None
        The new partition width in minutes (`--chunk-minutes`). Defaults to each pipe's
        `verify.chunk_minutes` (30 days). For example, `10080` is 7 days and `1440` is 1 day.

    yes, force, noask:
        Confirmation controls. Pass `-y` / `--yes` (or `-f` / `--force`) to skip the prompt — the
        rebuild reads the whole table into memory and briefly drops it, so confirm for large tables.

    Usage:
    - partition pipes -c sql:main -m weather -l us --chunk-minutes 10080
        - Rebuild one pipe's table to 7-day partitions.
    - partition pipes -i sql:main -f
        - Repartition every partitioned pipe on `sql:main` to its configured width, no prompt.

    """
    import os
    import contextlib
    from datetime import timedelta
    from meerschaum import get_pipes
    from meerschaum.utils.prompt import yes_no
    from meerschaum.utils.warnings import warn, info
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.misc import interval_str
    from meerschaum.utils.formatting import pprint
    from meerschaum.utils.formatting._shell import progress
    from meerschaum.utils.daemon import running_in_daemon
    from meerschaum._internal.static import STATIC_CONFIG

    pipes = get_pipes(as_list=True, debug=debug, **kw)
    if len(pipes) == 0:
        return False, "No pipes to repartition."

    width_str = (
        interval_str(timedelta(minutes=chunk_minutes))
        if chunk_minutes is not None
        else "each pipe's configured `verify.chunk_minutes`"
    )
    question = (
        f"Repartition the target tables for these pipes to {width_str}?\n"
        "    Non-TimescaleDB tables are rebuilt (read, dropped, and re-synced), which may take "
        "a while and load each table into memory.\n\n"
    )
    for pipe in pipes:
        question += f"    - {pipe}\n"
    question += "\n"

    if force:
        answer = True
    else:
        answer = yes_no(question, default='n', noask=noask, yes=yes)

    if not answer:
        return False, "No pipes were repartitioned."

    noninteractive_val = os.environ.get(STATIC_CONFIG['environment']['noninteractive'], None)
    noninteractive = str(noninteractive_val).lower() in ('1', 'true', 'yes')
    _progress = (
        progress()
        if (
            kw.get('shell', False)
            and not noninteractive
            and not running_in_daemon()
            and not nopretty
            and not debug
        )
        else None
    )

    def _width_display(pipe) -> str:
        """Human-readable applied width, from the pipe's resolved chunk interval."""
        try:
            iv = pipe.get_chunk_interval(chunk_minutes, debug=debug)
        except Exception:
            return "-"
        return interval_str(iv) if isinstance(iv, timedelta) else f"{iv:,}"

    def _partition_count(pipe) -> Optional[int]:
        get_info = getattr(pipe.instance_connector, 'get_partition_info', None)
        if get_info is None:
            return None
        try:
            return get_info(pipe, debug=debug).get('count', None)
        except Exception:
            return None

    success_dict = {}
    successes, fails = 0, 0
    total_partitions = 0
    stats_rows = []

    cm = _progress if _progress is not None else contextlib.nullcontext()
    with cm:
        task = (
            _progress.add_task("Repartitioning pipes...", total=len(pipes))
            if _progress is not None
            else None
        )
        for pipe in pipes:
            if not nopretty:
                info(f"Repartitioning {pipe}...")
            part_success, part_msg = pipe.repartition(chunk_minutes=chunk_minutes, debug=debug)
            success_dict[pipe] = part_msg
            if part_success:
                successes += 1
            else:
                fails += 1
                warn(part_msg, stack=False)

            count = _partition_count(pipe) if part_success else None
            if count is not None:
                total_partitions += count

            stats_rows.append((
                str(pipe),
                "OK" if part_success else "FAILED",
                _width_display(pipe) if part_success else "-",
                f"{count:,}" if count is not None else "-",
            ))

            if _progress is not None:
                _progress.advance(task)

    if debug:
        dprint("Results for repartitioning pipes.")
        pprint(success_dict)

    if stats_rows and not nopretty:
        headers = ("Pipe", "Result", "Width", "Partitions")
        name_w = max([len(headers[0])] + [len(r[0]) for r in stats_rows])
        result_w = max([len(headers[1])] + [len(r[1]) for r in stats_rows])
        width_w = max([len(headers[2])] + [len(r[2]) for r in stats_rows])
        parts_w = max([len(headers[3])] + [len(r[3]) for r in stats_rows])

        header = (
            f"    {headers[0]:<{name_w}}  {headers[1]:<{result_w}}  "
            f"{headers[2]:>{width_w}}  {headers[3]:>{parts_w}}"
        )
        sep = "    " + "-" * (len(header) - 4)
        lines = [header, sep]
        for name, result, width, parts in stats_rows:
            lines.append(
                f"    {name:<{name_w}}  {result:<{result_w}}  "
                f"{width:>{width_w}}  {parts:>{parts_w}}"
            )
        lines.append(sep)
        lines.append(
            f"    {'Total':<{name_w}}  {'':<{result_w}}  {'':>{width_w}}  "
            f"{format(total_partitions, ',') :>{parts_w}}"
        )
        print("\n" + "\n".join(lines) + "\n")

    msg = (
        f"Repartitioned {successes} of {len(pipes)} pipe"
        + ('s' if len(pipes) != 1 else '')
        + (f" ({fails} failed)" if fails else "")
        + f" across {total_partitions:,} partition" + ('s' if total_partitions != 1 else '')
        + "."
    )
    return successes > 0, msg


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
partition.__doc__ += _choices_docstring('partition')
