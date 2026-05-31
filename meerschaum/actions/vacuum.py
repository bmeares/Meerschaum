#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Vacuum pipes' target tables to reclaim disk space.
"""

from meerschaum.utils.typing import SuccessTuple, Any, List, Optional


def vacuum(action: Optional[List[str]] = None, **kw: Any) -> SuccessTuple:
    """
    Vacuum pipes' target tables to reclaim dead-tuple disk space.

    For PostgreSQL-family tables this runs `VACUUM` (optionally `VACUUM FULL` with `--full`);
    other flavors fall back to their respective space-reclaiming mechanisms.
    """
    from meerschaum.actions import choose_subaction
    options = {
        'pipes': _vacuum_pipes,
    }
    return choose_subaction(action, options, **kw)


### PostgreSQL-family flavors: plain `VACUUM` reclaims dead tuples internally but does not
### return space to the OS (and may grow the table via FSM/VM forks). Only `VACUUM FULL`
### shrinks the on-disk size, so per-pipe stats are meaningless without `--full`.
_PG_VACUUM_FAMILY = {
    'postgresql',
    'postgis',
    'citus',
    'timescaledb',
    'timescaledb-ha',
}


def _vacuum_pipes(
    action: Optional[List[str]] = None,
    yes: bool = False,
    force: bool = False,
    noask: bool = False,
    full: bool = False,
    nopretty: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Vacuum the target tables for the selected pipes.
    """
    import os
    import contextlib
    from meerschaum import get_pipes
    from meerschaum.utils.prompt import yes_no
    from meerschaum.utils.warnings import warn, info
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.formatting import pprint, format_bytes
    from meerschaum.utils.formatting._shell import progress
    from meerschaum.utils.daemon import running_in_daemon
    from meerschaum._internal.static import STATIC_CONFIG

    pipes = get_pipes(as_list=True, debug=debug, **kw)
    if len(pipes) == 0:
        return False, "No pipes to vacuum."

    pg_without_full = (
        not full
        and any(
            getattr(pipe.instance_connector, 'flavor', None) in _PG_VACUUM_FAMILY
            for pipe in pipes
        )
    )
    if pg_without_full and not nopretty:
        info("Run `vacuum pipes --full` to reclaim space (needs exclusive lock).")

    question = (
        "Are you sure you want to vacuum the target tables for these pipes?\n"
        + (
            "    `VACUUM FULL` takes an exclusive lock and may take a while.\n\n"
            if full
            else "    This may take a while.\n\n"
        )
    )
    for pipe in pipes:
        question += f"    - {pipe}\n"
    question += "\n"

    if force:
        answer = True
    else:
        answer = yes_no(question, default='n', noask=noask, yes=yes)

    if not answer:
        return False, "No pipes were vacuumed."

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

    success_dict = {}
    successes, fails = 0, 0
    total_before, total_after = 0, 0
    stats_rows = []

    cm = _progress if _progress is not None else contextlib.nullcontext()
    with cm:
        task = (
            _progress.add_task("Vacuuming pipes...", total=len(pipes))
            if _progress is not None
            else None
        )
        for pipe in pipes:
            flavor = getattr(pipe.instance_connector, 'flavor', None)
            ### PostgreSQL-family plain `VACUUM` doesn't shrink on-disk size; stats are noise.
            show_stats = full or flavor not in _PG_VACUUM_FAMILY

            if not nopretty:
                info(f"Vacuuming {pipe}...")
            size_before = pipe.get_size(debug=debug) if show_stats else None
            vacuum_success, vacuum_msg = pipe.vacuum(full=full, debug=debug)
            success_dict[pipe] = vacuum_msg
            if vacuum_success:
                successes += 1
            else:
                fails += 1
                warn(vacuum_msg, stack=False)

            size_after = pipe.get_size(debug=debug) if show_stats else None
            if size_before is not None and size_after is not None:
                total_before += size_before
                total_after += size_after
                reclaimed = size_before - size_after
                stats_rows.append((
                    reclaimed,
                    str(pipe),
                    format_bytes(size_before),
                    format_bytes(size_after),
                    format_bytes(reclaimed) if reclaimed >= 0 else f"-{format_bytes(-reclaimed)}",
                ))

            if _progress is not None:
                _progress.advance(task)

    ### Sort by space saved descending (most reclaimed at top).
    stats_rows.sort(key=lambda row: row[0], reverse=True)

    if debug:
        dprint("Results for vacuuming pipes.")
        pprint(success_dict)

    if stats_rows and not nopretty:
        total_reclaimed = total_before - total_after
        name_width = max([len("Pipe")] + [len(row[1]) for row in stats_rows])
        before_width = max([len("Before")] + [len(row[2]) for row in stats_rows])
        after_width = max([len("After")] + [len(row[3]) for row in stats_rows])
        saved_width = max([len("Saved")] + [len(row[4]) for row in stats_rows])

        header = (
            f"    {'Pipe':<{name_width}}  {'Before':>{before_width}}  "
            f"{'After':>{after_width}}  {'Saved':>{saved_width}}"
        )
        sep = "    " + "-" * (len(header) - 4)
        lines = [header, sep]
        for _, name, before, after, saved in stats_rows:
            lines.append(
                f"    {name:<{name_width}}  {before:>{before_width}}  "
                f"{after:>{after_width}}  {saved:>{saved_width}}"
            )
        lines.append(sep)
        total_saved_str = (
            format_bytes(total_reclaimed)
            if total_reclaimed >= 0
            else f"-{format_bytes(-total_reclaimed)}"
        )
        lines.append(
            f"    {'Total':<{name_width}}  {format_bytes(total_before):>{before_width}}  "
            f"{format_bytes(total_after):>{after_width}}  {total_saved_str:>{saved_width}}"
        )
        print("\n" + "\n".join(lines) + "\n")

    msg = (
        f"Finished vacuuming {len(pipes)} pipe"
        + ('s' if len(pipes) != 1 else '')
        + f"\n    ({successes} succeeded, {fails} failed)."
    )

    return successes > 0, msg


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
vacuum.__doc__ += _choices_docstring('vacuum')
