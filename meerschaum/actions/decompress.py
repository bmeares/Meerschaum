#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Decompress pipes' target tables, the inverse of `compress pipes`.
"""

from meerschaum.utils.typing import SuccessTuple, Any, List, Optional


def decompress(action: Optional[List[str]] = None, **kw: Any) -> SuccessTuple:
    """
    Decompress pipes' target tables, the inverse of `compress pipes`.

    For TimescaleDB hypertables this removes the compression policy and converts compressed
    chunks back to row-store so future synced chunks stay uncompressed.
    """
    from meerschaum.actions import choose_subaction
    options = {
        'pipes': _decompress_pipes,
    }
    return choose_subaction(action, options, **kw)


def _decompress_pipes(
    action: Optional[List[str]] = None,
    yes: bool = False,
    force: bool = False,
    noask: bool = False,
    no_policy: bool = False,
    nopretty: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Decompress the target tables for the selected pipes.
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
        return False, "No pipes to decompress."

    question = (
        "Are you sure you want to decompress the target tables for these pipes?\n"
        "    This rewrites table data, increases disk usage, and may take a while.\n\n"
    )
    for pipe in pipes:
        question += f"    - {pipe}\n"
    question += "\n"

    if force:
        answer = True
    else:
        answer = yes_no(question, default='n', noask=noask, yes=yes)

    if not answer:
        return False, "No pipes were decompressed."

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
            _progress.add_task("Decompressing pipes...", total=len(pipes))
            if _progress is not None
            else None
        )
        for pipe in pipes:
            if not nopretty:
                info(f"Decompressing {pipe}...")
            size_before = pipe.get_size(debug=debug)
            decompress_success, decompress_msg = pipe.decompress(no_policy=no_policy, debug=debug)
            success_dict[pipe] = decompress_msg
            if decompress_success:
                successes += 1
            else:
                fails += 1
                warn(decompress_msg, stack=False)

            size_after = pipe.get_size(debug=debug)
            if size_before is not None and size_after is not None:
                total_before += size_before
                total_after += size_after
                added = size_after - size_before
                stats_rows.append((
                    added,
                    str(pipe),
                    format_bytes(size_before),
                    format_bytes(size_after),
                    format_bytes(added) if added >= 0 else f"-{format_bytes(-added)}",
                ))

            if _progress is not None:
                _progress.advance(task)

    ### Sort by space added descending (largest change at top).
    stats_rows.sort(key=lambda row: row[0], reverse=True)

    if debug:
        dprint("Results for decompressing pipes.")
        pprint(success_dict)

    if stats_rows and not nopretty:
        total_added = total_after - total_before
        name_width = max([len("Pipe")] + [len(row[1]) for row in stats_rows])
        before_width = max([len("Before")] + [len(row[2]) for row in stats_rows])
        after_width = max([len("After")] + [len(row[3]) for row in stats_rows])
        added_width = max([len("Added")] + [len(row[4]) for row in stats_rows])

        header = (
            f"    {'Pipe':<{name_width}}  {'Before':>{before_width}}  "
            f"{'After':>{after_width}}  {'Added':>{added_width}}"
        )
        sep = "    " + "-" * (len(header) - 4)
        lines = [header, sep]
        for _, name, before, after, added in stats_rows:
            lines.append(
                f"    {name:<{name_width}}  {before:>{before_width}}  "
                f"{after:>{after_width}}  {added:>{added_width}}"
            )
        lines.append(sep)
        total_added_str = (
            format_bytes(total_added)
            if total_added >= 0
            else f"-{format_bytes(-total_added)}"
        )
        lines.append(
            f"    {'Total':<{name_width}}  {format_bytes(total_before):>{before_width}}  "
            f"{format_bytes(total_after):>{after_width}}  {total_added_str:>{added_width}}"
        )
        print("\n" + "\n".join(lines) + "\n")

    msg = (
        f"Finished decompressing {len(pipes)} pipe"
        + ('s' if len(pipes) != 1 else '')
        + f"\n    ({successes} succeeded, {fails} failed)."
    )

    return successes > 0, msg


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.actions import choices_docstring as _choices_docstring
decompress.__doc__ += _choices_docstring('decompress')
