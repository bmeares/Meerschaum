#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Compress pipes' target tables to reduce disk usage.
"""

from meerschaum.utils.typing import SuccessTuple, Any, List, Optional


def compress(action: Optional[List[str]] = None, **kw: Any) -> SuccessTuple:
    """
    Compress pipes' target tables to reduce disk usage.

    For TimescaleDB hypertables this enables native compression and installs a
    compression policy so future synced chunks are compressed automatically.
    """
    from meerschaum.actions import choose_subaction
    options = {
        'pipes': _compress_pipes,
    }
    return choose_subaction(action, options, **kw)


def _compress_pipes(
    action: Optional[List[str]] = None,
    yes: bool = False,
    force: bool = False,
    noask: bool = False,
    no_policy: bool = False,
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Compress the target tables for the selected pipes.
    """
    from meerschaum import get_pipes
    from meerschaum.utils.prompt import yes_no
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.formatting import pprint, format_bytes

    pipes = get_pipes(as_list=True, debug=debug, **kw)
    if len(pipes) == 0:
        return False, "No pipes to compress."

    question = (
        "Are you sure you want to compress the target tables for these pipes?\n"
        "    This rewrites table data and may take a while.\n\n"
    )
    for pipe in pipes:
        question += f"    - {pipe}\n"
    question += "\n"

    if force:
        answer = True
    else:
        answer = yes_no(question, default='n', noask=noask, yes=yes)

    if not answer:
        return False, "No pipes were compressed."

    success_dict = {}
    successes, fails = 0, 0
    total_before, total_after = 0, 0
    stats_rows = []
    for pipe in pipes:
        size_before = pipe.get_size(debug=debug)
        compress_success, compress_msg = pipe.compress(no_policy=no_policy, debug=debug)
        success_dict[pipe] = compress_msg
        if compress_success:
            successes += 1
        else:
            fails += 1
            warn(compress_msg, stack=False)

        size_after = pipe.get_size(debug=debug)
        if size_before is not None and size_after is not None:
            total_before += size_before
            total_after += size_after
            reclaimed = size_before - size_after
            stats_rows.append((
                str(pipe),
                format_bytes(size_before),
                format_bytes(size_after),
                format_bytes(reclaimed) if reclaimed >= 0 else f"-{format_bytes(-reclaimed)}",
            ))

    if debug:
        dprint("Results for compressing pipes.")
        pprint(success_dict)

    msg = (
        f"Finished compressing {len(pipes)} pipe"
        + ('s' if len(pipes) != 1 else '')
        + f"\n    ({successes} succeeded, {fails} failed)."
    )

    if stats_rows:
        total_reclaimed = total_before - total_after
        name_width = max([len("Pipe")] + [len(row[0]) for row in stats_rows])
        before_width = max([len("Before")] + [len(row[1]) for row in stats_rows])
        after_width = max([len("After")] + [len(row[2]) for row in stats_rows])
        saved_width = max([len("Saved")] + [len(row[3]) for row in stats_rows])

        header = (
            f"    {'Pipe':<{name_width}}  {'Before':>{before_width}}  "
            f"{'After':>{after_width}}  {'Saved':>{saved_width}}"
        )
        sep = "    " + "-" * (len(header) - 4)
        lines = [header, sep]
        for name, before, after, saved in stats_rows:
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
        msg += "\n\n" + "\n".join(lines)

    return successes > 0, msg
