#! /usr/bin/env python3

"""Compatibility sync function for legacy ``plugin:compose`` pipes."""

import time

import meerschaum as mrsm


def sync(pipe: mrsm.Pipe, **kwargs) -> mrsm.SuccessTuple:
    """Sync a legacy ``plugin:compose`` pipe's children one at a time."""
    from meerschaum.utils.formatting import make_header, UNICODE
    from meerschaum.utils.warnings import info

    child_messages = []
    loop_start = time.perf_counter()
    arrow = '⮡' if UNICODE else '->'
    success = True
    for child_num, child_pipe in enumerate(pipe.children):
        info(f"{pipe}:\n    {arrow} {child_num + 1}. Syncing {child_pipe}...")
        child_start = time.perf_counter()
        child_success, child_msg = child_pipe.sync(**kwargs)
        child_msg = child_msg.strip()
        mrsm.pprint((child_success, child_msg))
        child_messages.append((
            child_pipe,
            ("Successfully synced in " if child_success else "Failed to sync after ")
            + f"{round(time.perf_counter() - child_start, 2)} seconds:\n"
            + child_msg,
        ))
        success = success and child_success
        if not child_success:
            break

    msg = (
        f"Synced {len(child_messages)} pipe"
        + ('s' if len(child_messages) != 1 else '')
        + f" in {round(time.perf_counter() - loop_start, 2)} seconds."
    )
    for child_num, (child_pipe, child_msg) in enumerate(child_messages):
        msg += f"\n\n{make_header(str(child_num + 1) + '. ' + str(child_pipe))}\n{child_msg}"
    return success, msg
