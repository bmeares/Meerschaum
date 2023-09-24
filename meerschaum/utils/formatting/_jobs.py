#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Print jobs information.
"""

from __future__ import annotations
from meerschaum.utils.typing import List, Optional, Any
from meerschaum.utils.daemon import (
    Daemon,
    get_daemons,
    get_running_daemons,
    get_stopped_daemons,
    get_paused_daemons,
)

def pprint_jobs(
        daemons: List[Daemon],
        nopretty: bool = False,
    ):
    """Pretty-print a list of Daemons."""
    from meerschaum.utils.formatting import make_header
    
    running_daemons = get_running_daemons(daemons)
    paused_daemons = get_paused_daemons(daemons)
    stopped_daemons = get_stopped_daemons(daemons)

    def _nopretty_print():
        from meerschaum.utils.misc import print_options
        if running_daemons:
            if not nopretty:
                print('\n' + make_header('Running jobs'))
            for d in running_daemons:
                pprint_job(d, nopretty=nopretty)

        if paused_daemons:
            if not nopretty:
                print('\n' + make_header('Paused jobs'))
            for d in paused_daemons:
                pprint_job(d, nopretty=nopretty)

        if stopped_daemons:
            if not nopretty:
                print('\n' + make_header('Stopped jobs'))
            for d in stopped_daemons:
                pprint_job(d, nopretty=nopretty)

    def _pretty_print():
        from meerschaum.utils.formatting import get_console, UNICODE, ANSI
        from meerschaum.utils.packages import import_rich, attempt_import
        rich = import_rich()
        rich_table, rich_text, rich_box = attempt_import('rich.table', 'rich.text', 'rich.box')
        table = rich_table.Table(
            title = rich_text.Text('Jobs'),
            box = (rich_box.ROUNDED if UNICODE else rich_box.ASCII),
            show_lines = True,
            show_header = ANSI,
        )
        table.add_column("Name", justify='right', style=('magenta' if ANSI else ''))
        table.add_column("Command")
        table.add_column("Status")

        for d in running_daemons:
            if d.hidden:
                continue
            table.add_row(
                d.daemon_id,
                d.label,
                rich_text.Text(d.status, style=('green' if ANSI else ''))
            )

        for d in paused_daemons:
            if d.hidden:
                continue
            table.add_row(
                d.daemon_id,
                d.label,
                rich_text.Text(d.status, style=('yellow' if ANSI else ''))
            )

        for d in stopped_daemons:
            if d.hidden:
                continue
            table.add_row(
                d.daemon_id,
                d.label,
                rich_text.Text(d.status, style=('red' if ANSI else ''))
            )
        get_console().print(table)

    print_function = _pretty_print if not nopretty else _nopretty_print
    print_function()


def pprint_job(
        daemon: Daemon,
        nopretty: bool = False,
    ):
    """Pretty-print a single Daemon."""
    if daemon.hidden:
        return
    from meerschaum.utils.warnings import info
    if not nopretty:
        info(f"Command for job '{daemon.daemon_id}':")
        print('\n' + daemon.label + '\n')
    else:
        print(daemon.daemon_id)
