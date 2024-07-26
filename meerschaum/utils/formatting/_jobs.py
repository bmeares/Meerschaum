#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Print jobs information.
"""

from __future__ import annotations
import meerschaum as mrsm
from meerschaum.utils.typing import List, Optional, Any, is_success_tuple
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
        from meerschaum.utils.formatting import get_console, UNICODE, ANSI, format_success_tuple
        from meerschaum.utils.packages import import_rich, attempt_import
        rich = import_rich()
        rich_table, rich_text, rich_box, rich_json, rich_panel, rich_console = attempt_import(
            'rich.table', 'rich.text', 'rich.box', 'rich.json', 'rich.panel', 'rich.console',
        )
        table = rich_table.Table(
            title = rich_text.Text('Jobs'),
            box = (rich_box.ROUNDED if UNICODE else rich_box.ASCII),
            show_lines = True,
            show_header = ANSI,
        )
        table.add_column("Name", justify='right', style=('magenta' if ANSI else ''))
        table.add_column("Command")
        table.add_column("Status")

        def get_success_text(d):
            success_tuple = d.properties.get('result', None)
            if isinstance(success_tuple, list):
                success_tuple = tuple(success_tuple)
            if not is_success_tuple(success_tuple):
                return rich_text.Text('')

            success = success_tuple[0]
            msg = success_tuple[1]
            lines = msg.split('\n')
            msg = '\n'.join(line.lstrip().rstrip() for line in lines)
            success_tuple = success, msg
            success_tuple_str = (
                format_success_tuple(success_tuple, left_padding=1)
                if success_tuple is not None
                else None
            )
            success_tuple_text = (
                rich_text.Text.from_ansi(success_tuple_str)
            ) if success_tuple_str is not None else None

            if success_tuple_text is None:
                return rich_text.Text('')

            return rich_text.Text('\n') + success_tuple_text


        for d in running_daemons:
            if d.hidden:
                continue
            table.add_row(
                d.daemon_id,
                d.label,
                rich_console.Group(
                    rich_text.Text(d.status, style=('green' if ANSI else '')),
                ),
            )

        for d in paused_daemons:
            if d.hidden:
                continue
            table.add_row(
                d.daemon_id,
                d.label,
                rich_console.Group(
                    rich_text.Text(d.status, style=('yellow' if ANSI else '')),
                ),
            )

        for d in stopped_daemons:
            if d.hidden:
                continue

            table.add_row(
                d.daemon_id,
                d.label,
                rich_console.Group(
                    rich_text.Text(d.status, style=('red' if ANSI else '')),
                    get_success_text(d)
                ),
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
