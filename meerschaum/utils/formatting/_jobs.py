#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Print jobs information.
"""

from __future__ import annotations

import meerschaum as mrsm
from meerschaum.utils.typing import List, Optional, Any, is_success_tuple, Dict
from meerschaum.utils.jobs import (
    Job,
    get_jobs,
    get_running_jobs,
    get_stopped_jobs,
    get_paused_jobs,
)


def pprint_jobs(
    jobs: Dict[str, Job],
    nopretty: bool = False,
):
    """Pretty-print a list of Daemons."""
    from meerschaum.utils.formatting import make_header
    
    running_jobs = get_running_jobs(jobs=jobs)
    paused_jobs = get_paused_jobs(jobs=jobs)
    stopped_jobs = get_stopped_jobs(jobs=jobs)
    executor_keys = (list(jobs.values())[0].executor_keys if jobs else None) or 'local'

    def _nopretty_print():
        from meerschaum.utils.misc import print_options
        if running_jobs:
            if not nopretty:
                print('\n' + make_header('Running jobs'))
            for name, job in running_jobs.items():
                pprint_job(job, nopretty=nopretty)

        if paused_jobs:
            if not nopretty:
                print('\n' + make_header('Paused jobs'))
            for name, job in paused_jobs.items():
                pprint_job(job, nopretty=nopretty)

        if stopped_jobs:
            if not nopretty:
                print('\n' + make_header('Stopped jobs'))
            for name, job in stopped_jobs.items():
                pprint_job(job, nopretty=nopretty)

    def _pretty_print():
        from meerschaum.utils.formatting import get_console, UNICODE, ANSI, format_success_tuple
        from meerschaum.utils.packages import import_rich, attempt_import
        rich = import_rich()
        rich_table, rich_text, rich_box, rich_json, rich_panel, rich_console = attempt_import(
            'rich.table', 'rich.text', 'rich.box', 'rich.json', 'rich.panel', 'rich.console',
        )
        table = rich_table.Table(
            title=rich_text.Text(f"\nJobs on Executor '{executor_keys}'"),
            box=(rich_box.ROUNDED if UNICODE else rich_box.ASCII),
            show_lines=True,
            show_header=ANSI,
        )
        table.add_column("Name", justify='right', style=('magenta' if ANSI else ''))
        table.add_column("Command")
        table.add_column("Status")

        def get_success_text(job):
            success_tuple = job.result
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


        for name, job in running_jobs.items():
            if job.hidden:
                continue

            status_text = (
                rich_text.Text(job.status, style=('green' if ANSI else ''))
                if not job.is_blocking_on_stdin()
                else rich_text.Text('waiting for input', style=('yellow' if ANSI else ''))
            )

            table.add_row(
                job.name,
                job.label,
                rich_console.Group(status_text),
            )

        for name, job in paused_jobs.items():
            if job.hidden:
                continue
            table.add_row(
                job.name,
                job.label,
                rich_console.Group(
                    rich_text.Text(job.status, style=('yellow' if ANSI else '')),
                ),
            )

        for name, job in stopped_jobs.items():
            if job.hidden:
                continue

            table.add_row(
                job.name,
                job.label,
                rich_console.Group(
                    rich_text.Text(job.status, style=('red' if ANSI else '')),
                    get_success_text(job)
                ),
            )
        get_console().print(table)

    print_function = _pretty_print if not nopretty else _nopretty_print
    print_function()


def pprint_job(
    job: Job,
    nopretty: bool = False,
):
    """Pretty-print a single `Job`."""
    if job.hidden:
        return

    from meerschaum.utils.warnings import info
    if not nopretty:
        info(f"Command for job '{job.name}':")
        print('\n' + job.label + '\n')
    else:
        print(job.name)
