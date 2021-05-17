#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Print jobs information.
"""

from __future__ import annotations
from meerschaum.utils.typing import List, Optional, Any
from meerschaum.utils.daemon import Daemon, get_daemons, get_running_daemons, get_stopped_daemons

def pprint_jobs(
        daemons : List[Daemon],
        nopretty : bool = False,
    ):
    """
    Pretty-print a list of Daemons.
    """
    from meerschaum.utils.formatting import make_header
    
    running_daemons = get_running_daemons(daemons)
    stopped_daemons = get_stopped_daemons(daemons, running_daemons)

    def _nopretty_print():
        from meerschaum.utils.misc import print_options
        if running_daemons:
            if not nopretty:
                print('\n' + make_header('Running jobs'))
            for d in running_daemons:
                pprint_job(d, nopretty=nopretty)
            #  print_options(running_daemons, nopretty=nopretty, no_rich=True, header='Running jobs')
        if stopped_daemons:
            if not nopretty:
                print('\n' + make_header('Stopped jobs'))
            for d in stopped_daemons:
                pprint_job(d, nopretty=nopretty)
            #  print_options(stopped_daemons, nopretty=nopretty, no_rich=True, header='Stopped jobs')

    _nopretty_print()

def pprint_job(
        daemon : Daemon,
        nopretty : bool = False,
    ):
    """
    Pretty-print a single Daemon.
    """
    from meerschaum.utils.warnings import info
    if not nopretty:
        info(f"Command for job '{daemon.daemon_id}':")
        print('\n' + daemon.label + '\n')
    else:
        print(daemon.daemon_id)

