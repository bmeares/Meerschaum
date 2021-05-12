#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Print jobs information.
"""

from __future__ import annotations
from meerschaum.utils.typing import List, Optional, Any
from meerschaum.utils.daemon import Daemon, get_daemons

def pprint_jobs(
        daemons : List[Daemon],
        nopretty : bool = False,
    ):
    """
    Pretty-print a list of Daemons.
    """
    
    running_daemons = [
        d for d in daemons
            if d.properties is not None
                and 'ended' not in d.properties.get('process', {})
    ]
    stopped_daemons = [d for d in daemons if d not in running_daemons]

    def _nopretty_print():
        from meerschaum.utils.misc import print_options
        if running_daemons:
            print_options(running_daemons, nopretty=nopretty, no_rich=True, header='Running jobs')
        if stopped_daemons:
            print_options(stopped_daemons, nopretty=nopretty, no_rich=True, header='Stopped jobs')

    _nopretty_print()
