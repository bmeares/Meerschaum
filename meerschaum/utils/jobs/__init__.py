#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Higher-level utilities for managing `meerschaum.utils.daemon.Daemon`.
"""

from meerschaum.utils.jobs._Job import Job
from meerschaum.utils.typing import Dict


def get_jobs() -> Dict[str, Job]:
    """
    Return a dictionary of the existing jobs.
    """
    from meerschaum.utils.daemon import get_daemons
    daemons = get_daemons()
    return {
        daemon.daemon_id: Job(name=daemon.daemon_id)
        for daemon in daemons
    }
