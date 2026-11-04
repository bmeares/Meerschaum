#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""RC33 test plugin: an action that loops by itself and reports its laps (`mrsm.jobs.report_lap`)."""

__version__ = '0.1.0'

import time

from meerschaum.plugins import make_action


@make_action
def w33_lapper(action=None, loop: bool = False, min_seconds: float = 120, **kwargs):
    """Report one lap, then (with `--loop`) wait `min_seconds` and report the next."""
    import meerschaum as mrsm
    laps = 0
    while True:
        laps += 1
        mrsm.jobs.report_lap(laps != 2, f"lap {laps}", next_in=float(min_seconds))
        if not loop:
            return True, f"Ran {laps} lap."
        time.sleep(float(min_seconds))
