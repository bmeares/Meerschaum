#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Define `mrsm compose up`: a delegation to the native Compose.
"""

from meerschaum.utils.typing import Any, Dict, List, Optional, SuccessTuple
import meerschaum as mrsm
from meerschaum.utils.warnings import dprint, info, warn
from meerschaum.utils.formatting import print_options


def _compose_up(
    compose_config: Dict[str, Any],
    debug: bool = False,
    **kw: Any
) -> SuccessTuple:
    """
    Start the project's jobs, after the initial syncs (`mrsm compose up`).
    """
    from meerschaum.compose.subactions import _delegate
    return _delegate('up', compose_config, debug=debug, **kw)


def run_initial_syncs(
    pipes: List[mrsm.Pipe],
    compose_config: Dict[str, Any],
    sysargs: Optional[List[str]] = None,
    debug: bool = False,
    **kw
) -> SuccessTuple:
    """
    Try two passes of syncing before starting the jobs (each pipe is synced by `mrsm sync pipes`).
    """
    from meerschaum.compose.utils import run_mrsm_command
    flags_to_remove = {
        '-c', '-C', '--connector-keys',
        '-m', '-M', '--metric-keys',
        '-l', '-L', '--location-keys',
        '-i', '-I', '--mrsm-instance', '--instance',
    }
    sysargs = sysargs or []
    indices_to_remove = {i for i, flag in enumerate(sysargs) if flag in flags_to_remove}
    flags = [
        flag
        for i, flag in enumerate(sysargs)
        if i not in indices_to_remove
            and (i - 1) not in indices_to_remove
    ]
    if '--no-daemon' not in flags:
        flags.append('--no-daemon')

    def _sync(pipe: mrsm.Pipe) -> SuccessTuple:
        if pipe.temporary:
            return pipe.sync(debug=debug, **kw)
        return run_mrsm_command(
            [
                'sync', 'pipes',
                '-c', str(pipe.connector_keys),
                '-m', str(pipe.metric_key),
                '-l', str(pipe.location_key),
                '-i', str(pipe.instance_keys),
            ] + flags,
            compose_config,
            capture_output=False,
            debug=debug,
            _replace=False,
        )

    failed_pipes = []
    for pipe in pipes:
        info(f"Syncing {pipe}...")
        success, msg = _sync(pipe)
        if not success:
            warn(f"Failed to sync {pipe}:\n{msg}", stack=False)
            failed_pipes.append(pipe)

    ### Pipes may be interdependent, so try again if we encounter any errors.
    for pipe in failed_pipes:
        info(f"Retry syncing {pipe}...")
        success, msg = _sync(pipe)
        if not success:
            warn(f"Failed to sync {pipe}:\n{msg}", stack=False)
            return False, f"Unable to begin syncing {pipe}:\n{msg}"

    return True, "Success"
