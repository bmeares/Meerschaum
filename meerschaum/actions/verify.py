#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Verify the states of pipes, pacakages, and more.
"""

from __future__ import annotations
from meerschaum.utils.typing import Union, Any, Sequence, SuccessTuple, Optional, Tuple, List

def verify(
        action: Optional[List[str]] = None,
        **kw
    ) -> SuccessTuple:
    """
    Verify the states of pipes, packages, and more.
    """
    from meerschaum.utils.misc import choose_subaction
    if action is None:
        action = []
    options = {
        'pipes': _verify_pipes,
        'packages': _verify_packages,
        'venvs': _verify_venvs,
    }
    return choose_subaction(action, options, **kw)


def _verify_pipes(**kwargs) -> SuccessTuple:
    """
    Verify the contents of pipes, syncing across their entire datetime intervals.
    """
    from meerschaum.actions.sync import _sync_pipes
    kwargs['verify'] = True
    return _sync_pipes(**kwargs)

    import time
    from meerschaum.utils.warnings import info
    running = True
    while running:
        try:
            success, msg = _do_verify_pipes_lap(**kwargs)
        except KeyboardInterrupt:
            loop = False
        running = loop
        if min_seconds > 0:
            info(f"Sleeping for {min_seconds} second" + ('s' if min_seconds != 1 else '') + '.')
            time.sleep(min_seconds)

    return success, msg


def _do_verify_pipes_lap(
        workers: Optional[int] = None,
        **kw
    ) -> SuccessTuple:
    """
    Verify the contents of pipes.
    """
    import time
    from meerschaum.utils.debug import dprint
    from meerschaum.utils.warnings import info
    from meerschaum.utils.formatting import print_pipes_results
    from meerschaum import get_pipes
    from meerschaum.utils.pool import get_pool
    pipes = get_pipes(as_list=True, **kw)
    if not pipes:
        return False, "No pipes to verify."

    workers = pipes[0].get_num_workers(workers)
    pool = get_pool(workers=workers)

    verify_begin = time.perf_counter()
    results = {}
    for pipe in pipes:
        info(f"Verifying the contents of {pipe}.")
        results[pipe] = pipe.verify(workers=workers, **kw)
    verify_end = time.perf_counter()

    print_pipes_results(
        results,
        success_header = 'Successfully verified pipes:',
        failure_header = 'Failed to verify pipes:',
        nopretty = kw.get('nopretty', False),
    )
    success_pipes = [pipe for pipe, (_success, _msg) in results.items() if _success]
    failure_pipes = [pipe for pipe, (_success, _msg) in results.items() if not _success]
    success = len(failure_pipes) == 0
    message = (
        f"It took {round(verify_end - verify_begin, 2)} seconds to verify {len(pipes)} pipe"
        +  ('s' if len(pipes) != 1 else '')
        + f"\n    ({len(success_pipes)} succeeded, {len(failure_pipes)} failed)."
    )
    return success, message


def _verify_packages(
        debug: bool = False,
        venv: Optional[str] = 'mrsm',
        **kw
    ) -> SuccessTuple:
    """
    Verify the versions of packages.
    """
    from meerschaum.utils.packages import (
        attempt_import, need_update, all_packages, is_installed, venv_contains_package,
        _monkey_patch_get_distribution, manually_import_module,
    )
    from meerschaum.utils.formatting import pprint
    from meerschaum.utils.debug import dprint

    venv_packages, base_packages, miss_packages = [], [], []

    ### Verify the system dependencies.
    for import_name, install_name in all_packages.items():
        _where_list = (
            venv_packages if venv_contains_package(
                import_name, split=False, venv=venv, debug=debug
            ) else (
                base_packages if is_installed(import_name, venv=None)
                else miss_packages
            )
        )
        _where_list.append(import_name)

    if 'flask_compress' in venv_packages or 'dash' in venv_packages:
        flask_compress = attempt_import('flask_compress', lazy=False, debug=debug)
        _monkey_patch_get_distribution('flask-compress', flask_compress.__version__)
        if 'flask_compress' in venv_packages:
            venv_packages.remove('flask_compress')

    for import_name in base_packages:
        manually_import_module(import_name, debug=debug, venv=None)
    for import_name in venv_packages:
        manually_import_module(import_name, debug=debug, venv=venv)

    ### Verify the plugins dependencies.
    return True, f"Verified {len(base_packages) + len(venv_packages)} packages."


def _verify_venvs(
        action: Optional[List[str]],
        debug: bool = False,
        **kw
    ) -> SuccessTuple:
    """
    Verify your virtual environments.
    """
    from meerschaum.utils.venv import get_venvs, verify_venv
    for venv in get_venvs():
        verify_venv(venv)
    return True, "Success"


### NOTE: This must be the final statement of the module.
###       Any subactions added below these lines will not
###       be added to the `help` docstring.
from meerschaum.utils.misc import choices_docstring as _choices_docstring
verify.__doc__ += _choices_docstring('verify')
