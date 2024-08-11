#! /usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Copyright 2024 Bennett Meares

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import sys
import os
import copy

from meerschaum.utils.typing import List, Optional
from meerschaum.utils.formatting import print_tuple as _print_tuple


def main(sysargs: Optional[List[str]] = None) -> None:
    """Main CLI entry point."""
    if sysargs is None:
        sysargs = copy.deepcopy(sys.argv[1:])
    old_cwd = os.getcwd()

    ### Catch help flags.
    if '--help' in sysargs or '-h' in sysargs:
        from meerschaum._internal.arguments._parser import parse_help
        parse_help(sysargs)
        return _exit(old_cwd=old_cwd)

    ### Catch version flags.
    if '--version' in sysargs or '-V' in sysargs:
        from meerschaum._internal.arguments._parser import parse_version
        parse_version(sysargs)
        return _exit(old_cwd=old_cwd)

    from meerschaum._internal.entry import entry, get_shell

    ### Try to launch a shell if --shell is provided.
    if len(sysargs) == 0 or '--shell' in sysargs:
        if '--shell' in sysargs:
            sysargs.remove('--shell')
        get_shell(sysargs).cmdloop()
        return _exit(old_cwd=old_cwd)

    ### Print success or failure message.
    return_tuple = entry(sysargs)
    rc = 0
    if isinstance(return_tuple, tuple) and '--nopretty' not in sysargs:
        _print_tuple(return_tuple, upper_padding=1)
        rc = 0 if (return_tuple[0] is True) else 1

    return _exit(rc, old_cwd=old_cwd)


def _exit(return_code: int = 0, old_cwd: str = None) -> None:
    _close_pools()
    ### Restore the previous working directory.
    if old_cwd is not None and old_cwd != os.getcwd():
        os.chdir(old_cwd)
    sys.exit(return_code)


def _close_pools():
    """Close multiprocessing pools before exiting."""
    ### Final step: close global pools.
    from meerschaum.utils.pool import get_pools
    for pool in get_pools().values():
        try:
            pool.close()
            pool.terminate()
        except Exception as e:
            print(e)

if __name__ == "__main__":
    main(sys.argv[1:])
