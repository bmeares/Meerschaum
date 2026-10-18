#! /usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Copyright 2020–2026 Bennett Meares

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

from __future__ import annotations

import sys


def main(sysargs: list[str] | None = None) -> None:
    """
    Main CLI entry point: `python -m meerschaum ...` and the `mrsm` / `meerschaum` console scripts
    run the CLI in-process from the native extension (see `meerschaum._internal.binary`).
    """
    from meerschaum._internal.binary import main as run
    run(sysargs)


def _exit(return_code: int = 0, old_cwd: str | None = None) -> None:
    _close_pools()
    sys.exit(return_code)


def _close_pools():
    """Close multiprocessing pools before exiting."""
    ### Final step: close global pools.
    try:
        from meerschaum.utils.pool import get_pools
    except (ImportError, ModuleNotFoundError):
        return

    for pool in get_pools().values():
        try:
            pool.close()
            pool.terminate()
        except Exception as e:
            print(e)

if __name__ == "__main__":
    main(sys.argv[1:])
