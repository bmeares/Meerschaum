#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
`python -m meerschaum.api.host --socket PATH`: serve the Python API host on a Unix socket.

Host mode must be set before `meerschaum.api` is imported, and `-m` imports the parent packages
first, so without `MRSM_API_HOST` set the process re-executes itself with it.
"""

import os
import sys
import argparse


def main(argv=None) -> int:
    from meerschaum.api.host import HOST_ENV, is_host_mode
    parser = argparse.ArgumentParser(
        prog='python -m meerschaum.api.host',
        description="Serve Meerschaum's Python API host (API plugins, plugin Dash apps, plugin MCP tools).",
    )
    parser.add_argument('--socket', required=True, help="The Unix socket to listen on.")
    parser.add_argument('--debug', action='store_true', help="Verbose logs.")
    args = parser.parse_args(argv)

    if not is_host_mode():
        env = dict(os.environ)
        env[HOST_ENV] = '1'
        os.execve(
            sys.executable,
            [sys.executable, '-m', 'meerschaum.api.host'] + list(sys.argv[1:] if argv is None else argv),
            env,
        )

    from meerschaum.api.host import run
    run(args.socket, debug=args.debug)
    return 0


if __name__ == '__main__':
    sys.exit(main())
