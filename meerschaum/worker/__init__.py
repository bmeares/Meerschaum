#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The Meerschaum Python worker (design §4.2, D2).

The Rust host (`mrsm` CLI, jobs, server, engine) starts `python -m meerschaum.worker --socket PATH`
when it needs Python code: plugin connectors (`fetch`, `sync`, `register`, `setup`, `subscribe`),
custom `@make_connector` connectors (plain and `IS_INSTANCE = True`), plugin actions, sync hooks,
and the plugin manifest. Control messages are MessagePack, data is Arrow IPC streams, and both
travel over a Unix domain socket (a named pipe on Windows).

The protocol is specified in `crates/meerschaum-worker/README.md`. The worker depends only on the
standard library, `pyarrow` and the v4 plugin machinery (`msgpack` is used when installed).
"""

PROTOCOL_VERSION: int = 1

__all__ = ('PROTOCOL_VERSION', 'serve', 'main')


def serve(address: str) -> int:
    """Connect to the host at `address` and serve calls until shutdown."""
    from meerschaum.worker._server import serve as _serve
    return _serve(address)


def main(argv=None) -> int:
    """Entry point for `python -m meerschaum.worker`."""
    import argparse
    parser = argparse.ArgumentParser(
        prog='python -m meerschaum.worker',
        description='Serve Python plugin code to a Meerschaum Rust host.',
    )
    parser.add_argument('--socket', required=True, help='The host socket (or named pipe) to connect to.')
    parser.add_argument('--version', action='version', version=f'protocol {PROTOCOL_VERSION}')
    args = parser.parse_args(argv)
    return serve(args.socket)
