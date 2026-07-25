#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Serve Meerschaum's MCP server over the stdio transport.

This is the transport for a local Meerschaum install with no API server: an MCP
client spawns `mrsm start mcp` as a subprocess and speaks JSON-RPC over its
stdin and stdout.

There is no authentication, because there is nothing to authenticate against — a
stdio server runs as the user who launched it and can already do anything that
user's shell can. Scopes are therefore `['*']`, matching the CLI's trust model.
Use the HTTP transport (`/mcp` on the API) whenever the caller is remote.

Because stdout *is* the protocol channel, nothing may be printed to it. Not every
Meerschaum code path cooperates — `info()` and action output use plain `print()`,
and a rich traceback renders to a stdout console — so dispatch runs with stdout
redirected to stderr, where MCP clients surface it as server logs. Responses are
written to the real stdout handle, captured before the redirect.
"""

from __future__ import annotations

import contextlib
import json
import sys

from meerschaum.mcp._dispatch import (
    handle_payload,
    jsonrpc_error,
    PARSE_ERROR,
)
from meerschaum.utils.typing import Any, List, Optional, SuccessTuple

### stdio inherits the launching shell's privileges, so every tool is available.
STDIO_SCOPES: List[str] = ['*']


def _write_message(message: Any, stream) -> None:
    """
    Write one newline-delimited JSON-RPC message and flush.
    """
    stream.write(json.dumps(message, default=str) + '\n')
    stream.flush()


def serve_stdio(
    stdin=None,
    stdout=None,
    scopes: Optional[List[str]] = None,
) -> SuccessTuple:
    """
    Read newline-delimited JSON-RPC messages from stdin and write responses to
    stdout until the stream closes.

    Parameters
    ----------
    stdin: Optional[IO], default None
        The input stream. Defaults to `sys.stdin`.

    stdout: Optional[IO], default None
        The output stream. Defaults to `sys.stdout`.

    scopes: Optional[List[str]], default None
        The scopes to serve with. Defaults to `STDIO_SCOPES` (everything).

    Returns
    -------
    A `SuccessTuple`.
    """
    stdin = stdin if stdin is not None else sys.stdin
    stdout = stdout if stdout is not None else sys.stdout
    scopes = scopes if scopes is not None else STDIO_SCOPES

    for line in stdin:
        line = line.strip()
        if not line:
            continue

        try:
            payload = json.loads(line)
        except Exception:
            _write_message(jsonrpc_error(None, PARSE_ERROR, "Parse error"), stdout)
            continue

        try:
            ### Anything the tool prints goes to stderr, not the protocol channel.
            with contextlib.redirect_stdout(sys.stderr):
                response = handle_payload(payload, scopes)
        except Exception as e:
            ### A crash in dispatch must not kill the server: report it and keep
            ### reading, or the client loses the session over one bad call.
            _write_message(
                jsonrpc_error(
                    (payload.get('id') if isinstance(payload, dict) else None),
                    -32603,
                    f"{type(e).__name__}: {e}",
                ),
                stdout,
            )
            continue

        ### Notifications get no reply.
        if response is None:
            continue

        _write_message(response, stdout)

    return True, "Success"
