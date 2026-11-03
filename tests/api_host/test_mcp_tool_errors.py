#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
A plugin MCP tool that raises while it runs is a tool execution error, never "Invalid arguments"
(the Rust server forwards plugin tools to the host's `/_mrsm/host/mcp` and returns its reply as
is). The error names the exception type, its message and the innermost plugin frame that raised
it. Only arguments the tool's function cannot accept are reported as invalid arguments.
"""

import json

from tests.api_host._helpers import run_python, write_plugin, sandbox  # noqa: F401

PLUGIN = '''
import json
from meerschaum.mcp import tool


def _flag(value):
    import pandas as pd
    if pd.NA:
        return value
    return value


@tool(
    'mcperr_flag',
    'Raise inside the tool (a pandas NA in a boolean context).',
    scopes=['mcperr:read'],
    read_only=True,
    input_schema={'type': 'object', 'properties': {'strict': {'type': 'boolean'}}},
)
def flag(strict: bool = False):
    return {'flag': _flag(strict)}


@tool(
    'mcperr_encode',
    'Raise from the standard library, called by the tool.',
    scopes=['mcperr:read'],
    read_only=True,
    input_schema={'type': 'object', 'properties': {}},
)
def encode():
    return {'text': json.dumps(object())}
'''


def _lines(source: str, needle: str) -> int:
    return next(i for i, line in enumerate(source.splitlines(), 1) if needle in line)


def test_plugin_tool_errors_are_execution_errors(sandbox):
    root, plugins = sandbox
    write_plugin(plugins, 'mcperr', PLUGIN)
    code = """
import time, json
import jwt
from starlette.testclient import TestClient
from meerschaum.api._oauth2 import SECRET
from meerschaum.api.host import build_app, MCP_PATH
client = TestClient(build_app())
token = jwt.encode(
    {'mrsm_host': 'mcp', 'scopes': ['mcperr:read'], 'exp': int(time.time()) + 60},
    SECRET, algorithm='HS256',
)
out = {}
for key, name, arguments in (
    ('no_arguments', 'mcperr_flag', {}),
    ('valid_boolean', 'mcperr_flag', {'strict': True}),
    ('unknown_argument', 'mcperr_flag', {'nope': 1}),
    ('stdlib', 'mcperr_encode', {}),
):
    response = client.post(
        MCP_PATH,
        json={'jsonrpc': '2.0', 'id': 1, 'method': 'tools/call',
              'params': {'name': name, 'arguments': arguments}},
        headers={'x-mrsm-host-token': token},
    )
    out[key] = response.json()
print('JSON:' + json.dumps(out))
"""
    result = run_python(root, plugins, code)
    assert result.returncode == 0, result.stderr[-4000:]
    line = [l for l in result.stdout.splitlines() if l.startswith('JSON:')][-1]
    out = json.loads(line[len('JSON:'):])

    raised_at = f"mcperr/__init__.py:{_lines(PLUGIN, 'if pd.NA:')}"
    for key in ('no_arguments', 'valid_boolean'):
        reply = out[key]
        assert 'error' not in reply, reply
        assert reply['result']['isError'] is True, reply
        text = reply['result']['content'][0]['text']
        assert 'Invalid arguments' not in text, text
        assert text.startswith(
            "Error in tool 'mcperr_flag': TypeError: boolean value of NA is ambiguous"
        ), text
        assert raised_at in text, text
        assert 'in _flag' in text, text

    unknown = out['unknown_argument']['result']
    assert unknown['isError'] is True
    assert unknown['content'][0]['text'].startswith("Invalid arguments for 'mcperr_flag':"), unknown

    ### The innermost *plugin* frame, not the standard library's.
    stdlib = out['stdlib']['result']
    assert stdlib['isError'] is True
    text = stdlib['content'][0]['text']
    assert 'Invalid arguments' not in text, text
    assert text.startswith("Error in tool 'mcperr_encode': TypeError: "), text
    assert f"mcperr/__init__.py:{_lines(PLUGIN, 'json.dumps(object())')}" in text, text
    assert 'in encode' in text, text
    assert 'encoder.py' not in text, text
