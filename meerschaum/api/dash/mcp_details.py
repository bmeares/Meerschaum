#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The configuration an AI agent needs to connect to this instance's MCP server (`<server>/mcp`),
ready to paste into each client.

The first part of this module is pure (no Dash): `build_snippets(server_url, api_key)` returns,
for every client, the blocks to paste (a shell line, a configuration file). The second part
builds the console's client picker, which fills those blocks in the browser from templates, so
that the URL is the one the browser reached the console at (the right one behind a reverse proxy).

The bearer credential is a token's API key (`Token.get_api_key()`): `mrsm-key:` followed by the
base64 of `<client id>:<client secret>`.

Each client's format was checked against its documentation:

- Claude Code: https://code.claude.com/docs/en/mcp
  (`claude mcp add --transport http <name> <url> --header "..."`; `.mcp.json` entries need
  `"type": "http"` and expand `${VAR}` in `url` and `headers`).
- Claude Desktop: `claude_desktop_config.json` only starts local (stdio) servers, so the
  remote server is bridged with `mcp-remote`: https://github.com/geelen/mcp-remote
  (`--header`, with the value in `env` because some clients do not escape spaces in `args`;
  `--allow-http` for a plain HTTP server). Custom connectors with request headers are the
  in-app alternative: https://claude.com/docs/connectors/custom/remote-mcp
- Codex: https://developers.openai.com/codex/mcp
  (`[mcp_servers.<name>]` with `url` and `bearer_token_env_var` in `~/.codex/config.toml`).
- OpenCode: https://opencode.ai/docs/mcp-servers/ and https://opencode.ai/config.json
  (`mcp.servers.<name>` with `"type": "remote"`, `headers`, `{env:NAME}` substitution and
  `"oauth": false` for a header credential; OpenCode 1.x put the server directly under `mcp`).
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

#: What stands for the API key when there is none to show.
API_KEY_PLACEHOLDER: str = '<API key>'

#: The server's name in every client's configuration.
SERVER_NAME: str = 'meerschaum'

#: The environment variable the snippets read the API key from.
API_KEY_ENV_VAR: str = 'MRSM_API_KEY'

#: The picker's clients, in order: `(value, label)`.
MCP_CLIENTS: List[tuple] = [
    ('claude-code', 'Claude Code'),
    ('claude-code-project', 'Claude Code (.mcp.json)'),
    ('claude-desktop', 'Claude Desktop'),
    ('codex', 'Codex'),
    ('opencode', 'OpenCode'),
    ('generic', 'Generic (mcpServers JSON)'),
    ('curl', 'curl'),
]

#: Read-only scopes suggested for an agent's token (clamped to the user's own).
AGENT_TOKEN_SCOPES: List[str] = [
    'pipes:read',
    'sql:read',
    'connectors:read',
    'jobs:read',
    'logs:read',
]

#: The label the New agent token form starts with.
AGENT_TOKEN_LABEL: str = 'agent'


def get_mcp_url(server_url: str) -> str:
    """Return the MCP endpoint of the server at `server_url` (with or without a trailing slash)."""
    base = str(server_url or '').strip().rstrip('/')
    return base if base.endswith('/mcp') else base + '/mcp'


def _key(api_key: Optional[str]) -> str:
    return str(api_key) if api_key else API_KEY_PLACEHOLDER


def _json(doc: Dict[str, Any]) -> str:
    return json.dumps(doc, indent=2)


def _export_block(api_key: Optional[str]) -> Dict[str, str]:
    """The shell line that holds the API key, for the clients which read it from the environment."""
    return {'label': 'Shell', 'text': f'export {API_KEY_ENV_VAR}="{_key(api_key)}"'}


def claude_code_command(server_url: str, api_key: Optional[str] = None) -> str:
    """The `claude mcp add` command (Claude Code keeps the header in its own settings)."""
    return (
        f'claude mcp add --transport http {SERVER_NAME} {get_mcp_url(server_url)} '
        f'--header "Authorization: Bearer {_key(api_key)}"'
    )


def claude_code_project_json(server_url: str) -> str:
    """A project's `.mcp.json`: the key stays in the environment (`${MRSM_API_KEY}`)."""
    return _json({
        'mcpServers': {
            SERVER_NAME: {
                'type': 'http',
                'url': get_mcp_url(server_url),
                'headers': {'Authorization': 'Bearer ${' + API_KEY_ENV_VAR + '}'},
            },
        },
    })


def claude_desktop_json(server_url: str, api_key: Optional[str] = None) -> str:
    """
    `claude_desktop_config.json`: Claude Desktop starts `mcp-remote`, which bridges to the server.
    The header's value sits in `env` (no space in `args`, which some clients do not escape).
    """
    mcp_url = get_mcp_url(server_url)
    args = ['-y', 'mcp-remote', mcp_url, '--header', 'Authorization:${AUTH_HEADER}']
    if mcp_url.lower().startswith('http://'):
        args.append('--allow-http')
    return _json({
        'mcpServers': {
            SERVER_NAME: {
                'command': 'npx',
                'args': args,
                'env': {'AUTH_HEADER': f'Bearer {_key(api_key)}'},
            },
        },
    })


def codex_toml(server_url: str) -> str:
    """The `~/.codex/config.toml` table: the bearer token is read from the environment."""
    return (
        f'[mcp_servers.{SERVER_NAME}]\n'
        f'url = {json.dumps(get_mcp_url(server_url))}\n'
        f'bearer_token_env_var = {json.dumps(API_KEY_ENV_VAR)}'
    )


def opencode_json(server_url: str) -> str:
    """`opencode.json`: a remote server whose header reads the key from the environment."""
    return _json({
        '$schema': 'https://opencode.ai/config.json',
        'mcp': {
            'servers': {
                SERVER_NAME: {
                    'type': 'remote',
                    'url': get_mcp_url(server_url),
                    'oauth': False,
                    'headers': {'Authorization': 'Bearer {env:' + API_KEY_ENV_VAR + '}'},
                },
            },
        },
    })


def generic_json(server_url: str, api_key: Optional[str] = None) -> str:
    """The `mcpServers` shape most other clients read (Cursor's `mcp.json`, for one)."""
    return _json({
        'mcpServers': {
            SERVER_NAME: {
                'url': get_mcp_url(server_url),
                'headers': {'Authorization': f'Bearer {_key(api_key)}'},
            },
        },
    })


def curl_command(server_url: str, api_key: Optional[str] = None) -> str:
    """`GET /mcp` lists what the token can reach."""
    return f'curl -H "Authorization: Bearer {_key(api_key)}" {get_mcp_url(server_url)}'


def build_snippets(
    server_url: str,
    api_key: Optional[str] = None,
) -> Dict[str, List[Dict[str, str]]]:
    """
    Return every client's blocks to paste, keyed by the client's value in `MCP_CLIENTS`. A block
    is `{'label': where it goes, 'text': what to paste}`. The first block of every client carries
    the API key (or `API_KEY_PLACEHOLDER`); a second one never does.
    """
    export = _export_block(api_key)
    return {
        'claude-code': [
            {'label': 'Terminal', 'text': claude_code_command(server_url, api_key)},
        ],
        'claude-code-project': [
            export,
            {'label': '.mcp.json', 'text': claude_code_project_json(server_url)},
        ],
        'claude-desktop': [
            {
                'label': 'claude_desktop_config.json',
                'text': claude_desktop_json(server_url, api_key),
            },
        ],
        'codex': [
            export,
            {'label': '~/.codex/config.toml', 'text': codex_toml(server_url)},
        ],
        'opencode': [
            export,
            {'label': 'opencode.json', 'text': opencode_json(server_url)},
        ],
        'generic': [
            {'label': 'mcp.json', 'text': generic_json(server_url, api_key)},
        ],
        'curl': [
            {'label': 'Terminal', 'text': curl_command(server_url, api_key)},
        ],
    }


### The console's picker.

#: Stand-ins the browser replaces: the server's URL (one per scheme, since a plain HTTP server
#: changes a snippet) and the API key.
_URL_TOKENS: Dict[str, str] = {
    'https': 'https://mrsm-server-url.invalid',
    'http': 'http://mrsm-server-url.invalid',
}
_KEY_TOKEN: str = '@@MRSM_API_KEY@@'

#: The most blocks a client has (the picker has that many slots).
MAX_BLOCKS: int = 2

#: The kind (`mcp_component_id`) of a Copy button whose block holds a real API key: copying it
#: counts as copying the token (the token-created dialog may then be closed).
KEY_CLIPBOARD_KIND: str = 'key-clipboard'


def snippet_templates() -> Dict[str, Any]:
    """
    The snippets with stand-ins for the server's URL and the API key, for `FILL_SNIPPETS_JS` to
    fill in the browser. They hold no secret.
    """
    return {
        'key': _KEY_TOKEN,
        'placeholder': API_KEY_PLACEHOLDER,
        'schemes': {
            scheme: {'url': url_token, 'clients': build_snippets(url_token, _KEY_TOKEN)}
            for scheme, url_token in _URL_TOKENS.items()
        },
    }


#: The clientside callback behind every picker: `(client, templates, api_key)` to the MCP URL and
#: each slot's label, text and visibility. The server's URL is the page's own origin (and the
#: path the console is mounted under, if any), so a proxied deployment shows its public address.
FILL_SNIPPETS_JS: str = """
function(client, templates, apiKey) {
    if (!templates) {
        return window.dash_clientside.no_update;
    }
    var loc = window.location;
    var dashAt = loc.pathname.indexOf('/dash');
    var base = loc.origin + (dashAt > 0 ? loc.pathname.slice(0, dashAt) : '');
    var scheme = templates.schemes[loc.protocol === 'http:' ? 'http' : 'https'];
    var blocks = scheme.clients[client] || [];
    var key = apiKey || templates.placeholder;
    var fill = function(text) {
        return text.split(scheme.url).join(base).split(templates.key).join(key);
    };
    var first = blocks[0] || {label: '', text: ''};
    var second = blocks[1];
    return [
        base + '/mcp',
        first.label,
        fill(first.text),
        second ? second.label : '',
        second ? fill(second.text) : '',
        second ? {} : {'display': 'none'}
    ];
}
"""


def mcp_is_served() -> bool:
    """
    Return whether the API server serves `/mcp`: `api:mcp:enabled` and the `mcp` route group is
    allowed (the server registers the route under the same two conditions).
    """
    try:
        from meerschaum.api import mcp_enabled, route_group_is_allowed
        return bool(mcp_enabled) and bool(route_group_is_allowed('mcp'))
    except Exception:
        import meerschaum as mrsm
        return bool(mrsm.get_config('api', 'mcp', 'enabled', warn=False))


def agent_token_scopes(own_scopes: List[str]) -> List[str]:
    """Return the scopes the New agent token form starts with, out of the user's `own_scopes`."""
    return [scope for scope in AGENT_TOKEN_SCOPES if scope in own_scopes]


def mcp_component_id(kind: str, index: str) -> Dict[str, str]:
    """The ID of one of a picker's components (`index`: which picker)."""
    return {'type': f'mcp-{kind}', 'index': index}


def build_mcp_client_picker(index: str, api_key: Optional[str] = None) -> Any:
    """
    Return a client picker: the MCP URL, the Client select and the selected client's blocks, each
    with its Copy button. `index` tells the pickers apart (`connect` on the Tokens page,
    `registered` after a token is created); `api_key` is inlined when given, else the snippets
    show `API_KEY_PLACEHOLDER`.
    """
    from meerschaum.api import CHECK_UPDATE
    from meerschaum.utils.packages import attempt_import, import_html, import_dcc
    dcc, html = import_dcc(check_update=CHECK_UPDATE), import_html(check_update=CHECK_UPDATE)
    dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)

    def cid(kind: str) -> Dict[str, str]:
        return mcp_component_id(kind, index)

    def block(slot: int) -> Any:
        return html.Div(
            [
                html.Div(
                    [
                        html.Span(id=cid(f'label-{slot}'), className='mcp-block-label'),
                        dcc.Clipboard(
                            'Copy',
                            copied_children='Copied',
                            target_id=cid(f'code-{slot}'),
                            ### The first block carries the API key, when there is one.
                            id=cid(
                                KEY_CLIPBOARD_KIND if (api_key and slot == 0)
                                else f'clipboard-{slot}'
                            ),
                            title='Copy',
                            className='mcp-copy',
                        ),
                    ],
                    className='mcp-block-header',
                ),
                html.Pre(id=cid(f'code-{slot}'), className='mcp-code'),
            ],
            id=cid(f'block-{slot}'),
            className='mcp-block',
        )

    return html.Div(
        [
            dbc.Row(
                [
                    dbc.Col(
                        [
                            dbc.Label('Client'),
                            dbc.Select(
                                options=[
                                    {'label': label, 'value': value}
                                    for value, label in MCP_CLIENTS
                                ],
                                value=MCP_CLIENTS[0][0],
                                id=cid('client'),
                            ),
                        ],
                        xs=12,
                        md=5,
                    ),
                    dbc.Col(
                        [
                            dbc.Label('MCP URL'),
                            html.Pre(id=cid('url'), className='mcp-code mcp-url'),
                        ],
                        xs=12,
                        md=7,
                    ),
                ],
                class_name='g-3',
            ),
            *[block(slot) for slot in range(MAX_BLOCKS)],
            dcc.Store(id=cid('templates'), data=snippet_templates()),
            dcc.Store(id=cid('key'), data=(api_key or None)),
        ],
        className='mcp-details',
    )


def build_mcp_connect_modal() -> Any:
    """Return the Tokens page's "Connect an agent" dialog (snippets with the key's placeholder)."""
    from meerschaum.api import CHECK_UPDATE
    from meerschaum.utils.packages import attempt_import
    dbc = attempt_import('dash_bootstrap_components', lazy=False, check_update=CHECK_UPDATE)
    return dbc.Modal(
        [
            dbc.ModalHeader(dbc.ModalTitle('Connect an agent')),
            dbc.ModalBody(build_mcp_client_picker('connect')),
            dbc.ModalFooter([
                dbc.Button('New agent token', color='primary', id='tokens-mcp-new-token-button'),
            ]),
        ],
        id='tokens-mcp-modal',
        size='lg',
        fullscreen='sm-down',
        scrollable=True,
        is_open=False,
        class_name='tokens-modal mcp-modal',
    )
