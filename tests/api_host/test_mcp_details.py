#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The console's "Connect an agent" details: each MCP client's configuration (pure builders), and
the Tokens page's dialogs which show them.
"""

import json
import shlex
import tomllib

import pytest

from tests.api_host._helpers import AS_VIEWER, run_python, sandbox  # noqa: F401

KEY = 'mrsm-key:YTg1ZTI3NDEtMDAwMC00MDAwLTgwMDAtMDAwMDAwMDAwMDAwOnMzY3IzdCsvPQ=='
URL = 'https://your-api-host'
MCP_URL = 'https://your-api-host/mcp'
BEARER = f'Bearer {KEY}'


def _json(result):
    lines = [line for line in result.stdout.splitlines() if line.startswith('JSON:')]
    assert lines, result.stderr[-3000:]
    return json.loads(lines[-1][len('JSON:'):])


@pytest.fixture(scope='module')
def details():
    """The pure half of the module needs neither Dash nor the extension's API host."""
    import importlib.util
    import pathlib
    path = pathlib.Path(__file__).resolve().parents[2] / 'meerschaum/api/dash/mcp_details.py'
    spec = importlib.util.spec_from_file_location('_mcp_details_under_test', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize('server_url', [URL, URL + '/', URL + '//', MCP_URL, MCP_URL + '/'])
def test_mcp_url_ignores_trailing_slashes(details, server_url):
    assert details.get_mcp_url(server_url) == MCP_URL
    for blocks in details.build_snippets(server_url, KEY).values():
        text = '\n'.join(block['text'] for block in blocks)
        assert MCP_URL in text and MCP_URL + '/' not in text and '/mcp/mcp' not in text


def test_every_client_has_blocks_and_the_first_carries_the_key(details):
    snippets = details.build_snippets(URL, KEY)
    assert list(snippets) == [value for value, _ in details.MCP_CLIENTS]
    for client, blocks in snippets.items():
        assert 1 <= len(blocks) <= details.MAX_BLOCKS, client
        assert all(block['label'] and block['text'] for block in blocks), client
        ### Copying the first block is copying the key; a later block never holds it.
        assert KEY in blocks[0]['text'], client
        assert all(KEY not in block['text'] for block in blocks[1:]), client
        assert details.API_KEY_PLACEHOLDER not in ''.join(b['text'] for b in blocks), client


def test_without_a_key_the_placeholder_stands_in(details):
    for client, blocks in details.build_snippets(URL).items():
        assert details.API_KEY_PLACEHOLDER in blocks[0]['text'], client
        assert 'mrsm-key:' not in ''.join(block['text'] for block in blocks), client
    assert details.build_snippets(URL, '') == details.build_snippets(URL, None)


def test_claude_code_command(details):
    argv = shlex.split(details.claude_code_command(URL + '/', KEY))
    assert argv == [
        'claude', 'mcp', 'add', '--transport', 'http', 'meerschaum', MCP_URL,
        '--header', f'Authorization: {BEARER}',
    ]


def test_claude_code_project_file_reads_the_key_from_the_environment(details):
    export, config = details.build_snippets(URL, KEY)['claude-code-project']
    assert shlex.split(export['text']) == ['export', f'MRSM_API_KEY={KEY}']
    assert config['label'] == '.mcp.json'
    server = json.loads(config['text'])['mcpServers']['meerschaum']
    assert server == {
        'type': 'http',
        'url': MCP_URL,
        'headers': {'Authorization': 'Bearer ${MRSM_API_KEY}'},
    }


def test_claude_desktop_bridges_with_mcp_remote(details):
    server = json.loads(details.claude_desktop_json(URL, KEY))['mcpServers']['meerschaum']
    assert server['command'] == 'npx'
    ### No space inside `args` (the header's value comes from `env`).
    assert server['args'] == ['-y', 'mcp-remote', MCP_URL, '--header', 'Authorization:${AUTH_HEADER}']
    assert server['env'] == {'AUTH_HEADER': BEARER}
    plain = json.loads(details.claude_desktop_json('http://localhost:8000', KEY))
    assert plain['mcpServers']['meerschaum']['args'][-1] == '--allow-http'
    assert plain['mcpServers']['meerschaum']['args'][2] == 'http://localhost:8000/mcp'


def test_codex_config_is_toml_with_a_bearer_token_variable(details):
    export, config = details.build_snippets(URL + '/', KEY)['codex']
    assert shlex.split(export['text']) == ['export', f'MRSM_API_KEY={KEY}']
    assert config['label'] == '~/.codex/config.toml'
    assert tomllib.loads(config['text']) == {
        'mcp_servers': {
            'meerschaum': {'url': MCP_URL, 'bearer_token_env_var': 'MRSM_API_KEY'},
        },
    }


def test_opencode_config(details):
    export, config = details.build_snippets(URL, KEY)['opencode']
    assert shlex.split(export['text']) == ['export', f'MRSM_API_KEY={KEY}']
    doc = json.loads(config['text'])
    assert doc['$schema'] == 'https://opencode.ai/config.json'
    assert doc['mcp']['servers']['meerschaum'] == {
        'type': 'remote',
        'url': MCP_URL,
        'oauth': False,
        'headers': {'Authorization': 'Bearer {env:MRSM_API_KEY}'},
    }


def test_generic_json_and_curl(details):
    server = json.loads(details.generic_json(URL, KEY))['mcpServers']['meerschaum']
    assert server == {'url': MCP_URL, 'headers': {'Authorization': BEARER}}
    assert shlex.split(details.curl_command(URL, KEY)) == [
        'curl', '-H', f'Authorization: {BEARER}', MCP_URL,
    ]


def test_templates_hold_stand_ins_and_fill_like_the_builders(details):
    """What the browser does (replace the URL and key stand-ins) yields the builders' output."""
    templates = details.snippet_templates()
    assert templates['placeholder'] == details.API_KEY_PLACEHOLDER
    assert 'mrsm-key:' not in json.dumps(templates)
    for scheme, base in (('https', URL), ('http', 'http://localhost:8000')):
        entry = templates['schemes'][scheme]
        filled = {
            client: [
                {
                    'label': block['label'],
                    'text': block['text'].replace(entry['url'], base).replace(templates['key'], KEY),
                }
                for block in blocks
            ]
            for client, blocks in entry['clients'].items()
        }
        assert filled == details.build_snippets(base, KEY)
    for token in ('templates.schemes', 'scheme.url', 'templates.key', 'templates.placeholder',
                  'window.location', "'/mcp'"):
        assert token in details.FILL_SNIPPETS_JS


def test_agent_token_scopes_are_read_only_and_clamped(details):
    assert all(scope.endswith(':read') for scope in details.AGENT_TOKEN_SCOPES)
    assert details.agent_token_scopes(['pipes:read', 'pipes:write', 'jobs:read']) == [
        'pipes:read', 'jobs:read',
    ]
    assert details.agent_token_scopes([]) == []


WALK = '''
import json
def walk(component):
    if isinstance(component, (list, tuple)):
        for c in component:
            yield from walk(c)
        return
    if component is None or isinstance(component, (str, int, float)):
        return
    yield component
    yield from walk(getattr(component, 'children', None))

def by_id(tree):
    return {json.dumps(c.id, sort_keys=True) if isinstance(c.id, dict) else c.id: c
            for c in walk(tree) if getattr(c, 'id', None) is not None}

def text(tree):
    return ' '.join(c for comp in walk(tree) for c in (
        [comp.children] if isinstance(getattr(comp, 'children', None), str) else []))
'''


def test_tokens_page_offers_the_mcp_details(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, AS_VIEWER + WALK + '''
from types import SimpleNamespace
from meerschaum._internal.static import STATIC_CONFIG
from meerschaum.api.dash import dash_app, pages
import meerschaum.api.dash.callbacks.tokens as ct
import meerschaum.api.dash.tokens as tokens
import meerschaum.api.dash.mcp_details as mcp_details
from dash.exceptions import PreventUpdate

as_viewer()
ct.get_user_from_session = lambda session_id: None
layout = by_id(pages.tokens.layout)
modal = layout['tokens-mcp-modal']
picker = by_id(modal)
select = picker[json.dumps({'index': 'connect', 'type': 'mcp-client'})]

closed, opened, agent_form = ct.new_agent_token_button_click(1, {'session-id': 'x'})
agent = by_id(agent_form)
_, plain_form = ct.create_tokens_button_click(1, {'session-id': 'x'})
plain = by_id(plain_form)

class FakeToken:
    label, id, secret, expiration, scopes, user = 'agent', 'the-id', 'the-secret', None, ['pipes:read'], None
    def register(self, debug=False):
        return True, 'Success'
    def get_api_key(self):
        return 'mrsm-key:QUJD'
registered = by_id(tokens.build_tokens_register_output_modal(FakeToken()))
class FailedToken(FakeToken):
    def register(self, debug=False):
        return False, 'No.'
failed = by_id(tokens.build_tokens_register_output_modal(FailedToken()))

def raises(fn):
    try:
        fn()
    except PreventUpdate:
        return True
    return False

blueprint = getattr(dash_app, 'blueprint', None)
clientside = [str(cb.f) for cb in getattr(blueprint, 'clientside_callbacks', [])]
print('JSON:' + json.dumps({
    'served': mcp_details.mcp_is_served(),
    'page_ids': sorted(k for k in layout if 'mcp' in k),
    'button': layout['tokens-mcp-button'].children,
    'modal': [modal.fullscreen, modal.scrollable, bool(modal.is_open)],
    'modal_text': text(modal),
    'clients': [o['value'] for o in select.options],
    'client': select.value,
    'connect_key': picker[json.dumps({'index': 'connect', 'type': 'mcp-key'})].data,
    'connect_key_clipboards': [k for k in picker if 'key-clipboard' in k],
    'connect_clipboard_targets': [
        picker[json.dumps({'index': 'connect', 'type': f'mcp-clipboard-{slot}'})].target_id
        for slot in range(mcp_details.MAX_BLOCKS)
    ],
    'opens': [ct.connect_agent_button_click(1), closed, opened],
    'agent_title': text(agent_form[0]),
    'agent_name': agent['tokens-name-input'].value,
    'agent_grant_all': agent['tokens-toggle-scopes-switch'].value,
    'agent_scopes': agent['tokens-scopes-checklist'].value,
    'known_scopes': all(s in STATIC_CONFIG['tokens']['scopes'] for s in mcp_details.AGENT_TOKEN_SCOPES),
    'plain_title': text(plain_form[0]),
    'plain_grant_all': plain['tokens-toggle-scopes-switch'].value,
    'plain_name_is_agent': plain['tokens-name-input'].value == 'agent',
    'registered_key': registered[json.dumps({'index': 'registered', 'type': 'mcp-key'})].data,
    'registered_templates_have_no_key': 'QUJD' not in json.dumps(
        registered[json.dumps({'index': 'registered', 'type': 'mcp-templates'})].data),
    'registered_key_clipboard': registered[
        json.dumps({'index': 'registered', 'type': 'mcp-key-clipboard'})].target_id,
    'registered_close_disabled': registered['tokens-close-register-output-modal-button'].disabled,
    'registered_details': 'tokens-register-details' in registered,
    'failed_mcp': [k for k in failed if 'mcp' in k],
    'close_gate': [
        raises(lambda: ct.enable_close_button(None, [None])),
        raises(lambda: ct.enable_close_button(None, [])),
        list(ct.enable_close_button(None, [1])),
        list(ct.enable_close_button(1, [None])),
    ],
    'fill_callback': (blueprint is None) or (mcp_details.FILL_SNIPPETS_JS in clientside),
}))
'''))
    assert out['served'] is True
    assert out['button'] == 'Connect an agent'
    assert 'tokens-mcp-modal' in out['page_ids'] and 'tokens-mcp-new-token-button' in out['page_ids']
    assert out['modal'] == ['sm-down', True, False]
    for label in ('Connect an agent', 'Client', 'MCP URL', 'Copy', 'New agent token'):
        assert label in out['modal_text'], out['modal_text']
    assert out['clients'][:5] == ['claude-code', 'claude-code-project', 'claude-desktop', 'codex', 'opencode']
    assert out['client'] == 'claude-code'
    ### Without a key: the placeholder, and no Copy button which would count as copying a token.
    assert out['connect_key'] is None and out['connect_key_clipboards'] == []
    assert out['connect_clipboard_targets'] == [
        {'type': f'mcp-code-{slot}', 'index': 'connect'} for slot in range(2)
    ]
    assert out['opens'] == [True, False, True]
    assert out['agent_title'] == 'New agent token' and out['agent_name'] == 'agent'
    assert out['agent_grant_all'] is False
    assert out['agent_scopes'] == ['pipes:read', 'sql:read', 'connectors:read', 'jobs:read', 'logs:read']
    assert out['known_scopes'] is True
    ### The ordinary form is unchanged (and replaces an agent form left behind).
    assert out['plain_title'] == 'New token' and out['plain_grant_all'] is True
    assert out['plain_name_is_agent'] is False
    ### After registration the key is inlined, and copying its block allows closing.
    assert out['registered_key'] == 'mrsm-key:QUJD'
    assert out['registered_templates_have_no_key'] is True
    assert out['registered_key_clipboard'] == {'type': 'mcp-code-0', 'index': 'registered'}
    assert out['registered_close_disabled'] is True and out['registered_details'] is True
    assert out['failed_mcp'] == []
    assert out['close_gate'] == [True, True, [False, True], [False, True]]
    assert out['fill_callback'] is True


def test_agent_scopes_are_clamped_to_the_users_own(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, AS_VIEWER + WALK + '''
import meerschaum.api.dash.tokens as tokens
from meerschaum.core import User
from meerschaum.api import get_api_connector
conn = get_api_connector()
conn.register_user(User('mcp-reader', 'correct-horse-1', attributes={'scopes': ['pipes:read', 'jobs:read', 'pipes:write']}, instance=conn))
form = by_id(tokens.build_tokens_register_input_modal(User('mcp-reader', instance=conn), agent=True))
print('JSON:' + json.dumps({
    'grant_all': form['tokens-toggle-scopes-switch'].value,
    'scopes': form['tokens-scopes-checklist'].value,
}))
'''))
    assert out == {'grant_all': False, 'scopes': ['pipes:read', 'jobs:read']}


def test_mcp_details_are_hidden_when_mcp_is_off(sandbox):
    root, plugins = sandbox
    out = _json(run_python(root, plugins, AS_VIEWER + WALK + '''
from meerschaum.api.dash import dash_app, pages
import meerschaum.api.dash.tokens as tokens
import meerschaum.api.dash.mcp_details as mcp_details

class FakeToken:
    label, id, secret, expiration, scopes, user = 'agent', 'the-id', 'the-secret', None, ['pipes:read'], None
    def register(self, debug=False):
        return True, 'Success'
    def get_api_key(self):
        return 'mrsm-key:QUJD'
print('JSON:' + json.dumps({
    'served': mcp_details.mcp_is_served(),
    'page': [k for k in by_id(pages.tokens.layout) if 'mcp' in k],
    'registered': [k for k in by_id(tokens.build_tokens_register_output_modal(FakeToken())) if 'mcp' in k],
    'create': 'tokens-create-button' in by_id(pages.tokens.layout),
}))
''', extra_env={'MRSM_CONFIG': json.dumps({'api': {'mcp': {'enabled': False}}})}))
    assert out == {'served': False, 'page': [], 'registered': [], 'create': True}
