#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Test the MCP registry, dispatch, scope filtering, and security guards.

These tests are transport- and database-independent — they exercise
`meerschaum.mcp` directly, so they run without Docker or an API server.
"""

import json

import pytest

import meerschaum as mrsm
from meerschaum.mcp import (
    get_tools,
    get_all_resources,
    get_prompts,
    handle_message,
    handle_payload,
    has_scope,
    paginate,
    encode_cursor,
    decode_cursor,
    LATEST_PROTOCOL_VERSION,
    SUPPORTED_PROTOCOL_VERSIONS,
)
from meerschaum.mcp._security import is_read_only_query, is_action_permitted


def rpc(method, params=None, scopes=None, request_id=1):
    """
    Dispatch a JSON-RPC request and return the response.
    """
    message = {'jsonrpc': '2.0', 'id': request_id, 'method': method}
    if params is not None:
        message['params'] = params

    return handle_message(message, scopes)


def test_registry_is_populated():
    """
    The registry must expose tools, resources, and prompts.
    """
    assert get_tools()
    assert get_all_resources()
    assert get_prompts()


def test_every_tool_is_well_formed():
    """
    Every tool needs a scope-checkable schema and an honest set of annotations.
    """
    for name, tool in get_tools().items():
        assert tool.description, f"'{name}' has no description."
        assert tool.input_schema.get('type') == 'object', f"'{name}' has a bad schema."
        assert callable(tool.handler), f"'{name}' has no handler."

        ### A read-only tool must never also be flagged destructive.
        assert not (tool.read_only and tool.destructive), (
            f"'{name}' is both read-only and destructive."
        )

        annotations = tool.annotations
        assert annotations['readOnlyHint'] == tool.read_only
        ### `destructiveHint` is only meaningful for non-read-only tools.
        assert ('destructiveHint' in annotations) is not tool.read_only

        ### Required arguments must actually appear in the schema.
        properties = tool.input_schema.get('properties', {})
        for required_arg in tool.input_schema.get('required', []):
            assert required_arg in properties, (
                f"'{name}' requires '{required_arg}' but does not declare it."
            )


def test_destructive_tools_are_annotated():
    """
    A client relies on `destructiveHint` to prompt before data loss.
    """
    for name in ('clear_pipe', 'drop_pipe', 'delete_pipe'):
        tool = get_tools()[name]
        assert tool.destructive, f"'{name}' must be marked destructive."
        assert not tool.read_only


def test_read_tools_are_read_only():
    """
    Tools which only read must be annotated so clients never prompt for them.
    """
    for name in (
        'list_pipes',
        'get_pipe_data',
        'get_pipe_attributes',
        'get_pipe_stats',
        'read_sql',
    ):
        assert get_tools()[name].read_only, f"'{name}' must be read-only."


def test_has_scope():
    """
    `*` grants everything; otherwise every required scope must be present.
    """
    assert has_scope(['*'], ['pipes:delete'])
    assert has_scope(['pipes:read'], ['pipes:read'])
    assert has_scope(['pipes:read', 'pipes:write'], ['pipes:read', 'pipes:write'])
    assert has_scope([], [])
    assert not has_scope(['pipes:read'], ['pipes:write'])
    assert not has_scope([], ['pipes:read'])
    assert not has_scope(['pipes:read'], ['pipes:read', 'pipes:write'])


def test_initialize_negotiates_protocol_version():
    """
    A supported version is echoed back; anything else falls back to the latest.
    """
    for version in SUPPORTED_PROTOCOL_VERSIONS:
        result = rpc('initialize', {'protocolVersion': version}, ['*'])['result']
        assert result['protocolVersion'] == version

    result = rpc('initialize', {'protocolVersion': '1999-01-01'}, ['*'])['result']
    assert result['protocolVersion'] == LATEST_PROTOCOL_VERSION

    assert result['serverInfo']['name'] == 'meerschaum'
    assert result['serverInfo']['version'] == mrsm.__version__
    assert set(result['capabilities']) == {'tools', 'resources', 'prompts'}
    assert result['instructions']


def test_tools_list_is_filtered_by_scope():
    """
    Tools a caller cannot invoke must be hidden, not advertised then refused.
    """
    all_names = set(get_tools().keys())

    unscoped = {tool['name'] for tool in rpc('tools/list', {}, [])['result']['tools']}
    assert unscoped == set(), "Tools leaked to an unscoped caller."

    everything = {tool['name'] for tool in rpc('tools/list', {}, ['*'])['result']['tools']}
    assert everything == all_names

    read_only = {
        tool['name'] for tool in rpc('tools/list', {}, ['pipes:read'])['result']['tools']
    }
    assert 'list_pipes' in read_only
    assert 'delete_pipe' not in read_only
    assert 'execute_action' not in read_only
    assert 'read_sql' not in read_only


def test_tools_call_enforces_scope():
    """
    Calling a tool without its scope must fail as a tool error, not silently run.
    """
    response = rpc(
        'tools/call',
        {'name': 'delete_pipe', 'arguments': {
            'connector_keys': 'sql:local', 'metric_key': 'nope',
        }},
        ['pipes:read'],
    )
    assert response['result']['isError'] is True
    assert 'scope' in response['result']['content'][0]['text'].lower()


def test_tools_call_validates_arguments():
    """
    A missing required argument must be reported, not raise.
    """
    response = rpc(
        'tools/call',
        {'name': 'get_pipe_data', 'arguments': {'connector_keys': 'sql:local'}},
        ['*'],
    )
    assert response['result']['isError'] is True
    assert 'metric_key' in response['result']['content'][0]['text']


def test_tools_call_unknown_tool():
    """
    An unknown tool is a tool error so the model can correct itself.
    """
    response = rpc('tools/call', {'name': 'no_such_tool', 'arguments': {}}, ['*'])
    assert response['result']['isError'] is True


def test_resources_and_templates_are_separated():
    """
    Static resources and templates are listed by different methods.
    """
    resources = rpc('resources/list', {}, ['*'])['result']['resources']
    templates = rpc('resources/templates/list', {}, ['*'])['result']['resourceTemplates']

    assert all('uri' in res for res in resources)
    assert all('uriTemplate' in template for template in templates)
    assert 'mrsm://pipes' in {res['uri'] for res in resources}
    assert any('{connector_keys}' in t['uriTemplate'] for t in templates)


def test_docs_resource_needs_no_scope():
    """
    `mrsm://docs` teaches a client how to use the server, so it must always
    be readable — including by a caller with no scopes at all.
    """
    response = rpc('resources/read', {'uri': 'mrsm://docs'}, [])
    contents = response['result']['contents'][0]
    assert contents['mimeType'] == 'text/markdown'
    text = contents['text']
    assert 'Meerschaum MCP reference' in text
    ### The scope matrix must be generated into it.
    assert 'pipes:read' in text
    for name in get_tools():
        assert name in text, f"'{name}' is missing from the generated reference."


def test_resource_read_unknown_uri():
    """
    An unknown resource URI is an invalid-params error.
    """
    response = rpc('resources/read', {'uri': 'mrsm://nope'}, ['*'])
    assert 'error' in response
    assert response['error']['code'] == -32602


def test_prompts_render_messages():
    """
    Every prompt must render a well-formed message list from its arguments.
    """
    prompts = rpc('prompts/list', {}, ['*'])['result']['prompts']
    assert {prompt['name'] for prompt in prompts} == set(get_prompts().keys())

    response = rpc(
        'prompts/get',
        {'name': 'explain_pipe', 'arguments': {
            'connector_keys': 'sql:local', 'metric_key': 'weather',
        }},
        ['*'],
    )
    messages = response['result']['messages']
    assert messages[0]['role'] == 'user'
    assert 'sql:local' in messages[0]['content']['text']


def test_prompts_get_validates_arguments():
    """
    An unexpected prompt argument is an invalid-params error.
    """
    response = rpc(
        'prompts/get',
        {'name': 'explain_pipe', 'arguments': {'bogus_argument': 1}},
        ['*'],
    )
    assert 'error' in response
    assert response['error']['code'] == -32602


def test_notifications_get_no_response():
    """
    A notification carries no `id` and must not be answered.
    """
    assert handle_message(
        {'jsonrpc': '2.0', 'method': 'notifications/initialized'}, ['*']
    ) is None


def test_unknown_method():
    """
    An unknown method is a JSON-RPC "method not found".
    """
    response = rpc('bogus/method', {}, ['*'])
    assert response['error']['code'] == -32601


def test_ping():
    """
    `ping` must succeed with an empty result.
    """
    assert rpc('ping', {}, [])['result'] == {}


def test_batch_payload():
    """
    A batch returns one response per request, skipping notifications.
    """
    responses = handle_payload(
        [
            {'jsonrpc': '2.0', 'id': 1, 'method': 'ping'},
            {'jsonrpc': '2.0', 'method': 'notifications/initialized'},
            {'jsonrpc': '2.0', 'id': 2, 'method': 'tools/list'},
        ],
        ['*'],
    )
    assert len(responses) == 2
    assert {response['id'] for response in responses} == {1, 2}

    ### A batch of only notifications needs no reply at all.
    assert handle_payload(
        [{'jsonrpc': '2.0', 'method': 'notifications/initialized'}], ['*']
    ) is None


def test_pagination_round_trip():
    """
    Cursors must walk a list exactly once and then stop.
    """
    items = list(range(250))
    seen, cursor, pages = [], None, 0
    while True:
        page, cursor = paginate(items, cursor, page_size=100)
        seen.extend(page)
        pages += 1
        if cursor is None:
            break
        assert pages < 10, "Pagination failed to terminate."

    assert seen == items
    assert pages == 3


def test_pagination_exact_multiple():
    """
    When the last page exactly fills, the cursor must still terminate.
    """
    page, cursor = paginate(list(range(100)), None, page_size=100)
    assert len(page) == 100
    assert cursor is None


def test_cursor_round_trip():
    """
    Cursors are opaque but must decode to what they encoded.
    """
    assert decode_cursor(encode_cursor(42)) == 42
    assert decode_cursor(None) == 0

    for bad_cursor in ('not-base64!!', encode_cursor(-1).replace('=', 'A'), 'e30='):
        with pytest.raises(ValueError):
            decode_cursor(bad_cursor)


@pytest.mark.parametrize(
    'query',
    [
        'SELECT * FROM t',
        "select a from b where c = 'please delete everything'",
        'SELECT * FROM t -- ; DROP TABLE u',
        'select 1 /* ; drop table u */',
        'explain select * from t',
        'show tables',
        'table mrsm.pipes',
        'my_table',
        'mrsm.pipes',
        '"mrsm"."pipes"',
        '[dbo].[pipes]',
        'SELECT * FROM t;',
    ],
)
def test_read_sql_allows_reads(query: str):
    """
    Legitimate read queries and bare table names must be accepted.
    """
    permitted, reason = is_read_only_query(query)
    assert permitted, f"Wrongly refused: {query} ({reason})"


@pytest.mark.parametrize(
    'query',
    [
        'DROP TABLE users',
        'UPDATE t SET a = 1',
        'DELETE FROM t',
        'TRUNCATE t',
        'SELECT 1; DROP TABLE users',
        'WITH x AS (SELECT 1) INSERT INTO y SELECT * FROM x',
        'SELECT * INTO newtbl FROM t',
        "COPY t TO PROGRAM 'sh -c whoami'",
        ### A comment marker inside a string literal must not hide what follows it.
        "SELECT '-- x' ; DROP TABLE t",
        "SELECT '#' ; DROP TABLE t",
        'SELECT $$--$$; DROP TABLE t',
        ### MySQL executes the body of `/*! ... */`.
        "SELECT 1 /*! INTO OUTFILE '/tmp/pwn' */",
        ### Mutations expressed as function calls.
        "SELECT dblink_exec('dbname=x', 'DROP TABLE t')",
        "SELECT lo_export(1, '/tmp/x')",
        "SELECT pg_read_file('/etc/passwd')",
        "SELECT setval('s', 1)",
        "EXEC xp_cmdshell 'whoami'",
        "SELECT * FROM t WHERE a = 'unterminated",
        'GRANT ALL ON t TO PUBLIC',
        'SET ROLE postgres',
        'ALTER TABLE t ADD COLUMN c int',
        'CREATE TABLE t (a int)',
        '',
        '   ',
    ],
)
def test_read_sql_refuses_writes(query: str):
    """
    `read_sql` runs under a read scope, so anything that writes must be refused.
    """
    permitted, reason = is_read_only_query(query)
    assert not permitted, f"Wrongly allowed: {query}"
    assert reason


def test_read_sql_tool_refuses_writes_before_connecting():
    """
    The guard must run before the connector is resolved, so a bad query cannot
    reach the database even if the connector keys are valid.
    """
    response = rpc(
        'tools/call',
        {'name': 'read_sql', 'arguments': {
            'connector_keys': 'sql:local', 'query': 'DROP TABLE users',
        }},
        ['*'],
    )
    assert response['result']['isError'] is True
    assert 'DROP' in response['result']['content'][0]['text']


@pytest.fixture(scope='module')
def sqlite_instance(tmp_path_factory):
    """
    Return a throwaway SQLite instance connector, so these tests need no server.
    """
    return mrsm.get_connector(
        'sql',
        'test_mcp_protected',
        flavor='sqlite',
        database=(tmp_path_factory.mktemp('mcp') / 'mcp.db').as_posix(),
    )


@pytest.mark.parametrize('table', ['mrsm_users', 'mrsm_tokens', 'mrsm_pipes', 'mrsm_plugins'])
def test_pipe_tools_refuse_protected_tables(table: str, sqlite_instance):
    """
    A pipe pointing at an instance table must not be read, written, or
    registered through MCP: `mrsm_users` holds password hashes.
    """
    from meerschaum.mcp._tools import _check_protected_target
    with pytest.raises(PermissionError, match=table):
        _check_protected_target(
            mrsm.Pipe(
                'mcp', 'protected',
                instance=sqlite_instance,
                parameters={'target': table},
            )
        )

    ### Registering a pipe which targets a protected table must be refused.
    register_response = rpc(
        'tools/call',
        {'name': 'register_pipe', 'arguments': {
            'connector_keys': 'mcp',
            'metric_key': 'protected',
            'instance_keys': str(sqlite_instance),
            'parameters': {'target': table},
        }},
        ['*'],
    )
    assert register_response['result']['isError'] is True
    assert table in register_response['result']['content'][0]['text']

    ### An edit must not be able to re-point a pipe at a protected table either.
    edit_response = rpc(
        'tools/call',
        {'name': 'edit_pipe', 'arguments': {
            'connector_keys': 'mcp',
            'metric_key': 'protected',
            'instance_keys': str(sqlite_instance),
            'parameters': {'target': table},
        }},
        ['*'],
    )
    assert edit_response['result']['isError'] is True
    assert table in edit_response['result']['content'][0]['text']


def test_read_sql_refuses_protected_tables():
    """
    `read_sql` must not be a way around the protected-table guard.
    """
    response = rpc(
        'tools/call',
        {'name': 'read_sql', 'arguments': {
            'connector_keys': 'sql:local', 'query': 'SELECT * FROM mrsm_users',
        }},
        ['*'],
    )
    assert response['result']['isError'] is True
    assert 'mrsm_users' in response['result']['content'][0]['text']


def test_clear_and_deduplicate_require_delete_scope():
    """
    Both tools delete rows, so `pipes:write` alone must not reach them —
    the REST route for clearing rows requires `pipes:delete`.
    """
    for tool_name in ('clear_pipe', 'deduplicate_pipe'):
        assert 'pipes:delete' in get_tools()[tool_name].scopes

    write_only_tools = set(_visible_tool_names(['pipes:write']))
    assert 'clear_pipe' not in write_only_tools
    assert 'deduplicate_pipe' not in write_only_tools


def test_read_sql_requires_its_own_scope():
    """
    Executing SQL is a larger privilege than listing connector labels, which is
    all `connectors:read` grants over REST.
    """
    assert get_tools()['read_sql'].scopes == ['sql:read']
    assert 'read_sql' not in set(_visible_tool_names(['connectors:read']))
    assert 'read_sql' in set(_visible_tool_names(['sql:read']))


def _visible_tool_names(scopes):
    """
    Return the names of the tools visible to `scopes`.
    """
    from meerschaum.mcp import get_visible_tools
    return list(get_visible_tools(scopes).keys())


def test_execute_action_refuses_denied_subactions():
    """
    A denylisted action must not be reachable as another action's subaction,
    e.g. `start job` with `{"action": ["sh", ...]}`.
    """
    for arguments in (
        {'action': 'start job', 'kwargs': {'action': ['sh', 'whoami'], 'name': 'x', 'yes': True}},
        {'action': 'start jobs sh whoami', 'kwargs': {'yes': True}},
    ):
        response = rpc('tools/call', {'name': 'execute_action', 'arguments': arguments}, ['*'])
        result = json.loads(response['result']['content'][0]['text'])
        assert result['success'] is False, f"Wrongly allowed: {arguments}"
        assert 'denylist' in result['message']


def test_stdio_transport_keeps_stdout_clean():
    """
    stdout is the protocol channel, so a tool which prints (actions and `info()`
    use plain `print()`) must not corrupt it.
    """
    import io
    from meerschaum.mcp._stdio import serve_stdio

    request = {
        'jsonrpc': '2.0',
        'id': 1,
        'method': 'tools/call',
        'params': {'name': 'execute_action', 'arguments': {'action': 'show version'}},
    }
    stdout = io.StringIO()
    success, _ = serve_stdio(stdin=io.StringIO(json.dumps(request) + '\n'), stdout=stdout)
    assert success

    lines = [line for line in stdout.getvalue().splitlines() if line.strip()]
    assert len(lines) == 1, f"Non-protocol output on stdout: {lines}"
    assert json.loads(lines[0])['id'] == 1


def test_notification_shaped_request_gets_no_response():
    """
    A message without an `id` is a notification: responding to it with
    `"id": null` is a protocol error for strict clients.
    """
    assert handle_message({'jsonrpc': '2.0', 'method': 'tools/list'}, ['*']) is None


def test_code_execution_actions_are_denied_by_default():
    """
    `sh`, `os`, and `python` execute arbitrary code on the host.
    """
    for action_name in ('sh', 'os', 'python'):
        permitted, reason = is_action_permitted(action_name)
        assert not permitted, f"'{action_name}' must be denied by default."
        assert reason

    permitted, _ = is_action_permitted('sync')
    assert permitted, "'sync' must be permitted by default."


def test_execute_action_refuses_denied_actions():
    """
    The denylist must be enforced by the tool, not only by the helper.
    """
    response = rpc(
        'tools/call',
        {'name': 'execute_action', 'arguments': {'action': 'sh'}},
        ['*'],
    )
    result = json.loads(response['result']['content'][0]['text'])
    assert result['success'] is False
    assert 'denylist' in result['message']


def test_execute_action_rejects_unknown_action():
    """
    An unknown action must report back rather than raise.
    """
    response = rpc(
        'tools/call',
        {'name': 'execute_action', 'arguments': {'action': 'not_a_real_action'}},
        ['*'],
    )
    result = json.loads(response['result']['content'][0]['text'])
    assert result['success'] is False


def test_read_only_server_hides_write_tools(monkeypatch):
    """
    Read-only mode must hide every modifying tool, even from a `*` caller.
    """
    monkeypatch.setattr('meerschaum.mcp.is_read_only_server', lambda: True)

    names = {tool['name'] for tool in rpc('tools/list', {}, ['*'])['result']['tools']}
    assert names, "Read-only mode hid everything, including reads."
    for name in names:
        assert get_tools()[name].read_only, f"'{name}' leaked in read-only mode."

    assert 'delete_pipe' not in names
    assert 'sync_pipe' not in names
    assert 'list_pipes' in names

    ### And calling one anyway must be refused.
    response = rpc(
        'tools/call',
        {'name': 'delete_pipe', 'arguments': {
            'connector_keys': 'sql:local', 'metric_key': 'nope',
        }},
        ['*'],
    )
    assert response['result']['isError'] is True
    assert 'read-only' in response['result']['content'][0]['text'].lower()


def test_structured_content_matches_text():
    """
    A tool declaring an output schema must return `structuredContent` which
    agrees with its text block.

    Uses a throwaway SQLite instance so the test needs no running database.
    """
    from meerschaum.config._paths import ROOT_DIR_PATH

    data_path = ROOT_DIR_PATH / 'data'
    data_path.mkdir(exist_ok=True)
    conn = mrsm.get_connector(
        'sql', 'test_mcp_sqlite',
        database=str(data_path / 'test_mcp.db'),
        flavor='sqlite',
    )

    response = rpc(
        'tools/call',
        {'name': 'list_pipes', 'arguments': {'instance_keys': str(conn)}},
        ['*'],
    )
    result = response['result']
    assert result['isError'] is False, result['content'][0]['text']
    assert 'structuredContent' in result
    assert result['structuredContent'] == json.loads(result['content'][0]['text'])
    assert 'pipes' in result['structuredContent']
    assert 'next_cursor' in result['structuredContent']


def test_tools_without_output_schema_omit_structured_content():
    """
    `structuredContent` must only appear when the tool declared an output
    schema, since a client may validate one against the other.
    """
    for name, tool in get_tools().items():
        if tool.output_schema is None:
            assert 'structuredContent' not in (tool.to_dict()), name
            assert 'outputSchema' not in tool.to_dict(), name
        else:
            assert tool.to_dict()['outputSchema'] == tool.output_schema


def test_generated_docs_are_current():
    """
    The published tool reference is generated, so it must not drift.
    """
    import pathlib
    import subprocess
    import sys

    repo_root = pathlib.Path(__file__).resolve().parent.parent
    script = repo_root / 'scripts' / 'generate_mcp_docs.py'
    if not script.exists():
        pytest.skip("The docs generator is not present in this checkout.")

    completed = subprocess.run(
        [sys.executable, script.as_posix(), '--check'],
        capture_output=True,
        text=True,
        cwd=repo_root.as_posix(),
    )
    assert completed.returncode == 0, (
        f"{completed.stdout}\n{completed.stderr}"
    )


def test_prompts_get_requires_required_arguments():
    """
    A prompt whose required arguments are missing must fail rather than render
    a template with holes in it.
    """
    response = rpc('prompts/get', {'name': 'explain_pipe', 'arguments': {}}, ['*'])
    assert 'error' in response
    assert response['error']['code'] == -32602
    assert 'connector_keys' in response['error']['message']
