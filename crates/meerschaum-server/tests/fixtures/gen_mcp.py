#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Record the MCP contract of the Python v4 API server (`POST /mcp`, `GET /mcp`) for the Rust MCP
server in `meerschaum-server` (§7.2, §14).

Starts the Python API server on a free port against a fresh sqlite instance in a temporary root
(see `_pyserver.py`), logs in with several scope sets, and records the JSON-RPC exchanges:
`initialize`, `ping`, notifications and batches, `tools/list` per scope set (the per-tool scope
rule), a representative `tools/call` per tool, resources, prompts, the protocol errors, and the
read-only server mode (a second server started with `api:mcp:read_only: true`).

The recording format and the normalization are those of `gen_contract.py` (credentials become
`{slot}` placeholders, volatile keys `<volatile>`, ISO timestamps one canonical form, the
version `<version>`). Each exchange also carries `server`: `default` or `read_only`.

The API instance is also the configured default instance (`meerschaum:instance`), as in a default
`start api`, so the `mrsm://pipes` resource (which v4 reads from `mrsm.get_pipes()`) lists the
same pipes as the tools.

Usage (from the repository root):

    scripts/parity/run.sh crates/meerschaum-server/tests/fixtures/gen_mcp.py

All data is synthetic.
"""

import os
import sys
import json
import base64
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import _pyserver  # noqa: E402

_pyserver.reexec_in_temp_root(__file__)

### Phase 2: set before `meerschaum` is imported (here and in the server's environment).
os.environ['MRSM_PATCH'] = json.dumps({'meerschaum': {'instance': _pyserver.INSTANCE}})

import gen_contract as gc  # noqa: E402  (shares the recorder and the normalization)

OUT = _pyserver.HERE / 'mcp.json'
SERVER = {'read_only_from': None}
NEXT_ID = {'id': 0}

SQLITE_DATA = (
    "Row values, dtypes and messages are the instance's own (sqlite vs. the in-memory test "
    "instance)."
)
SOURCE_SYNC = (
    "Syncing from a source connector, verification and deduplication run in other crates "
    "(the server's `McpBackend` seam); only the result shape is compared."
)
ACTION_RUN = "The action's own output; the replay's actions backend is a stub."


def next_id() -> int:
    NEXT_ID['id'] += 1
    return NEXT_ID['id']


def main_record(port: int):
    import requests
    import meerschaum as mrsm
    gc.VERSION = mrsm.__version__
    gc.install_recorder()
    base = f"http://127.0.0.1:{port}"

    def raw(method, path, **kw):
        with requests.Session() as session:
            return session.request(method, base + path, **kw)

    def token(slot):
        return next(k for k, v in gc.AUTH_SLOTS.items() if v == slot)

    def mcp(slot, method=None, params=None, *, payload=None, data=None, notify=False):
        headers = {'Authorization': token(slot)} if slot else {}
        if data is not None:
            return raw(
                'POST', '/mcp', data=data,
                headers={**headers, 'Content-Type': 'application/json'},
            )
        if payload is None:
            payload = {'jsonrpc': '2.0', 'method': method}
            if not notify:
                payload['id'] = next_id()
            if params is not None:
                payload['params'] = params
        return raw('POST', '/mcp', json=payload, headers=headers)

    def call(slot, name, arguments=None, **kw):
        params = {'name': name}
        if arguments is not None:
            params['arguments'] = arguments
        return mcp(slot, 'tools/call', params, **kw)

    step = gc.step
    login_form = {'username': _pyserver.USERNAME, 'password': _pyserver.PASSWORD}

    def logins():
        step('login_admin', lambda: raw('POST', '/login', data=login_form),
             capture={'jwt.admin': 'access_token'})
        for slot, scope in (
            ('reader', 'pipes:read'),
            ('writer', 'pipes:read pipes:write'),
            ('deleter', 'pipes:read pipes:write pipes:delete pipes:drop'),
            ('sql', 'sql:read'),
            ('actions', 'actions:execute connectors:read'),
            ('none', 'users:read'),
        ):
            step(f'login_{slot}', lambda scope=scope: raw(
                'POST', '/login', data={**login_form, 'scope': scope},
            ), capture={f'jwt.{slot}': 'access_token'})

    # Authentication.
    step('post_no_auth', lambda: mcp(None, 'tools/list'))
    step('get_no_auth', lambda: raw('GET', '/mcp'))
    step('post_bad_jwt', lambda: raw(
        'POST', '/mcp', json={'jsonrpc': '2.0', 'id': 0, 'method': 'ping'},
        headers={'Authorization': 'Bearer garbage'},
    ))
    logins()

    # Protocol.
    step('initialize', lambda: mcp('admin', 'initialize', {
        'protocolVersion': '2025-06-18', 'capabilities': {},
        'clientInfo': {'name': 'fixture', 'version': '0'},
    }))
    step('initialize_old_version', lambda: mcp('reader', 'initialize', {'protocolVersion': '2024-11-05'}))
    step('initialize_unknown_version', lambda: mcp('reader', 'initialize', {'protocolVersion': '1999-01-01'}))
    step('initialize_no_params', lambda: mcp('none', 'initialize'))
    step('ping', lambda: mcp('none', 'ping'))
    step('notification', lambda: mcp('admin', 'notifications/initialized', notify=True))
    step('request_without_id', lambda: mcp('admin', 'tools/list', notify=True))
    step('notification_with_id', lambda: mcp('admin', payload={
        'jsonrpc': '2.0', 'id': 99, 'method': 'notifications/cancelled',
    }))
    step('batch', lambda: mcp('reader', payload=[
        {'jsonrpc': '2.0', 'id': next_id(), 'method': 'ping'},
        {'jsonrpc': '2.0', 'method': 'notifications/initialized'},
        {'jsonrpc': '2.0', 'id': next_id(), 'method': 'prompts/list'},
    ]))
    step('batch_only_notifications', lambda: mcp('reader', payload=[
        {'jsonrpc': '2.0', 'method': 'notifications/initialized'},
    ]))
    step('batch_empty', lambda: mcp('reader', payload=[]))
    step('batch_non_object', lambda: mcp('reader', payload=[1, 'x']))
    step('parse_error', lambda: mcp('admin', data='{not json'))
    step('non_object_request', lambda: mcp('admin', payload=5))
    step('params_not_object', lambda: mcp('admin', payload={
        'jsonrpc': '2.0', 'id': next_id(), 'method': 'tools/list', 'params': [1],
    }))
    step('method_not_found', lambda: mcp('admin', 'resources/subscribe', {'uri': 'mrsm://docs'}))

    # tools/list per scope set.
    for slot in ('admin', 'reader', 'writer', 'deleter', 'sql', 'actions', 'none'):
        step(f'tools_list_{slot}', lambda slot=slot: mcp(slot, 'tools/list'))
    step('tools_list_cursor', lambda: mcp('admin', 'tools/list', {
        'cursor': base64.urlsafe_b64encode(b'{"offset":10}').decode(),
    }))
    step('tools_list_bad_cursor', lambda: mcp('admin', 'tools/list', {'cursor': 'nope'}))
    step('tools_list_negative_cursor', lambda: mcp('admin', 'tools/list', {
        'cursor': base64.urlsafe_b64encode(b'{"offset":-1}').decode(),
    }))
    for slot in ('admin', 'reader', 'none'):
        step(f'get_info_{slot}', lambda slot=slot: raw('GET', '/mcp', headers={'Authorization': token(slot)}))

    # Resources.
    for slot in ('admin', 'reader', 'none'):
        step(f'resources_list_{slot}', lambda slot=slot: mcp(slot, 'resources/list'))
        step(f'resource_templates_list_{slot}', lambda slot=slot: mcp(slot, 'resources/templates/list'))
    step('read_docs', lambda: mcp('none', 'resources/read', {'uri': 'mrsm://docs'}))
    step('read_connectors', lambda: mcp('actions', 'resources/read', {'uri': 'mrsm://connectors'}))
    step('read_actions', lambda: mcp('admin', 'resources/read', {'uri': 'mrsm://actions'}))
    step('read_actions_reader', lambda: mcp('reader', 'resources/read', {'uri': 'mrsm://actions'}))
    step('read_no_uri', lambda: mcp('admin', 'resources/read', {}))
    step('read_unknown', lambda: mcp('admin', 'resources/read', {'uri': 'mrsm://nope'}))

    # Prompts.
    for slot in ('admin', 'reader', 'none'):
        step(f'prompts_list_{slot}', lambda slot=slot: mcp(slot, 'prompts/list'))
    step('prompt_bootstrap', lambda: mcp('writer', 'prompts/get', {
        'name': 'bootstrap_pipe', 'arguments': {'source': 'a CSV of weather readings'},
    }))
    step('prompt_bootstrap_instance', lambda: mcp('admin', 'prompts/get', {
        'name': 'bootstrap_pipe',
        'arguments': {'source': 'the orders table', 'instance_keys': 'sql:fixture'},
    }))
    step('prompt_diagnose', lambda: mcp('reader', 'prompts/get', {
        'name': 'diagnose_sync_failure',
        'arguments': {'connector_keys': 'demo', 'metric_key': 'temperature', 'symptom': 'no rows'},
    }))
    step('prompt_explain', lambda: mcp('reader', 'prompts/get', {
        'name': 'explain_pipe',
        'arguments': {'connector_keys': 'demo', 'metric_key': 'temperature', 'location_key': 'garden'},
    }))
    step('prompt_hidden', lambda: mcp('reader', 'prompts/get', {
        'name': 'bootstrap_pipe', 'arguments': {'source': 'x'},
    }))
    step('prompt_unknown', lambda: mcp('admin', 'prompts/get', {'name': 'nope'}))
    step('prompt_unknown_argument', lambda: mcp('admin', 'prompts/get', {
        'name': 'explain_pipe',
        'arguments': {'connector_keys': 'a', 'metric_key': 'b', 'colour': 'red'},
    }))
    step('prompt_missing_argument', lambda: mcp('admin', 'prompts/get', {
        'name': 'explain_pipe', 'arguments': {'connector_keys': 'a'},
    }))

    # Tool calls: errors from the dispatcher.
    step('call_unknown_tool', lambda: call('admin', 'nope', {}))
    step('call_missing_scope', lambda: call('reader', 'register_pipe', {
        'connector_keys': 'demo', 'metric_key': 'temperature',
    }))
    step('call_arguments_not_object', lambda: call('admin', 'list_pipes', [1]))
    step('call_missing_argument', lambda: call('admin', 'get_pipe_attributes', {'connector_keys': 'demo'}))
    step('call_no_name', lambda: mcp('admin', 'tools/call', {}))

    # Tool calls: pipes.
    keys = {'connector_keys': 'demo', 'metric_key': 'temperature', 'location_key': 'garden'}
    step('register_pipe', lambda: call('writer', 'register_pipe', {
        **keys, 'columns': {'datetime': 'ts', 'id': 'station'}, 'tags': ['outdoor'],
        'dtypes': {'val': 'float64'},
    }))
    step('register_pipe_again', lambda: call('writer', 'register_pipe', {
        **keys, 'columns': {'datetime': 'ts', 'id': 'station'},
    }), compare='shape', reason=SQLITE_DATA)
    step('register_pipe_null_location', lambda: call('admin', 'register_pipe', {
        'connector_keys': 'demo', 'metric_key': 'humidity', 'location_key': 'None',
        'parameters': {'columns': {'id': 'room'}, 'upsert': True},
    }))
    step('register_protected', lambda: call('admin', 'register_pipe', {
        'connector_keys': 'demo', 'metric_key': 'sneaky', 'parameters': {'target': 'mrsm_users'},
    }))
    step('list_pipes', lambda: call('reader', 'list_pipes', {}), compare='shape', reason=SQLITE_DATA)
    step('list_pipes_filtered', lambda: call('reader', 'list_pipes', {
        'metric_keys': ['temperature'], 'tags': ['outdoor'],
    }), compare='shape', reason=SQLITE_DATA)
    step('list_pipes_no_match', lambda: call('reader', 'list_pipes', {'connector_keys': ['nope']}))
    step('list_pipes_bad_cursor', lambda: call('reader', 'list_pipes', {'cursor': '!!'}))
    step('list_pipes_other_instance', lambda: call('reader', 'list_pipes', {'instance_keys': 'sql:other'}))
    step('get_pipe_attributes', lambda: call('reader', 'get_pipe_attributes', keys),
         compare='shape', reason=SQLITE_DATA)
    step('get_pipe_attributes_unregistered', lambda: call('reader', 'get_pipe_attributes', {
        'connector_keys': 'demo', 'metric_key': 'unregistered',
    }), compare='shape', reason=SQLITE_DATA)
    step('edit_pipe', lambda: call('writer', 'edit_pipe', {
        **keys, 'parameters': {'tags': ['outdoor', 'garden'], 'verify': {'bound_days': 1}},
    }), compare='shape', reason=SQLITE_DATA)
    step('edit_pipe_protected', lambda: call('writer', 'edit_pipe', {
        **keys, 'parameters': {'target_table': 'mrsm_tokens'},
    }))
    step('stats_before_sync', lambda: call('reader', 'get_pipe_stats', keys),
         compare='shape', reason=SQLITE_DATA)
    docs = [
        {'ts': '2024-01-01 00:00:00', 'station': 'A', 'val': 1.5},
        {'ts': '2024-01-01 01:00:00', 'station': 'A', 'val': 2.5},
        {'ts': '2024-01-02 00:00:00', 'station': 'B', 'val': 3.5},
    ]
    step('sync_documents', lambda: call('writer', 'sync_documents', {**keys, 'documents': docs}),
         compare='shape', reason=SQLITE_DATA)
    step('sync_documents_empty', lambda: call('writer', 'sync_documents', {**keys, 'documents': []}),
         compare='shape', reason=SQLITE_DATA)
    step('stats', lambda: call('reader', 'get_pipe_stats', keys), compare='shape', reason=SQLITE_DATA)
    step('stats_params', lambda: call('reader', 'get_pipe_stats', {
        **keys, 'params': {'station': 'B'}, 'begin': '2024-01-01',
    }), compare='shape', reason=SQLITE_DATA)
    step('get_pipe_data', lambda: call('reader', 'get_pipe_data', keys),
         compare='shape', reason=SQLITE_DATA)
    step('get_pipe_data_page', lambda: call('reader', 'get_pipe_data', {
        **keys, 'limit': 1, 'order': 'desc', 'select_columns': ['ts', 'val'],
    }), compare='shape', reason=SQLITE_DATA)
    step('get_pipe_data_params', lambda: call('reader', 'get_pipe_data', {
        **keys, 'params': {'station': 'A'}, 'end': '2024-01-01 01:00:00',
    }), compare='shape', reason=SQLITE_DATA)
    step('get_pipe_data_backtrack', lambda: call('reader', 'get_pipe_data', {
        **keys, 'backtrack_minutes': 1440,
    }), compare='shape', reason=SQLITE_DATA)
    step('get_pipe_data_bad_cursor', lambda: call('reader', 'get_pipe_data', {**keys, 'cursor': 'x'}))
    step('sync_pipe', lambda: call('writer', 'sync_pipe', keys), compare='shape', reason=SOURCE_SYNC)
    step('verify_pipe', lambda: call('writer', 'verify_pipe', {**keys, 'begin': '2024-01-01'}),
         compare='shape', reason=SOURCE_SYNC)
    step('deduplicate_pipe', lambda: call('deleter', 'deduplicate_pipe', keys),
         compare='shape', reason=SOURCE_SYNC)
    step('deduplicate_pipe_writer', lambda: call('writer', 'deduplicate_pipe', keys))
    step('clear_pipe', lambda: call('deleter', 'clear_pipe', {**keys, 'begin': '2024-01-02'}),
         compare='shape', reason=SQLITE_DATA)
    step('drop_pipe', lambda: call('deleter', 'drop_pipe', keys), compare='shape', reason=SQLITE_DATA)
    step('delete_pipe', lambda: call('deleter', 'delete_pipe', keys), compare='shape', reason=SQLITE_DATA)

    # Tool calls: read_sql.
    for name, query in (
        ('read_sql_delete', 'DELETE FROM t'),
        ('read_sql_stacked', 'SELECT 1; SELECT 2'),
        ('read_sql_into', 'SELECT * INTO t2 FROM t'),
        ('read_sql_protected', 'SELECT * FROM mrsm_users'),
        ('read_sql_executable_comment', 'SELECT /*! 1 */ 2'),
        ('read_sql_unterminated', "SELECT 'abc"),
        ('read_sql_function', 'SELECT pg_read_file(\'x\')'),
        ('read_sql_prefix', 'SELECT xp_cmdshell'),
        ('read_sql_empty', '   '),
    ):
        step(name, lambda query=query: call('sql', 'read_sql', {
            'connector_keys': _pyserver.INSTANCE, 'query': query,
        }))
    step('read_sql_select', lambda: call('sql', 'read_sql', {
        'connector_keys': _pyserver.INSTANCE, 'query': 'SELECT 1 AS one, \'a;b\' AS two',
    }))

    # Tool calls: execute_action.
    step('execute_unknown', lambda: call('actions', 'execute_action', {'action': 'nope'}))
    step('execute_empty', lambda: call('actions', 'execute_action', {'action': '  '}))
    step('execute_denied', lambda: call('actions', 'execute_action', {'action': 'sh echo hi'}))
    step('execute_smuggled', lambda: call('actions', 'execute_action', {
        'action': 'start job', 'kwargs': {'action': ['sh', 'echo']},
    }))
    step('execute_smuggled_words', lambda: call('actions', 'execute_action', {
        'action': 'start jobs python',
    }))
    step('execute_other_instance', lambda: call('admin', 'execute_action', {
        'action': 'show version', 'kwargs': {'mrsm_instance': 'sql:other'},
    }))
    step('execute_show_version', lambda: call('admin', 'execute_action', {'action': 'show version'}),
         compare='shape', reason=ACTION_RUN)

    # Resources that read pipes (after the pipes above were deleted, re-register one).
    step('register_for_resources', lambda: call('writer', 'register_pipe', {
        'connector_keys': 'demo', 'metric_key': 'pressure', 'columns': {'datetime': 'ts'},
    }))
    step('read_pipes', lambda: mcp('reader', 'resources/read', {'uri': 'mrsm://pipes'}),
         compare='shape', reason=SQLITE_DATA)
    step('read_pipe', lambda: mcp('reader', 'resources/read', {'uri': 'mrsm://pipes/demo/pressure/None'}),
         compare='shape', reason=SQLITE_DATA)

    # The read-only server.
    _pyserver.kill_server()
    os.environ['MRSM_PATCH'] = json.dumps({
        'meerschaum': {'instance': _pyserver.INSTANCE},
        'api': {'mcp': {'read_only': True}},
    })
    SERVER['read_only_from'] = len(gc.EXCHANGES)
    port = _pyserver.start_server()
    base = f"http://127.0.0.1:{port}"
    step('ro_tools_list', lambda: mcp('admin', 'tools/list'))
    step('ro_prompts_list', lambda: mcp('admin', 'prompts/list'))
    step('ro_call_write', lambda: call('admin', 'drop_pipe', {
        'connector_keys': 'demo', 'metric_key': 'pressure',
    }))
    step('ro_read_docs', lambda: mcp('admin', 'resources/read', {'uri': 'mrsm://docs'}))
    step('ro_get_info', lambda: raw('GET', '/mcp', headers={'Authorization': token('reader')}))

    from meerschaum.actions import actions as _actions
    actions = sorted(_actions)
    for i, ex in enumerate(gc.EXCHANGES):
        ex['server'] = 'read_only' if i >= SERVER['read_only_from'] else 'default'

    _pyserver.write_fixture(OUT, {
        'username': _pyserver.USERNAME,
        'password': _pyserver.PASSWORD,
        'instance': _pyserver.INSTANCE,
        'connectors': {
            typ: [k for k in labels if k != 'default']
            for typ, labels in mrsm.get_config('meerschaum', 'connectors').items()
        },
        'actions': actions,
        # Requests outside a step (the second server's readiness probe) are not part of the contract.
        'exchanges': [ex for ex in gc.EXCHANGES if ex['step']],
    })
    print(f"Wrote {len(gc.EXCHANGES)} exchanges to {OUT}.")


def main():
    _pyserver.seed_admin()
    port = _pyserver.start_server()
    main_record(port)


if __name__ == '__main__':
    try:
        main()
    finally:
        _pyserver.kill_server()
