"""The web API and stdio MCP may share configuration without sharing a host socket."""

import json
import queue
import subprocess
import threading
import urllib.request

import pytest

from tests.console.conftest import Server, mrsm_binary

pytestmark = pytest.mark.console

PLUGIN = '''import os
from meerschaum.plugins import api_plugin
from meerschaum.mcp import tool

@api_plugin
def routes(app):
    @app.get('/concurrent-host/identity')
    def identity():
        return {'pid': os.getpid()}

@tool('concurrent_host_identity', 'Return this synthetic host process ID.',
      read_only=True, idempotent=True)
def host_identity():
    return {'pid': os.getpid()}
'''


def test_api_and_stdio_mcp_keep_their_own_python_hosts():
    server = Server('api-mcp-concurrency', native=False)
    plugin = server.plugins / 'concurrent_host'
    plugin.mkdir()
    (plugin / '__init__.py').write_text(PLUGIN)
    child = None
    try:
        server.start()

        def api_identity():
            with urllib.request.urlopen(
                server.base_url + '/concurrent-host/identity', timeout=15,
            ) as response:
                return json.load(response)['pid']

        api_pid = api_identity()
        with (server.root / 'mcp.log').open('wb') as log:
            child = subprocess.Popen(
                [str(mrsm_binary()), 'start', 'mcp'], env=server.env, cwd=server.root,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=log,
                text=True, start_new_session=True,
            )
            responses = queue.Queue()

            def read_responses():
                for line in child.stdout:
                    responses.put(line)
                responses.put(None)

            reader = threading.Thread(target=read_responses, daemon=True)
            reader.start()

            def rpc(request_id, method, params):
                child.stdin.write(json.dumps({
                    'jsonrpc': '2.0', 'id': request_id, 'method': method, 'params': params,
                }) + '\n')
                child.stdin.flush()
                line = responses.get(timeout=120)
                assert line is not None, (server.root / 'mcp.log').read_text()
                result = json.loads(line)
                assert result['id'] == request_id, result
                assert 'error' not in result, result
                return result['result']

            rpc(1, 'initialize', {'protocolVersion': '2025-06-18'})
            result = rpc(2, 'tools/call', {'name': 'concurrent_host_identity'})
            assert not result.get('isError'), result
            mcp_pid = json.loads(result['content'][0]['text'])['pid']
            assert mcp_pid != api_pid
            assert api_identity() == api_pid

            child.stdin.close()
            assert child.wait(timeout=30) == 0, (server.root / 'mcp.log').read_text()
            reader.join(timeout=5)
            assert not reader.is_alive()
            assert api_identity() == api_pid
            with urllib.request.urlopen(server.base_url + '/dash/_dash-layout', timeout=15) as response:
                assert response.status == 200
                assert json.load(response)
    finally:
        if child is not None and child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=20)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=5)
        server.stop()
