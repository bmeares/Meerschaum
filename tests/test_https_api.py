#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
`mrsm start api --keyfile KEY --certfile CERT`: HTTPS served by the API server itself.

One loopback server (the standalone binary, see `tests/_binary.py`) on a temporary root, with a
certificate for `localhost` signed by a throwaway CA (both made with the `openssl` CLI), the web
console (its Python host) and a probe plugin:

- the console loads over `https://`, and the Python host calls the server back on its
  loopback-only plain-HTTP listener (never the HTTPS port);
- plain HTTP on the HTTPS port answers `/healthcheck` (container healthchecks) and redirects the
  rest to `https://`;
- `api:` connectors with `protocol: https` log in and move data (`verify: False` for the
  throwaway CA), while a verifying connector refuses the certificate.
"""

from __future__ import annotations

import http.client
import json
import shutil
import ssl
import subprocess
import time
import urllib.error
import urllib.request
import uuid

import pytest

from tests._binary import binary_or_skip, skip_or_fail

PROBE_PLUGIN = '''
from meerschaum.plugins import api_plugin

@api_plugin
def r2_tls_probe_routes(app):
    @app.get('/r2-tls-probe')
    def r2_tls_probe():
        import json, urllib.request
        from meerschaum.api.host import get_host_config
        url = get_host_config().get('server_url')
        with urllib.request.urlopen(url + '/version', timeout=10) as response:
            version = json.loads(response.read().decode('utf-8'))
        return {'server_url': url, 'version': version}
'''


def _openssl(*args: str, cwd) -> None:
    result = subprocess.run(['openssl', *args], cwd=str(cwd), capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def make_certificates(directory) -> None:
    """A throwaway CA (`ca.pem`) and a key and certificate for localhost it signs."""
    if shutil.which('openssl') is None:
        skip_or_fail("The HTTPS tests make their certificates with the `openssl` command.")
    curve = ['-newkey', 'ec', '-pkeyopt', 'ec_paramgen_curve:prime256v1', '-nodes']
    _openssl(
        'req', '-x509', *curve, '-days', '2', '-subj', '/CN=Meerschaum test CA',
        '-addext', 'basicConstraints=critical,CA:TRUE',
        '-addext', 'keyUsage=critical,keyCertSign,cRLSign',
        '-keyout', 'ca.key', '-out', 'ca.pem', cwd=directory,
    )
    _openssl('req', *curve, '-subj', '/CN=localhost', '-keyout', 'key.pem', '-out', 'leaf.csr', cwd=directory)
    (directory / 'leaf.ext').write_text(
        'basicConstraints=critical,CA:FALSE\n'
        'keyUsage=critical,digitalSignature\n'
        'extendedKeyUsage=serverAuth\n'
        'subjectAltName=DNS:localhost,IP:127.0.0.1\n'
        'subjectKeyIdentifier=hash\n'
        'authorityKeyIdentifier=keyid,issuer\n'
    )
    _openssl(
        'x509', '-req', '-in', 'leaf.csr', '-CA', 'ca.pem', '-CAkey', 'ca.key', '-CAcreateserial',
        '-days', '2', '-extfile', 'leaf.ext', '-out', 'cert.pem', cwd=directory,
    )


@pytest.fixture(scope='module')
def https_server():
    binary_or_skip()
    from tests.console.conftest import Server, mrsm_binary

    class HttpsServer(Server):
        """`Server` with `--keyfile` / `--certfile`: readiness is asked over `https://`."""

        def __init__(self):
            super().__init__('r2-https', native=False)
            self.tls = self.root / 'tls'
            self.tls.mkdir()
            make_certificates(self.tls)
            self.args += [
                '--keyfile', str(self.tls / 'key.pem'),
                '--certfile', str(self.tls / 'cert.pem'),
            ]
            self.base_url = f"https://localhost:{self.port}"
            self.context = ssl.create_default_context(cafile=str(self.tls / 'ca.pem'))

        def get(self, path: str, timeout: float = 10.0) -> tuple[int, str]:
            try:
                with urllib.request.urlopen(
                    f"{self.base_url}{path}", timeout=timeout, context=self.context,
                ) as response:
                    return response.status, response.read().decode('utf-8', 'replace')
            except urllib.error.HTTPError as e:
                return e.code, e.read().decode('utf-8', 'replace')
            except Exception:
                return 0, ''

        def start(self) -> None:
            self.prepare()
            (self.plugins / 'r2_tls_probe.py').write_text(PROBE_PLUGIN)
            with open(self.log_path, 'wb') as log:
                self.proc = subprocess.Popen(
                    [
                        str(mrsm_binary()), 'start', 'api',
                        '--host', '127.0.0.1', '--port', str(self.port), '--no-webterm',
                        *self.args,
                    ],
                    env=self.env, cwd=str(self.root), stdout=log, stderr=subprocess.STDOUT,
                    start_new_session=True,
                )
            deadline = time.time() + 180
            while time.time() < deadline:
                if self.proc.poll() is not None:
                    raise RuntimeError(f"The server exited early:\n{self.log()}")
                if self.get('/version')[0] in (200, 401):
                    break
                time.sleep(0.5)
            else:
                raise RuntimeError(f"The server did not start in time:\n{self.log()}")
            ### The console's Python host starts with the first `/dash` request.
            while time.time() < deadline:
                if self.proc.poll() is not None:
                    raise RuntimeError(f"The server exited early:\n{self.log()}")
                status, _ = self.get('/dash/_dash-layout', timeout=60)
                if status and status < 500:
                    return
                time.sleep(1)
            raise RuntimeError(f"The console did not start in time:\n{self.log()}")

    instance = HttpsServer()
    try:
        instance.start()
    except Exception:
        instance.stop()
        raise
    yield instance
    instance.stop()


def test_the_console_loads_over_https_and_its_host_calls_back_on_loopback(https_server):
    assert f"Serving HTTPS on https://localhost:{https_server.port}" in https_server.log()
    status, layout = https_server.get('/dash/_dash-layout')
    assert status == 200, layout
    assert json.loads(layout).get('props'), layout

    ### A plugin route of the Python host, which asks the server for its version through the
    ### `server_url` the server gave it: the internal listener, plain HTTP on loopback.
    status, body = https_server.get('/r2-tls-probe')
    assert status == 200, body + https_server.log()
    probe = json.loads(body)
    assert probe['server_url'].startswith('http://127.0.0.1:'), probe
    assert not probe['server_url'].endswith(f":{https_server.port}"), probe
    assert probe['version']
    with urllib.request.urlopen(probe['server_url'] + '/version', timeout=10) as response:
        assert json.loads(response.read().decode('utf-8')) == probe['version']


def test_plain_http_on_the_https_port_keeps_the_healthcheck(https_server):
    host = f"localhost:{https_server.port}"
    connection = http.client.HTTPConnection('127.0.0.1', https_server.port, timeout=10)
    connection.request('GET', '/healthcheck', headers={'Host': host})
    response = connection.getresponse()
    assert response.status == 200
    assert json.loads(response.read().decode('utf-8'))['success'] is True
    connection.request('GET', '/dash/pipes?tab=data', headers={'Host': host})
    response = connection.getresponse()
    response.read()
    assert response.status == 308
    assert response.getheader('location') == f"https://{host}/dash/pipes?tab=data"
    connection.close()


def test_api_connectors_speak_https(https_server):
    import meerschaum as mrsm
    from tests.console.conftest import USERNAME, PASSWORD

    label = 'r2https' + uuid.uuid4().hex[:6]
    conn = mrsm.get_connector(
        'api', label, host='localhost', port=https_server.port, protocol='https',
        username=USERNAME, password=PASSWORD, verify=False,
    )
    success, msg = conn.login()
    assert success, msg
    assert conn.URI.startswith('https://')

    pipe = mrsm.Pipe(
        'r2', 'https_' + uuid.uuid4().hex[:8], instance=conn,
        columns={'datetime': 'dt', 'id': 'id'},
        dtypes={'dt': 'datetime', 'id': 'int', 'val': 'float'},
    )
    try:
        success, msg = pipe.sync([{'dt': '2026-01-01 00:00:00', 'id': 1, 'val': 1.5}])
        assert success, msg
        df = pipe.get_data()
        assert len(df) == 1
        assert float(df['val'][0]) == 1.5
    finally:
        pipe.delete()

    ### Verifying the certificate (the default): the throwaway CA is not trusted.
    verifying = mrsm.get_connector(
        'api', label + 'v', host='localhost', port=https_server.port, protocol='https',
        username=USERNAME, password=PASSWORD,
    )
    success, msg = verifying.login(warn=False)
    assert not success, msg
