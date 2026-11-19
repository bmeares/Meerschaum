"""Granted fixture fetch hook; imported only inside the selected executor boundary."""
def fetch(pipe, **kw):
    import json
    import os
    import pathlib
    import socket
    import ssl
    import time
    import urllib.error
    import urllib.parse
    import urllib.request

    import meerschaum as mrsm
    from meerschaum.config import get_config

    parameters = pipe.parameters
    expected_api = parameters['probe_kind'] == 'api'
    instance = pipe.instance_connector
    own = str(instance)
    other = parameters['other_instance']
    attributes = get_config('meerschaum', 'connectors', 'api', own.split(':', 1)[1])
    proof = {
        'hostname': socket.gethostname(),
        'expected_profile': (os.environ.get('MRSM_API_EXECUTION') == '1') == expected_api,
        'own_instance': own == parameters['expected_instance'],
        'main_sql_absent': get_config('meerschaum', 'connectors', 'sql', 'main', warn=False) is None,
        'other_connector_absent': get_config('meerschaum', 'connectors', 'api', other.split(':', 1)[1], warn=False) is None,
        'main_environment_absent': 'AWS_SECRET_ACCESS_KEY' not in os.environ,
        'container_socket_absent': not pathlib.Path('/var/run/docker.sock').exists(),
        'callback_read': pipe.get_rowcount() in (1, 2),
        'sealed_alias': (set(attributes) == {'mrsm_api_alias'}) if expected_api else 'mrsm_api_alias' not in attributes,
    }
    foreign = instance.get('/pipes', params={'instance_keys': other}, timeout=8)
    proof['foreign_instance_denied'] = foreign.status_code in (403, 404)
    proof['user_namespace_denied'] = instance.get('/users', timeout=8).status_code == 403
    try:
        pathlib.Path('/meerschaum/main-private.txt').read_text()
    except OSError:
        proof['main_file_absent'] = True
    else:
        proof['main_file_absent'] = False
    try:
        urllib.request.urlopen('http://api:8989/pipes?instance_keys=sql:main', timeout=8)
    except urllib.error.HTTPError as error:
        proof['unsigned_callback_denied'] = error.code == 401
    else:
        proof['unsigned_callback_denied'] = False

    if not expected_api:
        import venv
        from meerschaum.config import paths

        root = pathlib.Path('/meerschaum')
        assert pathlib.Path.cwd() == root and pathlib.Path(os.environ['MRSM_ROOT_DIR']) == root
        assert paths.VIRTENV_RESOURCES_PATH == root / 'venvs'
        data = root / 'data' / 'container-fixture'
        data.mkdir(parents=True, exist_ok=True)
        environment = paths.VIRTENV_RESOURCES_PATH / 'container-fixture'
        if not environment.exists():
            venv.EnvBuilder(with_pip=False, symlinks=True).create(environment)
        assert (environment / 'pyvenv.cfg').is_file()
        for directory in [data, environment]:
            origin = directory / 'origin'
            if not origin.exists():
                origin.write_text(proof['hostname'])
        assert (data / 'origin').read_text() == (environment / 'origin').read_text()
        (data / 'latest').write_text(proof['hostname'])
        proof['team_durable_cwd_data_venv'] = True

    if not expected_api and parameters.get('network_probe', False):
        # The DNS alias is the generated relay, so TLS still verifies the original name.
        with socket.create_connection((parameters['tls_host'], parameters['tls_port']), timeout=12) as connection:
            with ssl.create_default_context(cafile=parameters['tls_ca']).wrap_socket(connection, server_hostname=parameters['tls_host']) as tls:
                proof['relay_peer'] = tls.getpeername()[0]
                tls.sendall(('HEAD / HTTP/1.1\r\nHost: ' + parameters['tls_host'] + '\r\nConnection: close\r\n\r\n').encode())
                proof['relay_tls_verified'] = tls.recv(256).startswith(b'HTTP/')
        try:
            with socket.create_connection((parameters['tls_host'], parameters['tls_port']), timeout=12) as connection:
                ssl.create_default_context(cafile=parameters['tls_ca']).wrap_socket(connection, server_hostname='incorrect-certificate-name.invalid')
        except ssl.SSLCertVerificationError:
            proof['relay_wrong_name_denied'] = True
        else:
            proof['relay_wrong_name_denied'] = False
        if parameters.get('public_tls_probe', False):
            with urllib.request.urlopen('https://www.python.org/', timeout=20) as response:
                proof['optional_public_proxy_tls_verified'] = 200 <= response.status < 400
                response.read(256)
        try:
            socket.getaddrinfo(parameters['other_executor'], 8989, type=socket.SOCK_STREAM)
        except socket.gaierror:
            proof['foreign_team_dns_denied'] = True
        else:
            proof['foreign_team_dns_denied'] = False
        proxy = urllib.parse.urlsplit(os.environ['HTTPS_PROXY'])
        for destination in ['169.254.169.254', '168.63.129.16', '[::1]', parameters['tls_host']]:
            with socket.create_connection((proxy.hostname, proxy.port), timeout=8) as connection:
                connection.sendall(('GET http://' + destination + '/ HTTP/1.1\r\nHost: ' + destination + '\r\n\r\n').encode())
                reply = connection.recv(4096)
                assert reply.startswith(b'HTTP/') and reply.split(b' ')[1] == b'403', destination
        proof['metadata_private_ipv6_proxy_denied'] = True
        try:
            with socket.create_connection(('1.1.1.1', 443), timeout=2):
                pass
        except OSError:
            proof['direct_outbound_denied'] = True
        else:
            proof['direct_outbound_denied'] = False
    assert all(value for key, value in proof.items() if key not in ('hostname', 'relay_peer')), proof
    print('MRSM_CONTAINER_PROOF:' + json.dumps(proof, sort_keys=True), flush=True)
    if parameters.get('hold_seconds'):
        time.sleep(parameters['hold_seconds'])
    # Nonempty output exercises the actual executor -> Main native data mutation path.
    # Main independently checks the resulting row and exact binary payload after success.
    yield [{'id': 2, 'payload': b'container-\x00\xff-native-write'}]
