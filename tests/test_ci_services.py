"""CI isolation and collection coverage, without touching a developer's test databases."""

import argparse
import json
import os
import shutil
import subprocess
import sys
from types import SimpleNamespace

import pytest

from scripts.ci import services
from scripts.ci.pytest_shard import assignment
from scripts.ci.shards import SHARDS
from tests._ports import LEGACY_PORTS, service_port


@pytest.mark.parametrize('parameters, expected', [
    ({}, 'core'),
    ({'flavor': 'timescaledb-ha'}, 'postgres'),
    ({'pair': 'mariadb'}, 'mysql'),
    ({'instance': 'sqlite', 'other_instance': 'postgis'}, 'postgres'),
    ({'instance': 'valkey', 'other_instance': 'custom'}, 'mssql-valkey'),
    ({'chain': 'timescaledb_ha'}, 'postgres'),
    ({'flavor': 'oracle'}, 'oracle'),
])
def test_parameterized_instances_and_file_sources_belong_to_one_shard(parameters, expected):
    assert assignment('tests/example.py::test_pipe', parameters) == expected


def test_unknown_and_cross_family_backends_fail_closed():
    with pytest.raises(ValueError, match='unknown backend'):
        assignment('tests/example.py::test_pipe', {'flavor': 'new_backend'})
    with pytest.raises(ValueError, match='multiple backend shards'):
        assignment('tests/example.py::test_pipe', {'instance': 'mysql', 'other_instance': 'oracle'})


def test_live_unparameterized_tests_are_assigned_to_their_service():
    assert assignment('tests/test_utils_cache.py::test_valkey_backend_is_shared', {}) == 'mssql-valkey'
    assert assignment('tests/test_native_sync.py::test_postgres_numeric_special_values_read_back_like_v4', {}) == 'postgres'
    assert assignment('tests/example.py::test_pipe', {}, ('postgis_instance',)) == 'postgres'


def test_partition_validation_catches_missing_extra_and_repeated_tests():
    baseline = [{'nodeid': 'a', 'shard': 'core'}, {'nodeid': 'b', 'shard': 'mysql'}]
    lanes = {'core': baseline, 'mysql': baseline}
    assert services.validate_partition(baseline, lanes) == {'core': 1, 'mysql': 1}
    for bad in (
        {'core': baseline},
        {'core': [*baseline, baseline[0]], 'mysql': baseline},
        {'core': [*baseline, {'nodeid': 'c', 'shard': 'core'}], 'mysql': baseline},
    ):
        with pytest.raises(ValueError, match='partition mismatch'):
            services.validate_partition(baseline, bad)


def test_service_environment_drops_ambient_connectors_roots_and_tmux(tmp_path):
    state = services.claim_root(tmp_path / 'owned', 'postgres')
    env = services.environment(state, {'api': 43101, 'postgresql': 43102}, {
        'MRSM_ROOT_DIR': '/do/not/open', 'MRSM_SQL_PRODUCTION': 'secret',
        'MRSM_TEST_BIN': '/artifacts/mrsm', 'MRSM_TEST_ARTIFACT_MANIFEST': '/artifacts/manifest.json',
        'MRSM_TEST_PORTS': '{"postgis":5449}', 'TMUX': '/user/server', 'TMUX_PANE': '%0',
        'PATH': '/usr/bin', 'PYTHON_BIN': '/venv/bin/python',
    })
    assert env['MRSM_ROOT_DIR'] == str(tmp_path / 'owned')
    assert not {'MRSM_SQL_PRODUCTION', 'TMUX', 'TMUX_PANE'} & env.keys()
    assert env['MRSM_TEST_BIN'] == '/artifacts/mrsm'
    assert env['PYTHON_BIN'] == '/venv/bin/python'
    assert ':43102/' in env['MRSM_SQL_TEST_POSTGRESQL']
    assert env['MRSM_TEST_POSTGIS_PUBLISH'] == '127.0.0.1::5432'
    assert sum(spec['workers'] for spec in SHARDS.values()) == 16


def test_isolated_ports_never_use_the_legacy_stack(monkeypatch):
    monkeypatch.setenv('MRSM_TEST_ISOLATED', '1')
    monkeypatch.setenv('MRSM_TEST_FLAVORS', 'postgis')
    monkeypatch.setenv('MRSM_TEST_PORTS', '{"postgis":43101}')
    assert service_port('postgis') == 43101
    assert service_port('mysql') == 1
    monkeypatch.setenv('MRSM_TEST_PORTS', '{}')
    with pytest.raises(ValueError, match='No owned port'):
        service_port('postgis')
    monkeypatch.setenv('MRSM_TEST_PORTS', '{"postgis":5432}')
    with pytest.raises(ValueError, match='Invalid isolated'):
        service_port('postgis')
    monkeypatch.delenv('MRSM_TEST_ISOLATED')
    assert service_port('postgis') == LEGACY_PORTS['postgis']


@pytest.mark.parametrize('binding', ['0.0.0.0:43101', '[::]:43101', '127.0.0.1:5432', '127.0.0.1:8000'])
def test_docker_bindings_must_be_high_loopback_ports(binding):
    with pytest.raises(ValueError):
        services.mapped_port(binding)
    assert services.mapped_port('127.0.0.1:43101\n') == 43101


def test_root_claim_and_cleanup_reject_unowned_resources(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(services, 'checked', lambda *args, **kw: calls.append(args[0]))
    services.cleanup(tmp_path / 'never-initialized')
    assert calls == []
    root = tmp_path / 'owned'
    state = services.claim_root(root, 'mysql')
    with pytest.raises(ValueError, match='new or empty'):
        services.claim_root(root, 'mysql')
    services.cleanup(root)
    assert calls == []  # Never remove resources before successful project collision checks.
    state.update(active=True, owned_project=True)
    (root / services.OWNER_FILE).write_text(json.dumps(state))
    services.cleanup(root)
    assert calls == [services.compose_command(state['project'], 'down', '--volumes', '--remove-orphans')]
    services.cleanup(root)
    assert len(calls) == 2  # Repeated qualified down is safe and catches late startup resources.
    state['project'] = 'tests'
    (root / services.OWNER_FILE).write_text(json.dumps(state))
    with pytest.raises(ValueError, match='ownership'):
        services.cleanup(root)
    assert len(calls) == 2


def test_startup_collision_never_cleans_up_the_existing_project(tmp_path, monkeypatch):
    calls = []

    def command(cmd, _env, **kwargs):
        calls.append(cmd)
        return SimpleNamespace(stdout='existing-container\n' if 'ps' in cmd else '')

    monkeypatch.setattr(services, 'checked', command)
    with pytest.raises(ValueError, match='existing Compose project'):
        services.run(argparse.Namespace(root=tmp_path / 'owned', shard='mysql', workers=1, pytest_args=[]))
    assert not any('down' in cmd or 'up' in cmd for cmd in calls)


def test_partial_startup_failure_cleans_only_the_new_project(tmp_path, monkeypatch):
    calls = []

    def command(cmd, _env, **kwargs):
        calls.append(cmd)
        if 'up' in cmd:
            raise subprocess.CalledProcessError(1, cmd)
        return SimpleNamespace(stdout='')

    monkeypatch.setattr(services, 'checked', command)
    root = tmp_path / 'owned'
    with pytest.raises(subprocess.CalledProcessError):
        services.run(argparse.Namespace(root=root, shard='mysql', workers=1, pytest_args=[]))
    state = services.owned_state(root)
    assert calls[-1] == services.compose_command(state['project'], 'down', '--volumes', '--remove-orphans')
    assert state['active'] is False


def test_cleanup_stops_only_the_api_job_in_the_owned_root(tmp_path, monkeypatch):
    root = tmp_path / 'owned'
    state = services.claim_root(root, 'postgres')
    state.update(active=True, owned_project=True, python='/test-venv/bin/python',
                 ports={'api': 43101, 'postgresql': 43102})
    (root / services.OWNER_FILE).write_text(json.dumps(state))
    (root / 'jobs' / 'test_api').mkdir(parents=True)
    calls = []
    monkeypatch.setattr(services, 'checked', lambda command, env, **kw: calls.append((command, env)))
    services.cleanup(root)
    assert calls[0][0] == ['/test-venv/bin/python', '-m', 'meerschaum', 'delete', 'jobs', 'test_api', '-f', '-y']
    assert calls[1][0][-2:] == ['stop', 'daemon']
    assert all(env.get('MRSM_ROOT_DIR') == str(root) for _, env in calls[:2])
    assert calls[-1][0] == services.compose_command(state['project'], 'down', '--volumes', '--remove-orphans')


def test_mrsm_wrapper_preserves_the_requested_root_and_drops_tmux(tmp_path):
    python = tmp_path / 'fake-python'
    python.write_text('#!/bin/sh\nexec /usr/bin/python3 -c '\
                      "'import os,json; print(json.dumps({k:os.environ[k] for k in "
                      '("MRSM_ROOT_DIR","MRSM_PLUGINS_DIR","TMUX","TMUX_PANE") '
                      "if k in os.environ}))'\n")
    python.chmod(0o755)
    root = tmp_path / 'root'
    plugins = tmp_path / 'plugins'
    env = services.clean_environment(os.environ)
    env.update(PYTHON_BIN=str(python), MRSM_ROOT_DIR=str(root), MRSM_PLUGINS_DIR=str(plugins),
               TMUX='/user/socket', TMUX_PANE='%0')
    result = subprocess.run([str(services.REPO / 'scripts' / 'mrsm.sh'), '--version'],
                            env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stderr
    actual = json.loads(result.stdout)
    assert actual['MRSM_ROOT_DIR'] == str(root)
    assert actual['MRSM_PLUGINS_DIR'] == str(plugins)
    assert 'TMUX' not in actual and 'TMUX_PANE' not in actual


def test_test_wrapper_refuses_an_unowned_project_before_removing_data(tmp_path):
    root = tmp_path / 'owned'
    state = services.claim_root(root, 'core')
    (root / 'data').mkdir()
    existing = root / 'data' / 'preserve.txt'
    existing.write_text('untouched')
    env = services.environment(state, {'api': 43101})
    env.update(PYTHON_BIN=sys.executable, MRSM_TEST_COMPOSE_PROJECT='unowned-project')
    result = subprocess.run([str(services.REPO / 'scripts' / 'test.sh')],
                            env=env, capture_output=True, text=True, timeout=30)
    assert result.returncode == 2, result.stdout + result.stderr
    assert 'ownership does not match' in result.stderr
    assert existing.read_text() == 'untouched'


@pytest.mark.skipif(not shutil.which('docker'), reason='Docker Compose is not installed')
def test_compose_keeps_legacy_defaults_and_isolates_explicit_port_maps(tmp_path):
    state = services.claim_root(tmp_path / 'owned', 'postgres')
    isolated = services.environment(state, {'api': 43101})
    for env, dynamic in ((services.clean_environment(os.environ), False), (isolated, True)):
        result = subprocess.run(services.compose_command(state['project'], 'config', '--format', 'json'),
                                env=env, capture_output=True, text=True, timeout=30)
        assert result.returncode == 0, result.stderr
        config = json.loads(result.stdout)
        for name, spec in config['services'].items():
            binding = spec['ports'][0]
            if dynamic:
                assert binding['host_ip'] == '127.0.0.1'
                assert binding.get('published', '0') == '0'
            else:
                assert int(binding['published']) == LEGACY_PORTS[name]
        assert all(volume['name'].startswith(state['project'] + '_')
                   for volume in config['volumes'].values())
