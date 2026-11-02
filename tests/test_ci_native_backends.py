"""Native backend checks use compiled tests and owned loopback databases."""

import json

import pytest

from scripts.ci import native_backends
from scripts.ci.native_backends import TESTS, check_test_output, native_environment, select_tests


def artifacts(repo):
    return [json.dumps({'reason': 'compiler-artifact', 'executable': str(repo / 'target' / name),
        'target': {'name': name, 'kind': ['test'],
                   'src_path': str(repo / 'crates' / crate / 'tests' / (name + '.rs'))}})
            for name, crate in TESTS.items()]


def test_missing_or_foreign_native_executables_refuse_the_gate(tmp_path):
    rows = artifacts(tmp_path)
    assert set(select_tests(rows, tmp_path)) == set(TESTS)
    with pytest.raises(ValueError, match='Missing native tests'):
        select_tests(rows[:-1], tmp_path)
    foreign = json.loads(rows[0])
    foreign['executable'] = '/usr/bin/true'
    with pytest.raises(ValueError, match='Unexpected native test artifact'):
        select_tests([json.dumps(foreign), *rows[1:]], tmp_path)
    check_test_output('valkey_misc', 'test result: ok. 2 passed; 0 failed; 0 ignored;')
    with pytest.raises(ValueError, match='without running'):
        check_test_output('valkey_misc', 'SKIPPED leases_live\ntest result: ok. 2 passed;')
    for name in ('pipeline_metadata_move', 'rc15_fail_closed', 'w18_fail_fast'):
        with pytest.raises(ValueError, match='without running'):
            check_test_output(name, 'SKIPPED: no owned fixture\ntest result: ok. 2 passed;')
    with pytest.raises(ValueError, match='No passing native tests'):
        check_test_output('instance_users', 'test result: ok. 0 passed;')
    check_test_output('instance_users', 'Skipping oracle: not configured.\ntest result: ok. 2 passed;')
    for flavor in ('postgresql', 'mysql', 'mariadb'):
        with pytest.raises(ValueError, match='live SQL check returned without running'):
            check_test_output('instance_users', f'Skipping {flavor}: set URI.\ntest result: ok. 2 passed;')


def namesakes(repo, name):
    """The same test target name compiled by other crates of the workspace."""
    crates = {
        'w18_fail_fast': ('meerschaum-connector-api', 'meerschaum-connector-sql'),
        'pipeline_metadata_move': ('meerschaum-connector-sql',),
    }[name]
    return [json.dumps({'reason': 'compiler-artifact',
        'executable': str(repo / 'target' / 'debug' / 'deps' / f'{name}-{crate}'),
        'target': {'name': name, 'kind': ['test'],
                   'src_path': str(repo / 'crates' / crate / 'tests' / (name + '.rs'))}})
            for crate in crates]


@pytest.mark.parametrize('name', ['w18_fail_fast', 'pipeline_metadata_move'])
def test_equally_named_targets_of_other_crates_are_ignored(tmp_path, name):
    rows = artifacts(tmp_path)
    intended = next(row for row in rows if json.loads(row)['target']['name'] == name)
    others = namesakes(tmp_path, name)
    rest = [row for row in rows if row != intended]
    for order in ([*others, *rows], [*rows, *others], [others[0], *rest, intended, *others[1:]]):
        selected = select_tests(order, tmp_path)
        assert set(selected) == set(TESTS)
        assert selected[name] == tmp_path / 'target' / name
    with pytest.raises(ValueError, match=f"Missing native tests: \\['{name}'\\]"):
        select_tests([*others, *rest], tmp_path)
    duplicate = json.loads(intended)
    duplicate['executable'] = str(tmp_path / 'target' / 'other' / name)
    with pytest.raises(ValueError, match='Ambiguous native test artifact'):
        select_tests([*others, *rows, json.dumps(duplicate)], tmp_path)


def test_native_connections_drop_ambient_credentials_and_use_distinct_valkey_databases(tmp_path):
    ports = {'postgresql': 43101, 'mysql': 43102, 'mariadb': 43103, 'valkey': 43104}
    env = native_environment(tmp_path, ports, {
        'PATH': '/usr/bin', 'PGHOST': 'production', 'PGPASSWORD': 'secret',
        'MRSM_SQL_SRA': 'secret', 'MRSM_TEST_PG_URI': 'secret', 'VALKEY_URL': 'secret',
        'TMUX': '/user/server', 'TMUX_PANE': '%0', 'CARGO_TARGET_DIR': '/unrelated/target',
    })
    assert not {'PGHOST', 'PGPASSWORD', 'MRSM_SQL_SRA', 'VALKEY_URL', 'TMUX', 'TMUX_PANE',
                'CARGO_TARGET_DIR'} & env.keys()
    assert '127.0.0.1:43101/testdb' in env['MRSM_TEST_PG_URI']
    assert env['MRSM_VALKEY_TEST_URL'] == 'redis://127.0.0.1:43104/12'
    assert env['MRSM_VALKEY_FIRST_USER_TEST_URL'] == 'redis://127.0.0.1:43104/13'
    with pytest.raises(ValueError, match='Refusing test service port'):
        native_environment(tmp_path, {**ports, 'postgresql': 5432}, {})


def test_cleanup_refuses_unowned_projects_and_only_addresses_its_receipt(tmp_path, monkeypatch):
    calls = []
    monkeypatch.setattr(native_backends, 'run_child', lambda command, *_args: calls.append(command))
    receipt = tmp_path / 'native-backends.json'
    report = {'root': str(tmp_path), 'nonce': 'a' * 16, 'project': 'production',
              'owned_project': True, 'cleaned_up': False}
    native_backends.save_report(receipt, report)
    with pytest.raises(ValueError, match='unowned native backend project'):
        native_backends.cleanup(tmp_path)
    assert calls == []
    report['project'] = 'mrsm-native-' + report['nonce']
    native_backends.save_report(receipt, report)
    native_backends.cleanup(tmp_path)
    assert len(calls) == 1 and report['project'] in calls[0]
    assert calls[0][-3:] == ['down', '--volumes', '--remove-orphans']
    assert json.loads(receipt.read_text())['cleaned_up']
    native_backends.cleanup(tmp_path)
    assert len(calls) == 1
