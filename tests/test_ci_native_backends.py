"""Native backend checks use compiled tests and owned loopback databases."""

import json
import re

import pytest

from scripts.ci import native_backends
from scripts.ci.native_backends import (
    REPO, TESTS, check_test_output, native_environment, select_tests, target_sources,
)


def real_source(test):
    """The root file of the test target in this repository (`tests/<t>.rs` or `tests/<t>/main.rs`)."""
    return next((path for path in target_sources(test) if path.is_file()), None)


def artifacts(repo):
    """One compiler artifact per test target, laid out as the targets are in this repository."""
    rows = {}
    for test in TESTS.values():
        source = real_source(test)
        assert source is not None, f'{test.crate} has no test target {test.target}'
        relative = source.relative_to(REPO)
        rows[(test.crate, test.target)] = json.dumps({
            'reason': 'compiler-artifact',
            'executable': str(repo / 'target' / f'{test.target}-{test.crate}'),
            'target': {'name': test.target, 'kind': ['test'], 'src_path': str(repo / relative)},
        })
    return list(rows.values())


def test_every_native_test_names_a_real_target_module_and_test():
    """The list is checked against the tree: a target, module or test that moved fails here."""
    for name, test in TESTS.items():
        source = real_source(test)
        assert source is not None, f'{name}: {test.crate} has no test target {test.target}'
        if source.name != 'main.rs':
            module_file = source
            assert not test.filter or '::' not in test.filter, name
        else:
            # A suite runs only the module (and test) its filter names, never the whole bundle.
            assert test.filter and '::' in test.filter, f'{name}: a suite target needs a module filter'
            module = test.filter.split('::')[0]
            declared = re.findall(r'^\s*(?:pub\s+)?mod\s+(\w+)\s*;', source.read_text(), re.M)
            assert module in declared, f'{name}: {source} does not declare `mod {module};`'
            module_file = source.parent / f'{module}.rs'
            assert module_file.is_file(), f'{name}: missing {module_file}'
        text = module_file.read_text()
        test_name = (test.filter or '').split('::')[-1]
        if test_name:
            assert re.search(rf'\b{test_name}\b', text), f'{name}: no test `{test_name}` in {module_file}'
        assert re.search(r'#\[(tokio::)?test\b|flavor_tests!', text), f'{name}: {module_file} has no tests'
    for name in native_backends.EXACT_IGNORED:
        test = TESTS[name]
        source = real_source(test)
        module, _, test_name = test.filter.rpartition('::')
        lines = (source if not module else source.parent / f'{module}.rs').read_text().splitlines()
        line = next(i for i, text in enumerate(lines) if re.search(rf'\bfn {test_name}\b', text))
        assert any('#[ignore' in text for text in lines[max(0, line - 4):line]), (
            f'{name}: `--ignored --exact` needs `{test_name}` to be an ignored test')


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


def test_regressions_of_one_suite_share_its_executable(tmp_path):
    selected = select_tests(artifacts(tmp_path), tmp_path)
    valkey = {selected[name] for name, test in TESTS.items() if test.target == 'integration'}
    assert valkey == {tmp_path / 'target' / 'integration-meerschaum-connector-valkey'}
    assert selected['engine_store'] == selected['graph_window_sql_http']


def namesakes(repo, target):
    """The same test target name compiled by other crates of the workspace."""
    crates = sorted(path.parent.parent.name for path in REPO.glob(f'crates/*/tests/{target}/main.rs')
                    if not any(test.target == target and test.crate == path.parent.parent.name
                               for test in TESTS.values()))
    assert crates, f'no other crate has a test target {target}'
    return [json.dumps({'reason': 'compiler-artifact',
        'executable': str(repo / 'target' / 'debug' / 'deps' / f'{target}-{crate}'),
        'target': {'name': target, 'kind': ['test'],
                   'src_path': str(repo / 'crates' / crate / 'tests' / target / 'main.rs')}})
            for crate in crates]


def test_equally_named_targets_of_other_crates_are_ignored(tmp_path):
    rows = artifacts(tmp_path)
    intended = next(row for row in rows if json.loads(row)['executable'].endswith(
        'integration-meerschaum-connector-valkey'))
    others = namesakes(tmp_path, 'integration')
    rest = [row for row in rows if row != intended]
    for order in ([*others, *rows], [*rows, *others], [others[0], *rest, intended, *others[1:]]):
        selected = select_tests(order, tmp_path)
        assert set(selected) == set(TESTS)
        assert selected['valkey_replay'] == tmp_path / 'target' / 'integration-meerschaum-connector-valkey'
    with pytest.raises(ValueError, match='Missing native tests: .*valkey_replay'):
        select_tests([*others, *rest], tmp_path)
    duplicate = json.loads(intended)
    duplicate['executable'] = str(tmp_path / 'target' / 'other' / 'integration')
    with pytest.raises(ValueError, match='Ambiguous native test artifact'):
        select_tests([*others, *rows, json.dumps(duplicate)], tmp_path)


def test_native_connections_drop_ambient_credentials_and_use_distinct_valkey_databases(tmp_path):
    ports = {'postgresql': 43101, 'mysql': 43102, 'mariadb': 43103, 'valkey': 43104}
    env = native_environment(tmp_path, ports, {
        'PATH': '/usr/bin', 'PGHOST': 'production', 'PGPASSWORD': 'secret',
        'MRSM_SQL_SRA': 'secret', 'MRSM_TEST_PG_URI': 'secret', 'VALKEY_URL': 'secret',
        'TMUX': '/user/server', 'TMUX_PANE': '%0', 'CARGO_TARGET_DIR': '/unrelated/target',
        native_backends.SOURCE_PG_URI: 'ambient-connection',
        **dict.fromkeys(native_backends.APPEND_URIS.values(), 'ambient-connection'),
    })
    assert not {'PGHOST', 'PGPASSWORD', 'MRSM_SQL_SRA', 'VALKEY_URL', 'TMUX', 'TMUX_PANE',
                'CARGO_TARGET_DIR'} & env.keys()
    assert '127.0.0.1:43101/testdb' in env['MRSM_TEST_PG_URI']
    assert native_backends.SOURCE_PG_URI not in env
    assert not set(native_backends.APPEND_URIS.values()) & env.keys()
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
