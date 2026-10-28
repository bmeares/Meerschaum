"""A cold console consumer must use the exact CLI producer's verified native fixture."""

import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest
import yaml

from scripts.ci import console_fixture as fixture
from scripts.ci.services import clean_environment
from tests.test_ci_artifacts import producer, binary_manifest, git  # noqa: F401
from tests.test_workflows_packaging import _evaluate_actions

REPO = Path(__file__).resolve().parents[1]


def fixture_manifest(producer, profile='release'):
    repo, compiler, binary_dir = producer
    program, cli_manifest = binary_manifest(producer, profile)
    output = binary_dir.parent / 'console-fixture'
    plugin = output / fixture.PLUGIN
    plugin.mkdir(parents=True)
    (plugin / 'plugin.toml').write_text('name="w7_native_fig"\nversion="1.0.0"\n')
    (plugin / 'libw7_native_fig.so').write_bytes(b'\x7fELFsynthetic-console-fixture')
    (output / 'build-info.json').write_text(json.dumps({
        'plugin': fixture.PLUGIN, 'profile': profile,
        'cli_sha256': fixture.artifacts.sha256(program), 'sdk_sha256': 'a' * 64,
    }))
    fixture.record(output, profile, compiler, repo)
    return program, cli_manifest, output / 'provenance.json'


def test_cold_consumer_verifies_and_copies_without_any_sdk_or_rustc(producer, tmp_path):
    repo = producer[0]
    for relative in ('scripts/ci/console_fixture.py', 'scripts/ci/artifacts.py',
                     'tests/console/build_native.sh'):
        target = repo / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(REPO / relative, target)
    git(repo, 'add', '.')
    git(repo, 'commit', '-qm', 'Add consumer scripts')
    _, cli_manifest, manifest = fixture_manifest(producer, 'debug')
    env = {key: value for key, value in os.environ.items()
           if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE')}
    home = tmp_path / 'empty-home'
    home.mkdir()
    env.update(CI='true', HOME=str(home), PATH='/usr/bin:/bin', PYTHON_BIN=sys.executable,
               PYTHONPATH=str(repo), MRSM_TEST_CONSOLE_NATIVE_FIXTURE=str(manifest.parent),
               MRSM_TEST_CONSOLE_NATIVE_MANIFEST=str(manifest),
               MRSM_TEST_CONSOLE_NATIVE_PROFILE='debug',
               MRSM_TEST_ARTIFACT_MANIFEST=str(cli_manifest))
    plugins = tmp_path / 'plugins'
    result = subprocess.run(['bash', str(repo / 'tests/console/build_native.sh'), str(plugins)],
                            env=env, cwd=tmp_path, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert not (repo / 'target').exists()
    assert (plugins / fixture.PLUGIN / 'libw7_native_fig.so').read_bytes().startswith(b'\x7fELF')


@pytest.mark.parametrize('problem', ['missing', 'changed-bytes', 'extra-payload', 'wrong-profile',
                                   'different-cli', 'other-commit'])
def test_missing_stale_or_substituted_fixtures_are_rejected(producer, tmp_path, problem):
    program, cli_manifest, manifest = fixture_manifest(producer)
    profile = 'release'
    if problem == 'missing':
        manifest.unlink()
    elif problem == 'changed-bytes':
        (manifest.parent / fixture.PLUGIN / 'libw7_native_fig.so').write_bytes(b'different')
    elif problem == 'extra-payload':
        (manifest.parent / 'unverified.so').write_bytes(b'extra')
    elif problem == 'wrong-profile':
        profile = 'debug'
    elif problem == 'different-cli':
        program.write_bytes(b'\x7fELFother-cli-at-the-same-commit')
        repo, compiler, binary_dir = producer
        fixture.artifacts.create_manifest(binary_dir, cli_manifest, kind='binary', profile='release',
                                           target=fixture.TARGET, toolchain_file=compiler, repo=repo)
    else:
        git(producer[0], 'commit', '--allow-empty', '-qm', 'Another tested revision')
    with pytest.raises(fixture.artifacts.ArtifactError):
        fixture.install(manifest, cli_manifest, profile, tmp_path / 'plugins', producer[0])


def test_ci_cannot_fall_back_to_an_untracked_local_sdk(tmp_path):
    env = {key: value for key, value in os.environ.items()
           if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE')}
    env['CI'] = 'true'
    result = subprocess.run(['bash', str(REPO / 'tests/console/build_native.sh'), str(tmp_path)],
                            env=env, capture_output=True, text=True)
    assert result.returncode != 0
    assert 'local SDK fallback is disabled' in result.stderr
    assert not (tmp_path / fixture.PLUGIN).exists()


@pytest.mark.parametrize('ci_flag', ['CI', 'GITHUB_ACTIONS', None])
def test_late_native_install_failure_is_fatal_only_in_ci(tmp_path, monkeypatch, ci_flag):
    from tests.console import conftest as console
    monkeypatch.delenv('CI', raising=False)
    monkeypatch.delenv('GITHUB_ACTIONS', raising=False)
    if ci_flag:
        monkeypatch.setenv(ci_flag, 'true')
    monkeypatch.setattr(console, 'COPIED_PLUGINS', ())
    failed = subprocess.CompletedProcess([], 1, 'x' * 5000, 'fixture install failed')
    monkeypatch.setattr(console.subprocess, 'run', lambda *args, **kwargs: failed)
    monkeypatch.setattr(console, 'run_python', lambda *args: subprocess.CompletedProcess([], 0, '', ''))
    server = object.__new__(console.Server)
    server.root = tmp_path
    server.plugins = tmp_path / 'plugins'
    server.env = {}
    server.native = True
    if ci_flag:
        with pytest.raises(pytest.fail.Exception) as error:
            server.prepare()
        assert 'fixture install failed' in str(error.value)
        assert len(str(error.value)) < 1700
    else:
        server.prepare()
        assert server.native_built is False
    assert (tmp_path / 'native_build.log').read_text().endswith('fixture install failed')


def cargo_events(repo, sdk, binary):
    return [
        {'reason': 'compiler-artifact', 'target': {'name': 'meerschaum_plugin', 'kind': ['lib'],
         'src_path': str(repo / 'crates/meerschaum-plugin/src/lib.rs')}, 'filenames': [str(sdk)]},
        {'reason': 'compiler-artifact', 'target': {'name': 'mrsm', 'kind': ['bin']},
         'executable': str(binary)},
        {'reason': 'build-finished', 'success': True},
    ]


@pytest.mark.parametrize('problem', [None, 'failed-build', 'missing-cli', 'registry-sdk', 'two-sdks',
                                   'wrong-profile'])
def test_sdk_comes_from_matching_cargo_events_not_newest_cached_rlib(tmp_path, problem):
    repo = tmp_path / 'repo'
    deps = repo / 'target/debug/deps'
    deps.mkdir(parents=True)
    sdk = deps / 'libmeerschaum_plugin-current.rlib'
    sdk.write_bytes(b'current')
    stale = deps / 'libmeerschaum_plugin-newer-stale.rlib'
    stale.write_bytes(b'other-features')
    os.utime(stale, (2_000_000_000, 2_000_000_000))
    binary = repo / 'target/debug/mrsm'
    binary.write_bytes(b'\x7fELFcli')
    events = cargo_events(repo, sdk, binary)
    if problem == 'failed-build':
        events[-1]['success'] = False
    elif problem == 'missing-cli':
        events.pop(1)
    elif problem == 'registry-sdk':
        events[0]['target']['src_path'] = '/registry/meerschaum-plugin/src/lib.rs'
    elif problem == 'two-sdks':
        events[0]['filenames'].append(str(stale))
    messages = tmp_path / 'cargo.jsonl'
    messages.write_text('\n'.join(json.dumps(event) for event in events))
    if problem is None:
        assert fixture.select_sdk(messages, binary, 'debug', repo) == sdk
    else:
        with pytest.raises(fixture.artifacts.ArtifactError):
            fixture.select_sdk(messages, binary, 'release' if problem == 'wrong-profile' else 'debug', repo)


def test_owned_service_environment_keeps_verified_fixture_inputs():
    values = {name: f'value-{name}' for name in (
        'MRSM_TEST_CONSOLE_NATIVE_FIXTURE', 'MRSM_TEST_CONSOLE_NATIVE_MANIFEST',
        'MRSM_TEST_CONSOLE_NATIVE_PROFILE', 'MRSM_TEST_ARTIFACT_MANIFEST')}
    assert clean_environment(values) == values


def test_workflows_produce_separate_fixtures_and_only_console_core_downloads_them():
    wheels = yaml.safe_load((REPO / '.github/workflows/wheels.yaml').read_text())
    pytest_workflow = yaml.safe_load((REPO / '.github/workflows/pytest.yaml').read_text())
    release = [step for step in wheels['jobs']['binaries']['steps']
               if step.get('with', {}).get('name') == 'console-native-fixture-release']
    assert len(release) == 1 and 'x86_64-unknown-linux-gnu' in release[0]['if']
    debug = [step for step in pytest_workflow['jobs']['artifacts']['steps']
             if step.get('with', {}).get('name') == 'console-native-fixture-debug']
    assert len(debug) == 1 and debug[0]['with']['if-no-files-found'] == 'error'
    assert debug[0]['if'] == "${{ env.MRSM_TEST_CONSOLE == '1' }}"
    downloads = [step for step in pytest_workflow['jobs']['tests']['steps']
                 if step.get('uses', '').startswith('actions/download-artifact')
                 and 'console-native-fixture' in step.get('with', {}).get('path', '')]
    assert len(downloads) == 1
    assert "matrix.shard == 'core'" in downloads[0]['if']
    assert 'MRSM_TEST_CONSOLE' in downloads[0]['if']
    assert 'inputs.artifacts_run_id || github.run_id' in downloads[0]['with']['run-id']
    # Fixtures cannot enter a shipping binary's complete-payload provenance.
    shipping = [step for step in wheels['jobs']['binaries']['steps']
                if step.get('with', {}).get('name', '').startswith('mrsm-')]
    assert len(shipping) == 1 and shipping[0]['with']['path'] == 'out/*'


@pytest.mark.parametrize('shard', ['core', 'postgres', 'mysql', 'mssql-valkey', 'oracle'])
@pytest.mark.parametrize('required', ['0', '1'])
def test_only_required_console_core_fetches_the_fixture(shard, required):
    workflow = yaml.safe_load((REPO / '.github/workflows/pytest.yaml').read_text())
    step = next(step for step in workflow['jobs']['tests']['steps']
                if step.get('uses', '').startswith('actions/download-artifact')
                and 'console-native-fixture' in step.get('with', {}).get('path', ''))
    assert bool(_evaluate_actions(step['if'], {
        'matrix.shard': shard, 'env.MRSM_TEST_CONSOLE': required,
    })) is (shard == 'core' and required == '1')


@pytest.mark.parametrize('event,requested,expected', [
    ('pull_request', False, False), ('push', False, False),
    ('schedule', False, True), ('workflow_dispatch', True, True),
])
def test_debug_producer_has_no_fixture_cost_without_console(event, requested, expected):
    workflow = yaml.safe_load((REPO / '.github/workflows/pytest.yaml').read_text())
    flag = _evaluate_actions(workflow['jobs']['artifacts']['env']['MRSM_TEST_CONSOLE'], {
        'inputs.run_console': requested, 'github.event_name': event,
    })
    assert flag == ('1' if expected else '0')
    script = (REPO / 'scripts/ci/build_pytest.sh').read_text()
    guard = script.index('if [ "${MRSM_TEST_CONSOLE:-0}" = 1 ]; then')
    assert guard < script.index('console_fixture.py build') < script.index('console_fixture.py record')
