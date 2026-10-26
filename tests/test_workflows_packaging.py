"""
Wave 18 (PK): the packaging workflows' structure and the wheel tag guard.

`.github/workflows/wheels.yaml` builds the musllinux and macOS x86_64 wheels and `docker-build.yaml`
builds on self-hosted runners; nothing here runs a workflow. The tests parse the YAML
(with the extension's native loader, no PyYAML needed) and run the wheel tag/size guard script that
`wheels.yaml` embeds against fabricated wheels, so a regex mistake shows up before a CI run.
"""

import os
import json
import runpy
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
WORKFLOWS = REPO / '.github' / 'workflows'
RELEASE_MATRIX = REPO / 'scripts' / 'ci' / 'release_matrix.py'
release_matrices = runpy.run_path(str(RELEASE_MATRIX))['release_matrices']


def _matrix(workflow, job, name, version='5.0.0'):
    """Resolve the workflow's matrix output through the same version policy CI executes."""
    assert workflow['jobs'][job]['strategy']['matrix'] == (
        '${{ fromJSON(needs.metadata.outputs.' + name + ') }}'
    )
    metadata = workflow['jobs']['metadata']
    assert metadata['outputs'][name] == '${{ steps.matrix.outputs.' + name + ' }}'
    step = next(s for s in metadata['steps'] if s.get('id') == 'matrix')
    assert 'scripts/ci/release_matrix.py --github-output "$GITHUB_OUTPUT"' in step['run']
    return release_matrices(version)[name]['include']


def _load(name):
    from meerschaum.utils.yaml import yaml

    return yaml.safe_load((WORKFLOWS / name).read_text(encoding='utf-8'))


@pytest.fixture(scope='module')
def wheels():
    return _load('wheels.yaml')


@pytest.fixture(scope='module')
def docker_build():
    return _load('docker-build.yaml')


def _step(job, name_prefix):
    return next(s for s in job['steps'] if str(s.get('name', '')).startswith(name_prefix))


def _artifact(entry):
    """The upload name the workflow's expression gives a matrix entry."""
    if entry.get('artifact'):
        return entry['artifact']
    return 'wheels-' + entry['os'] + ('-cp314t' if entry.get('freethreaded') else '')


def test_the_wheel_matrix_covers_musllinux_and_intel_macos(wheels):
    include = _matrix(wheels, 'build', 'wheels')
    musl = {e['wheel_arch']: e for e in include if e.get('musl')}
    assert set(musl) == {'x86_64', 'aarch64'}
    for arch, entry in musl.items():
        assert entry['manylinux'] == 'musllinux_1_2'
        assert entry['target'] == f'{arch}-unknown-linux-musl'
        assert entry['os'] == 'ubuntu-latest', 'cross-built in the rust-musl-cross container'
        assert entry['artifact'] == f'wheels-musllinux-{arch}'
    assert any(e['os'] == 'macos-15-intel' for e in include)
    assert any(e['os'] == 'ubuntu-24.04-arm' and not e.get('musl') for e in include)
    names = [_artifact(e) for e in include]
    assert len(names) == len(set(names)), f'two matrix entries upload the same artifact: {names}'
    # The default entries keep the names the smoke jobs download.
    assert {'wheels-ubuntu-latest', 'wheels-ubuntu-24.04-arm', 'wheels-macos-latest',
            'wheels-macos-15-intel', 'wheels-windows-latest'} <= set(names)


def test_every_wheel_is_smoke_tested_where_it_runs(wheels):
    jobs = wheels['jobs']
    smoke_os = {e['os'] for e in _matrix(wheels, 'smoke', 'smoke')}
    assert smoke_os >= {'ubuntu-latest', 'ubuntu-24.04-arm', 'macos-latest', 'macos-15-intel',
                        'windows-latest'}
    musl = jobs['smoke-musl']
    assert set(musl['needs']) == {'metadata', 'build'}
    runs = {(e['os'], e['arch']) for e in _matrix(wheels, 'smoke-musl', 'smoke_musl')}
    assert runs == {('ubuntu-latest', 'x86_64'), ('ubuntu-24.04-arm', 'aarch64')}, (
        'each musllinux wheel must first run on a runner of its own architecture'
    )
    download = next(
        s for s in musl['steps'] if str(s.get('uses', '')).startswith('actions/download-artifact')
    )
    assert download['with']['name'] == 'wheels-musllinux-${{ matrix.arch }}'
    step = next(s for s in musl['steps'] if 'alpine' in str(s.get('run', '')))['run']
    assert 'alpine:3.22 sh /smoke.sh' in step and 'smoke_wheel_alpine.sh' in step
    checkout = next(s for s in musl['steps'] if str(s.get('uses', '')).startswith('actions/checkout'))
    assert 'smoke_wheel_alpine.sh' in checkout['with']['sparse-checkout']
    script = (REPO / 'scripts' / 'docker' / 'smoke_wheel_alpine.sh').read_text(encoding='utf-8')
    assert '--no-index' in script and 'MRSM_NO_AUTO_INSTALL=1' in script
    assert 'smoke_facade.py' in script and 'install_requests' in script
    assert not script.startswith('#!/usr/bin/env bash'), 'Alpine has no bash'
    ### POSIX syntax check with the host's `sh` (dash / bash in sh mode; the container runs busybox ash).
    subprocess.run(['sh', '-n', str(REPO / 'scripts' / 'docker' / 'smoke_wheel_alpine.sh')], check=True)


def test_the_binaries_include_intel_macos(wheels):
    include = _matrix(wheels, 'binaries', 'binaries')
    targets = {e['target']: e['os'] for e in include}
    assert targets['x86_64-apple-darwin'] == 'macos-15-intel'
    assert set(targets) == {
        'x86_64-unknown-linux-gnu', 'aarch64-unknown-linux-gnu', 'aarch64-apple-darwin',
        'x86_64-apple-darwin', 'x86_64-pc-windows-msvc',
    }


def test_docker_images_build_on_self_hosted_runners(docker_build):
    job = docker_build['jobs']['buildx']
    include = _matrix(docker_build, 'buildx', 'docker')
    assert {(e['arch'], e['platform']) for e in include} == {
        ('amd64', 'linux/amd64'), ('arm64', 'linux/arm64'),
    }
    assert job['runs-on'] == ['self-hosted', 'Linux', 'X64']
    assert job['env']['PYTHON_BIN'] == 'python3'
    assert not any('setup-python' in step.get('uses', '') for step in job['steps'])
    assert job['strategy']['max-parallel'] == 1
    assert docker_build['concurrency']['cancel-in-progress'] is False
    qemu = next(s for s in job['steps'] if 'setup-qemu-action' in s.get('uses', ''))
    assert qemu['if'] == "matrix.arch == 'arm64'"
    assert qemu['with']['platforms'] == 'arm64'
    setup = next(s for s in job['steps'] if 'setup-buildx-action' in s.get('uses', ''))
    assert setup['with']['keep-state'] is True
    assert setup['with']['use'] is False
    assert setup['with']['name'] == job['env']['BUILDX_BUILDER']
    assert 'matrix.arch' in setup['with']['name']
    isolate = _step(job, 'Isolate Docker configuration')['run']
    assert 'mktemp -d "$RUNNER_TEMP/mrsm-docker.' in isolate
    assert 'DOCKER_CONFIG=$docker_config' in isolate
    assert job['env']['DOCKER_DEFAULT_PLATFORM'] == '${{ matrix.platform }}'
    assert 'memory=' in setup['with']['driver-opts']
    build = _step(job, 'Build Docker images')
    assert '--platform ${{ matrix.platform }}' in build['run']
    push = next(s for s in job['steps'] if 'arch-tags' in str(s.get('run', '')))
    assert push['if'] == 'inputs.publish'
    assert '--push --arch-tags --platform ${{ matrix.platform }}' in push['run']

    publish = docker_build['jobs']['publish']
    assert set(publish['needs']) == {'metadata', 'buildx'}
    assert publish['runs-on'] == job['runs-on']
    assert 'inputs.publish' in publish['if']
    assert publish['env']['EXPECTED_PLATFORMS'] == '${{ needs.metadata.outputs.platforms }}'
    runs = '\n'.join(str(s.get('run', '')) for s in publish['steps'])
    assert 'buildx.sh --merge' in runs
    assert 'platforms == expected' in runs


@pytest.mark.parametrize('version', ['5.0.0-rc.4', '5.0.0-alpha.1', '5.0.0-beta.1', '5.0.0-dev.1'])
def test_prereleases_only_build_linux_x86_64(version, wheels, docker_build):
    matrices = release_matrices(version)
    assert matrices['prerelease'] is True
    assert matrices['docker_platforms'] == 'linux/amd64'
    assert _matrix(docker_build, 'buildx', 'docker', version) == [
        {'arch': 'amd64', 'platform': 'linux/amd64'},
    ]
    builds = _matrix(wheels, 'build', 'wheels', version)
    assert len(builds) == 3  # glibc abi3, musl abi3, experimental glibc cp314t
    assert all(e['os'] == 'ubuntu-latest' for e in builds)
    assert {e.get('target', 'x86_64-unknown-linux-gnu') for e in builds} == {
        'x86_64-unknown-linux-gnu', 'x86_64-unknown-linux-musl',
    }
    assert [e['target'] for e in _matrix(wheels, 'binaries', 'binaries', version)] == [
        'x86_64-unknown-linux-gnu',
    ]
    assert {e['os'] for e in _matrix(wheels, 'smoke', 'smoke', version)} == {'ubuntu-latest'}
    assert _matrix(wheels, 'smoke-musl', 'smoke_musl', version) == [
        {'os': 'ubuntu-latest', 'arch': 'x86_64'},
    ]
    assert wheels['jobs']['sdist']['needs'] == 'metadata'


@pytest.mark.parametrize('version', ['5.0.0-rc.4', '5.0.0', '5.0.0+build-1'])
def test_every_selected_smoke_job_has_a_built_artifact(version):
    matrix = release_matrices(version)
    builds = {_artifact(entry) for entry in matrix['wheels']['include']}
    downloads = {'wheels-' + entry['os'] for entry in matrix['smoke']['include']}
    downloads |= {'wheels-musllinux-' + entry['arch'] for entry in matrix['smoke_musl']['include']}
    downloads.add('wheels-ubuntu-latest-cp314t')
    assert builds == downloads
    assert matrix['prerelease'] == ('-rc.' in version)


def test_release_matrix_cli_emits_actions_outputs(tmp_path):
    output = tmp_path / 'outputs'
    result = subprocess.run(
        [sys.executable, '-I', str(RELEASE_MATRIX), '--version', '5.0.0-rc.4',
         '--github-output', str(output)], capture_output=True, text=True, check=True,
    )
    matrix = json.loads(result.stdout)
    outputs = dict(line.split('=', 1) for line in output.read_text().splitlines())
    assert outputs['prerelease'] == 'true'
    assert outputs['docker_platforms'] == 'linux/amd64'
    for key in ('wheels', 'binaries', 'smoke', 'smoke_musl', 'docker'):
        assert json.loads(outputs[key]) == matrix[key]


def test_nothing_publishes_without_the_input(docker_build):
    for name, job in docker_build['jobs'].items():
        for step in job['steps']:
            text = str(step.get('run', ''))
            if '--push' in text or '--merge' in text or 'login-action' in str(step.get('uses', '')):
                assert 'inputs.publish' in str(step.get('if', '')) or 'inputs.publish' in str(
                    job.get('if', '')
                ), (name, step.get('name'))
    ### YAML 1.1 reads the key `on` as the boolean True.
    triggers = docker_build.get('on', docker_build.get(True))
    assert triggers['workflow_dispatch']['inputs']['publish']['default'] is False
    assert set(triggers) == {'workflow_dispatch'}, 'a push or pull request must not build and publish'


# ---------------------------------------------------------------------------------------------
# The wheel tag guard embedded in wheels.yaml
# ---------------------------------------------------------------------------------------------

def _guard_script(wheels):
    build = wheels['jobs']['build']
    run = _step(build, 'Check the wheel tag and size')['run']
    start = run.index("<<'PY'\n") + len("<<'PY'\n")
    return run[start:run.index('\nPY')]


def _guard(wheels, tmp_path, tag, *, os_name, runner_arch, wheel_arch='', musl=False, size=None):
    dist = tmp_path / 'dist'
    dist.mkdir()
    wheel = dist / f'meerschaum-5.0.0rc2-cp311-abi3-{tag}.whl'
    with zipfile.ZipFile(wheel, 'w') as zf:
        zf.writestr(
            'meerschaum-5.0.0rc2.dist-info/WHEEL',
            f'Wheel-Version: 1.0\nTag: cp311-abi3-{tag}\n',
        )
        if size:
            zf.writestr('padding', b'0' * size)
    env = {
        **{k: v for k, v in os.environ.items() if not k.startswith('MRSM_')},
        'RUNNER_OS_NAME': os_name, 'RUNNER_ARCH_NAME': runner_arch,
        'WHEEL_ARCH': wheel_arch, 'EXPECT_MUSL': '1' if musl else '',
        'MAX_WHEEL_BYTES': '99614720',
    }
    result = subprocess.run(
        [sys.executable, '-I', '-c', _guard_script(wheels)],
        cwd=tmp_path, env=env, capture_output=True, text=True, timeout=60,
    )
    return result


@pytest.mark.parametrize('tag,kwargs,ok', [
    ('manylinux_2_28_x86_64', dict(os_name='Linux', runner_arch='X64'), True),
    ('manylinux_2_28_aarch64', dict(os_name='Linux', runner_arch='ARM64'), True),
    ('manylinux_2_34_x86_64', dict(os_name='Linux', runner_arch='X64'), False),
    ('manylinux_2_28_aarch64', dict(os_name='Linux', runner_arch='X64'), False),
    ('linux_x86_64', dict(os_name='Linux', runner_arch='X64'), False),
    ('musllinux_1_2_x86_64', dict(os_name='Linux', runner_arch='X64', wheel_arch='x86_64', musl=True), True),
    # Cross-built on an x86_64 runner: the matrix says the wheel is aarch64.
    ('musllinux_1_2_aarch64', dict(os_name='Linux', runner_arch='X64', wheel_arch='aarch64', musl=True), True),
    ('musllinux_1_2_x86_64', dict(os_name='Linux', runner_arch='X64', wheel_arch='aarch64', musl=True), False),
    ('musllinux_1_3_x86_64', dict(os_name='Linux', runner_arch='X64', wheel_arch='x86_64', musl=True), False),
    ('manylinux_2_28_x86_64', dict(os_name='Linux', runner_arch='X64', wheel_arch='x86_64', musl=True), False),
    ('macosx_11_0_arm64', dict(os_name='macOS', runner_arch='ARM64'), True),
    ('macosx_10_12_x86_64', dict(os_name='macOS', runner_arch='X64'), True),
    ('win_amd64', dict(os_name='Windows', runner_arch='X64'), True),
])
def test_the_wheel_tag_guard(wheels, tmp_path, tag, kwargs, ok):
    result = _guard(wheels, tmp_path, tag, **kwargs)
    assert (result.returncode == 0) == ok, (tag, kwargs, result.stdout, result.stderr)


def test_the_wheel_size_guard_still_fails_over_95_mib(wheels, tmp_path):
    result = _guard(
        wheels, tmp_path, 'musllinux_1_2_x86_64', os_name='Linux', runner_arch='X64',
        wheel_arch='x86_64', musl=True, size=100 * 1024 * 1024,
    )
    assert result.returncode != 0 and '95 MiB' in (result.stdout + result.stderr)


# ---------------------------------------------------------------------------------------------
# CI structure: quick builds for development, release builds only on dispatch / tag
# (docs/design/v5-build.md "CI: what runs when")
# ---------------------------------------------------------------------------------------------

import re


def _triggers(workflow):
    ### YAML 1.1 reads the key `on` as the boolean True.
    return workflow.get('on', workflow.get(True))


@pytest.fixture(scope='module')
def rust():
    return _load('rust.yaml')


@pytest.fixture(scope='module')
def pytest_workflow():
    return _load('pytest.yaml')


def test_the_release_workflows_run_only_on_dispatch_or_tag(wheels, docker_build):
    triggers = _triggers(wheels)
    assert set(triggers) == {'workflow_dispatch', 'push'}, 'no pull_request: release builds are --release'
    assert triggers['push'] == {'tags': ['v5*']}, 'a branch push must not build the wheels'
    assert set(_triggers(docker_build)) == {'workflow_dispatch'}


def _workflow_texts():
    return {path.name: path.read_text(encoding='utf-8') for path in sorted(WORKFLOWS.glob('*.yaml'))}


def test_the_ci_profile_is_a_quick_build_and_never_ships():
    manifest = (REPO / 'Cargo.toml').read_text(encoding='utf-8')
    section = manifest[manifest.index('[profile.ci]'):]
    section = section[:section.index('\n\n')]
    assert 'inherits = "release"' in section
    assert 'lto = false' in section and 'opt-level = 1' in section
    assert 'codegen-units = 256' in section and 'line-tables-only' in section
    ### The release profile is untouched: thin LTO, one codegen unit.
    release = manifest[manifest.index('[profile.release]'):]
    release = release[:release.index('\n\n')]
    assert 'lto = "thin"' in release and 'codegen-units = 1' in release
    script = (REPO / 'scripts' / 'rust' / 'build_py.sh').read_text(encoding='utf-8')
    assert 'profile="release"' in script.split('while [', 1)[0], 'build_py.sh must default to release'
    ### Nothing that ships names the ci profile.
    for name in ('wheels.yaml', 'docker-build.yaml'):
        assert 'profile ci' not in (WORKFLOWS / name).read_text(encoding='utf-8'), name
    for path in [REPO / 'Dockerfile', REPO / 'pyproject.toml', REPO / 'crates' / 'meerschaum-py' / 'pyproject.toml',
                 *(REPO / 'scripts' / 'docker').glob('*')]:
        if path.is_file():
            text = path.read_text(encoding='utf-8', errors='ignore')
            assert 'profile ci' not in text and '--profile=ci' not in text, path


def test_pytest_builds_the_extension_with_the_ci_profile(pytest_workflow):
    tests = pytest_workflow['jobs']['tests']
    build = _step(tests, 'Build the native Python extension')
    assert 'scripts/rust/build_py.sh --profile ci' in build['run']
    assert 'scripts/rust/build_py.sh --release' in build['run'], 'the release gate can test the shipped profile'
    assert build['env']['EXT_PROFILE'] == "${{ inputs.profile || 'ci' }}"
    inputs = _triggers(pytest_workflow)['workflow_dispatch']['inputs']
    assert inputs['profile']['default'] == 'ci' and inputs['profile']['options'] == ['ci', 'release']
    assert inputs['flavors']['default'] == 'all'
    ### One run of the suite at a time per host: the test containers and port 8989 are shared.
    assert tests['concurrency'] == {'group': 'mrsm-test-services', 'cancel-in-progress': False}
    assert tests['runs-on'] == 'self-hosted'
    assert 'head.repo.full_name == github.repository' in tests['if'], 'no fork code on the self-hosted runner'


def test_every_workflow_job_is_covered_by_a_summary(rust, pytest_workflow):
    for workflow in (rust, pytest_workflow):
        jobs = workflow['jobs']
        summary = jobs['summary']
        assert summary['if'] == '${{ always() }}'
        assert summary['runs-on'] == 'ubuntu-latest'
        assert set(summary['needs']) == set(jobs) - {'summary'}, 'a job missing from the summary is ignored'


def test_rust_jobs_follow_the_changes_job(rust):
    jobs = rust['jobs']
    triggers = _triggers(rust)
    assert 'paths' not in triggers['push'] and 'paths' not in (triggers['pull_request'] or {}), (
        'the `changes` job decides, so a skipped job never leaves a required check pending'
    )
    assert triggers['push']['tags'] == ['v5*']
    gates = {
        'fmt': 'rust', 'clippy': 'rust', 'check': 'rust', 'test': 'rust_test', 'parity': 'parity',
        'frontcrate': 'frontcrate', 'package': 'package', 'duckdb-platforms': 'duckdb', 'build': 'full',
    }
    assert set(gates) == set(jobs) - {'changes', 'summary'}
    for name, flag in gates.items():
        job = jobs[name]
        assert 'changes' in job['needs'] if isinstance(job['needs'], list) else job['needs'] == 'changes', name
        assert f"needs.changes.outputs.{flag} == 'true'" in job['if'], name
    ### The release binary (thin LTO, one codegen unit) is dispatch / tag only.
    assert jobs['build']['steps'][-2]['run'] == 'scripts/rust/ci.sh build'
    for name, job in jobs.items():
        if job['runs-on'] == 'self-hosted':
            assert 'head.repo.full_name == github.repository' in job['if'], name
            ### The workspace's target/ is the build cache: checkout never wipes it.
            clean = _step(job, 'Clean the workspace')['run']
            assert re.search(r'-e /target(\s|$)', clean), name
            assert any(s.get('with', {}).get('clean') is False for s in job['steps'] if 'checkout' in str(s.get('uses'))), name


def test_a_dispatch_or_push_does_not_cancel_the_other(rust, pytest_workflow):
    for workflow in (rust, pytest_workflow):
        group = workflow['concurrency']['group']
        ### A push and a pull request of one branch share a group (the same commit runs once);
        ### a dispatch keeps its own group, so it is never cancelled by either.
        assert "&& 'auto' || github.event_name" in group, group
        assert 'github.head_ref || github.ref_name' in group, group
        assert 'workflow_dispatch' not in workflow['concurrency']['cancel-in-progress'], 'a dispatch is the release gate'


def test_self_hosted_jobs_sharing_host_state_never_overlap(rust, pytest_workflow):
    ### A dispatched release run waits for these instead of racing a push or pull request run.
    for job, group in (('test', 'mrsm-rust-test'), ('parity', 'mrsm-rust-parity')):
        assert rust['jobs'][job]['concurrency'] == {'group': group, 'cancel-in-progress': False}, job
    assert pytest_workflow['jobs']['tests']['concurrency'] == {
        'group': 'mrsm-test-services', 'cancel-in-progress': False,
    }


def test_rust_cache_is_pinned_by_commit_everywhere():
    found = 0
    for name, text in _workflow_texts().items():
        for match in re.finditer(r'uses:\s*Swatinem/rust-cache@(\S+)', text):
            found += 1
            assert re.fullmatch(r'[0-9a-f]{40}', match.group(1)), f'{name}: pin rust-cache by full SHA'
    assert found >= 4


def test_nextest_runs_the_tests_and_doc_tests_still_run():
    script = (REPO / 'scripts' / 'rust' / 'ci.sh').read_text(encoding='utf-8')
    body = script[script.index('step_test()'):script.index('step_build()')]
    assert 'cargo nextest run --workspace' in body
    assert 'cargo test --workspace --doc' in body, 'nextest does not run doc tests'
    assert 'else' in body and 'cargo test --workspace\n' in body, 'plain cargo test without nextest'


def test_the_cargo_tests_build_the_v4_reference_before_running():
    """Each self-hosted runner has its own workspace: `.venv-ref/` exists only where `parity` ran."""
    script = (REPO / 'scripts' / 'rust' / 'ci.sh').read_text(encoding='utf-8')
    body = script[script.index('step_test()'):script.index('step_build()')]
    assert 'scripts/parity/setup_reference.sh' in body
    assert body.index('setup_reference.sh') < body.index('cargo nextest run')


def test_the_ci_tools_are_pinned_by_checksum():
    script = (REPO / 'scripts' / 'rust' / 'ci_tools.sh').read_text(encoding='utf-8')
    shas = re.findall(r'^[A-Z]+_SHA_(?:X86_64|AARCH64)="([0-9a-f]+)"$', script, re.M)
    assert len(shas) == 6 and all(len(s) == 64 for s in shas)
    assert re.search(r'^SCCACHE_VERSION="\d+\.\d+\.\d+"$', script, re.M)
    assert 'sha256sum' in script and 'checksum mismatch' in script
    subprocess.run(['bash', '-n', str(REPO / 'scripts' / 'rust' / 'ci_tools.sh')], check=True)
    subprocess.run(['bash', '-n', str(REPO / 'scripts' / 'rust' / 'ci.sh')], check=True)
