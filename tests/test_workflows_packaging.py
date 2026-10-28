"""
Packaging boundaries, test coverage, and the wheel tag guard.

`.github/workflows/wheels.yaml` builds the musllinux and macOS x86_64 wheels and `docker-build.yaml`
builds on self-hosted runners; nothing here runs a workflow. The tests parse the YAML
(with the extension's native loader, no PyYAML needed) and run the wheel tag/size guard script that
`wheel-platform.yaml` embeds against fabricated wheels, so a regex mistake shows up before a CI run.
"""

import ast
import json
import os
import re
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
def wheel_platform(wheels):
    called = wheels['jobs']['build']['uses']
    assert called.startswith('./.github/workflows/'), 'platform jobs must test the caller revision'
    return _load(Path(called).name)


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


def test_every_wheel_is_smoke_tested_where_it_runs(wheels, wheel_platform):
    jobs = wheel_platform['jobs']
    standard = _matrix(wheels, 'build', 'wheels')
    native = [entry for entry in standard if not entry.get('musl')]
    assert {entry.get('smoke_os', entry['os']) for entry in native} == {
        'ubuntu-latest', 'ubuntu-24.04-arm', 'macos-latest', 'macos-15-intel', 'windows-latest',
    }
    assert jobs['smoke']['strategy']['matrix']['python'] == ['3.11', '3.14']
    assert jobs['smoke']['runs-on'] == '${{ inputs.smoke-os }}'
    musl = jobs['smoke-musl']
    assert musl['needs'] == 'build'
    runs = {(entry['smoke_os'], entry['wheel_arch']) for entry in standard if entry.get('musl')}
    assert runs == {('ubuntu-latest', 'x86_64'), ('ubuntu-24.04-arm', 'aarch64')}, (
        'each musllinux wheel must first run on a runner of its own architecture'
    )
    download = next(
        s for s in musl['steps'] if str(s.get('uses', '')).startswith('actions/download-artifact')
    )
    assert download['with']['name'] == 'wheel-candidate-${{ inputs.artifact }}'
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
    assert len(builds) == 2  # Required glibc and musl abi3 wheels.
    assert all(e['os'] == 'ubuntu-latest' for e in builds)
    assert {e.get('target', 'x86_64-unknown-linux-gnu') for e in builds} == {
        'x86_64-unknown-linux-gnu', 'x86_64-unknown-linux-musl',
    }
    assert [e['target'] for e in _matrix(wheels, 'binaries', 'binaries', version)] == [
        'x86_64-unknown-linux-gnu',
    ]
    experimental = _matrix(wheels, 'experimental', 'experimental_wheels', version)
    assert len(experimental) == 1 and experimental[0]['freethreaded'] is True
    assert experimental[0]['os'] == 'ubuntu-latest'
    assert not any(entry.get('freethreaded') for entry in builds)
    assert wheels['jobs']['sdist']['needs'] == 'metadata'


@pytest.mark.parametrize('version', ['5.0.0-rc.4', '5.0.0', '5.0.0+build-1'])
def test_every_selected_smoke_job_has_a_built_artifact(version):
    matrix = release_matrices(version)
    builds = {_artifact(entry) for entry in matrix['wheels']['include']}
    downloads = {'wheels-' + entry['os'] for entry in matrix['smoke']['include']}
    downloads |= {'wheels-musllinux-' + entry['arch'] for entry in matrix['smoke_musl']['include']}
    assert builds == downloads
    assert {_artifact(entry) for entry in matrix['experimental_wheels']['include']} == {
        'wheels-ubuntu-latest-cp314t',
    }
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
    for key in ('wheels', 'experimental_wheels', 'binaries', 'smoke', 'smoke_musl', 'docker'):
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
# The wheel tag guard used by the actual platform build
# ---------------------------------------------------------------------------------------------

def _guard_script(wheels):
    build = wheels['jobs']['build']
    if 'uses' in build:
        build = _load(Path(build['uses']).name)['jobs']['build']
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
        **{k: v for k, v in os.environ.items()
           if not k.startswith('MRSM_') and k not in ('TMUX', 'TMUX_PANE')},
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
# CI structure: reusable validation and explicit release artifact builds
# (docs/design/v5-build.md "CI: what runs when")
# ---------------------------------------------------------------------------------------------

def _triggers(workflow):
    ### YAML 1.1 reads the key `on` as the boolean True.
    return workflow.get('on', workflow.get(True))


@pytest.fixture(scope='module')
def rust():
    return _load('rust.yaml')


@pytest.fixture(scope='module')
def pytest_workflow():
    return _load('pytest.yaml')


def test_release_artifacts_are_built_explicitly_and_tags_do_not_rebuild(wheels, docker_build):
    triggers = _triggers(wheels)
    assert set(triggers) == {'workflow_dispatch', 'workflow_call'}
    for trigger in triggers.values():
        assert trigger['inputs']['experimental']['default'] is False
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
    for name in ('wheels.yaml', 'wheel-platform.yaml', 'docker-build.yaml'):
        assert 'profile ci' not in (WORKFLOWS / name).read_text(encoding='utf-8'), name
    for path in [REPO / 'Dockerfile', REPO / 'pyproject.toml', REPO / 'crates' / 'meerschaum-py' / 'pyproject.toml',
                 *(REPO / 'scripts' / 'docker').glob('*')]:
        if path.is_file():
            text = path.read_text(encoding='utf-8', errors='ignore')
            assert 'profile ci' not in text and '--profile=ci' not in text, path


def test_pytest_builds_once_and_consumes_verified_complete_artifacts(pytest_workflow):
    jobs = pytest_workflow['jobs']
    build, tests = jobs['artifacts'], jobs['tests']
    assert 'artifacts' in tests['needs']
    assert not any('build_py.sh' in step.get('run', '') or 'cargo build' in step.get('run', '')
                   for step in tests['steps']), 'each shard consumes the shared artifact instead of rebuilding'
    assert any('scripts/ci/build_pytest.sh' in step.get('run', '') for step in build['steps'])
    assert "inputs.profile != 'release'" in build['if'], 'release tests reuse shipping artifacts'
    downloads = [step['with'] for step in tests['steps']
                 if str(step.get('uses', '')).startswith('actions/download-artifact')]
    assert len(downloads) == 3
    assert all('inputs.artifacts_run_id || github.run_id' in step['run-id'] for step in downloads)
    prepare = next(i for i, step in enumerate(tests['steps'])
                   if 'scripts/ci/prepare_pytest.sh' in step.get('run', ''))
    run = next(i for i, step in enumerate(tests['steps'])
               if 'scripts/ci/services.py run ' in step.get('run', ''))
    assert prepare < run, 'verify and install artifacts before starting test services'

    builder = (REPO / 'scripts/ci/build_pytest.sh').read_text()
    assert 'maturin" build --profile ci --locked' in builder
    assert 'artifacts.py create --kind wheel --profile ci' in builder
    assert 'artifacts.py create --kind binary --profile debug' in builder
    consumer = (REPO / 'scripts/ci/prepare_pytest.sh').read_text()
    assert consumer.index('artifacts.py verify') < consumer.index('-m pip install')
    assert 'artifacts.py overlay-wheel' in consumer, 'bundled native libraries must travel with the extension'
    assert 'MRSM_TEST_ARTIFACT_MANIFEST=' in consumer
    inputs = _triggers(pytest_workflow)['workflow_dispatch']['inputs']
    assert inputs['profile']['default'] == 'ci' and inputs['profile']['options'] == ['ci', 'release']
    assert inputs['shards']['default'] == 'all'
    assert tests['runs-on'] == 'self-hosted'
    called = _triggers(pytest_workflow)['workflow_call']['inputs']
    assert called['profile']['default'] == 'ci' and called['full']['default'] is True


def test_every_workflow_job_is_covered_by_a_summary(rust, pytest_workflow):
    for workflow in (rust, pytest_workflow):
        jobs = workflow['jobs']
        summary = jobs['summary']
        assert summary['if'] == '${{ always() }}'
        assert summary['runs-on'] == 'ubuntu-latest'
        assert set(summary['needs']) == set(jobs) - {'summary'}, 'a job missing from the summary is ignored'


@pytest.mark.parametrize('status,ok', [('success', True), ('skipped', True), ('failure', False), ('cancelled', False)])
def test_required_summaries_reject_failed_or_cancelled_work(rust, pytest_workflow, status, ok, tmp_path):
    for workflow in (rust, pytest_workflow):
        summary = workflow['jobs']['summary']
        run = next(step['run'] for step in summary['steps'] if "<<'PY'" in step.get('run', ''))
        script = run.split("<<'PY'\n", 1)[1].rsplit('\nPY', 1)[0]
        results = {name: {'result': 'success'} for name in summary['needs']}
        results[next(name for name in results if name != 'changes')]['result'] = status
        env = {key: value for key, value in os.environ.items()
               if not key.startswith('MRSM_') and key not in ('TMUX', 'TMUX_PANE')}
        env.update(NEEDS=json.dumps(results), MRSM_ROOT_DIR=str(tmp_path / 'root'),
                   MRSM_PLUGINS_DIR=str(tmp_path / 'plugins'))
        result = subprocess.run([sys.executable, '-I', '-c', script], cwd=tmp_path, env=env,
                                capture_output=True, text=True, timeout=30)
        assert (result.returncode == 0) is ok, result.stdout + result.stderr


def _evaluate_actions(expression, values):
    """Evaluate the small boolean/JSON subset used by these guards, with no unknown names."""
    expression = expression.removeprefix('${{').removesuffix('}}').strip()

    def literal(match):
        token = match.group()
        if token.startswith(("'", '"')) or token == 'fromJSON':
            return token
        if token in {'true', 'false', 'always()', 'cancelled()'}:
            return repr(token in {'true', 'always()'})
        if token in values:
            return repr(values[token])
        if re.fullmatch(r'needs\.[\w-]+\.outputs\.[\w-]+', token):
            return repr('true')
        if re.fullmatch(r'needs\.[\w-]+\.result', token):
            return repr('success')
        raise AssertionError(f'Unspecified Actions context: {token}')

    expression = re.sub(r"'(?:[^']|'')*'|\"(?:\\.|[^\"])*\"|[A-Za-z_][A-Za-z0-9_.-]*(?:\(\))?", literal, expression)
    expression = expression.replace('&&', ' and ').replace('||', ' or ')
    expression = re.sub(r'!(?!=)', ' not ', expression).strip()
    tree = ast.parse(expression, mode='eval')
    assert all(isinstance(node, (
        ast.Expression, ast.BoolOp, ast.UnaryOp, ast.And, ast.Or, ast.Not, ast.Compare,
        ast.Eq, ast.NotEq, ast.Constant, ast.Call, ast.Name, ast.Load,
    )) for node in ast.walk(tree))
    assert all(node.id == 'fromJSON' for node in ast.walk(tree) if isinstance(node, ast.Name))
    return eval(compile(tree, '<workflow guard>', 'eval'), {'__builtins__': {}, 'fromJSON': json.loads})


@pytest.mark.parametrize('shard', ['core', 'postgres'])
@pytest.mark.parametrize('event,requested,enabled', [
    ('pull_request', False, False),
    ('workflow_dispatch', True, True),
    ('workflow_dispatch', False, False),
    ('schedule', False, True),
])
def test_console_is_required_for_full_release_and_nightly_only(
    pytest_workflow, shard, event, requested, enabled,
):
    release_python = _load('release.yaml')['jobs']['python']
    assert release_python['with']['profile'] == 'release'
    assert release_python['with']['full'] is True
    assert release_python['with']['run_console'] is True
    expression = pytest_workflow['jobs']['tests']['env']['MRSM_TEST_CONSOLE']
    actual = _evaluate_actions(expression, {
        'matrix.shard': shard, 'github.event_name': event, 'inputs.run_console': requested,
    })
    assert actual == ('1' if enabled and shard == 'core' else '0')


def test_fork_prs_cannot_run_any_self_hosted_job(rust, pytest_workflow):
    for workflow in (rust, pytest_workflow):
        assert 'pull_request_target' not in _triggers(workflow)
        guarded = 0
        for name, job in workflow['jobs'].items():
            runners = job.get('runs-on', [])
            runners = [runners] if isinstance(runners, str) else runners
            if 'self-hosted' not in runners:
                continue
            guarded += 1
            values = {
                'github.event_name': 'pull_request',
                'github.event.pull_request.head.repo.full_name': 'outside/fork',
                'github.repository': 'project/repo',
                'inputs.full': True, 'inputs.profile': 'ci', 'inputs.parity': True,
                'vars.MRSM_SKIP_PARITY': 'false',
            }
            assert _evaluate_actions(job['if'], values) is False, name
            values['github.event.pull_request.head.repo.full_name'] = 'project/repo'
            assert _evaluate_actions(job['if'], values) is True, name
        assert guarded


def test_hosted_rust_checks_are_linux_on_prs_and_cross_platform_when_full(rust):
    check = rust['jobs']['check']
    expression = check['strategy']['matrix']['os']
    assert _evaluate_actions(expression, {'needs.changes.outputs.full': 'false'}) == ['ubuntu-latest']
    assert set(_evaluate_actions(expression, {'needs.changes.outputs.full': 'true'})) == {
        'ubuntu-latest', 'windows-latest', 'macos-latest',
    }
    assert not check.get('continue-on-error', False), 'the selected hosted checks must pass'


def test_reusable_validation_never_substitutes_branch_head_for_tested_sha(rust, pytest_workflow):
    for workflow in (rust, pytest_workflow):
        assert 'workflow_call' in _triggers(workflow)
        for name, job in workflow['jobs'].items():
            for step in job.get('steps', []):
                if str(step.get('uses', '')).startswith('actions/checkout'):
                    assert step['with']['ref'] == '${{ github.sha }}', name


def test_rust_jobs_follow_the_changes_job(rust):
    jobs = rust['jobs']
    triggers = _triggers(rust)
    assert 'paths' not in triggers['push'] and 'paths' not in (triggers['pull_request'] or {}), (
        'the `changes` job decides, so a skipped job never leaves a required check pending'
    )
    assert 'tags' not in triggers['push'], 'tag promotion must not duplicate release validation builds'
    assert 'schedule' in triggers and triggers['workflow_call']['inputs']['full']['default'] is True
    gates = {
        'fmt': 'rust', 'clippy': 'rust', 'check': 'rust', 'test': 'rust_test', 'parity': 'parity',
        'frontcrate': 'frontcrate', 'package': 'package', 'duckdb-platforms': 'duckdb',
    }
    assert set(gates) == set(jobs) - {'changes', 'summary'}
    for name, flag in gates.items():
        job = jobs[name]
        assert 'changes' in job['needs'] if isinstance(job['needs'], list) else job['needs'] == 'changes', name
        assert f"needs.changes.outputs.{flag} == 'true'" in job['if'], name
    assert not any('scripts/rust/ci.sh build' in step.get('run', '')
                   for job in jobs.values() for step in job.get('steps', [])), (
        'wheels.yaml owns shipping binaries; Rust validation must not duplicate their build'
    )
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


def test_shared_rust_state_is_locked_and_pytest_shards_are_isolated(rust, pytest_workflow):
    ### A dispatched release run waits for these instead of racing a push or pull request run.
    for job, group in (('test', 'mrsm-rust-test'), ('parity', 'mrsm-rust-parity')):
        assert rust['jobs'][job]['concurrency'] == {'group': group, 'cancel-in-progress': False}, job
    tests = pytest_workflow['jobs']['tests']
    assert 'concurrency' not in tests, 'isolated service projects can run concurrently'
    assert tests['strategy']['max-parallel'] == 5
    run = next(step['run'] for step in tests['steps']
               if 'scripts/ci/services.py run ' in step.get('run', ''))
    assert '--shard "${{ matrix.shard }}"' in run
    assert '--workers "${{ matrix.workers }}"' in run
    assert '$GITHUB_RUN_ID-$GITHUB_RUN_ATTEMPT-${{ matrix.shard }}' in run
    validate = next(step for step in tests['steps']
                    if 'scripts/ci/services.py validate ' in step.get('run', ''))
    assert "matrix.shard == 'core'" in validate['if'], 'the full collection must be partitioned exactly once'


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
