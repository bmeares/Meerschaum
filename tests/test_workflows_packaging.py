"""
Wave 18 (PK): the packaging workflows' structure and the wheel tag guard.

`.github/workflows/wheels.yaml` builds the musllinux and macOS x86_64 wheels and `docker-build.yaml`
builds both Docker architectures natively; nothing here runs a workflow. The tests parse the YAML
(with the extension's native loader, no PyYAML needed) and run the wheel tag/size guard script that
`wheels.yaml` embeds against fabricated wheels, so a regex mistake shows up before a CI run.
"""

import os
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
WORKFLOWS = REPO / '.github' / 'workflows'


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
    include = wheels['jobs']['build']['strategy']['matrix']['include']
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
    smoke_os = set(jobs['smoke']['strategy']['matrix']['os'])
    assert smoke_os >= {'ubuntu-latest', 'ubuntu-24.04-arm', 'macos-latest', 'macos-15-intel',
                        'windows-latest'}
    musl = jobs['smoke-musl']
    assert musl['needs'] == 'build'
    runs = {(e['os'], e['arch']) for e in musl['strategy']['matrix']['include']}
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
    include = wheels['jobs']['binaries']['strategy']['matrix']['include']
    targets = {e['target']: e['os'] for e in include}
    assert targets['x86_64-apple-darwin'] == 'macos-15-intel'
    assert set(targets) == {
        'x86_64-unknown-linux-gnu', 'aarch64-unknown-linux-gnu', 'aarch64-apple-darwin',
        'x86_64-apple-darwin', 'x86_64-pc-windows-msvc',
    }


def test_docker_images_build_natively_per_architecture(docker_build):
    job = docker_build['jobs']['buildx']
    include = job['strategy']['matrix']['include']
    assert {(e['arch'], e['runner'], e['platform']) for e in include} == {
        ('amd64', 'ubuntu-latest', 'linux/amd64'),
        ('arm64', 'ubuntu-24.04-arm', 'linux/arm64'),
    }
    assert not any('qemu' in str(s.get('uses', '')) for s in job['steps']), 'no QEMU: too slow'
    push = next(s for s in job['steps'] if 'arch-tags' in str(s.get('run', '')))
    assert push['if'] == 'inputs.publish'
    assert '--push --arch-tags --platform ${{ matrix.platform }}' in push['run']

    publish = docker_build['jobs']['publish']
    assert publish['needs'] == 'buildx', 'merge only after both architectures pushed'
    assert 'inputs.publish' in publish['if']
    runs = '\n'.join(str(s.get('run', '')) for s in publish['steps'])
    assert 'buildx.sh --merge' in runs
    assert 'linux/amd64' in runs and 'linux/arm64' in runs, 'the manifest list must hold both'


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
