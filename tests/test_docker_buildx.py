"""
`scripts/docker/buildx.sh` plans (W18PK): the commands it would run for a local build, for a
per-architecture push from a self-hosted runner, and for the merge into the multi-architecture tags.

`BUILDX_DRY_RUN=1` makes the script print its docker commands instead of running them (no Docker,
no network, nothing pulled or pushed); `PYTHON_BIN` points at a stub that prints the version, so a
final release (which moves `latest`, `full`, `slim`, `api`) is tested as well as a prerelease.
"""

import os
import re
import stat
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / 'scripts' / 'docker' / 'buildx.sh'
IMAGE = 'bmeares/meerschaum'


def _buildx(tmp_path, version, *args):
    python = tmp_path / 'python'
    python.write_text(f'#!/bin/sh\necho {version}\n')
    python.chmod(python.stat().st_mode | stat.S_IEXEC)
    env = {k: v for k, v in os.environ.items() if not k.startswith(('MRSM_', 'GITHUB_'))}
    env.update({'BUILDX_DRY_RUN': '1', 'PYTHON_BIN': str(python)})
    result = subprocess.run(
        ['bash', str(SCRIPT), *args], env=env, capture_output=True, text=True, timeout=60,
    )
    commands = [
        line[2:] for line in result.stdout.splitlines() if line.startswith('+ docker ')
    ]
    return result, commands


def _tags(command):
    return re.findall(r'(?:^| )-t (\S+)', command)


def _platform(command):
    match = re.search(r'--platform (\S+)', command)
    return match.group(1) if match else None


def test_a_local_build_loads_for_the_builders_platform(tmp_path):
    result, commands = _buildx(tmp_path, '5.0.0rc2')
    assert result.returncode == 0, result.stderr
    assert len(commands) == 5
    for command in commands:
        assert ' --load ' in command and '--push' not in command
        assert _platform(command) is None, 'a loaded image cannot be a manifest list'
    assert [_tags(c) for c in commands] == [
        [f'{IMAGE}:5.0.0rc2-minimal'],
        [f'{IMAGE}:5.0.0rc2-full', f'{IMAGE}:5.0.0rc2-api'],
        [f'{IMAGE}:5.0.0rc2-slim'],
        [f'{IMAGE}:5.0.0rc2-rust'],
        [f'{IMAGE}:5.0.0rc2-rust-full'],
    ]


@pytest.mark.parametrize('version,platforms', [
    ('5.0.0rc4', 'linux/amd64'),
    ('5.0.0a1', 'linux/amd64'),
    ('5.0.0b1', 'linux/amd64'),
    ('5.0.0.dev1', 'linux/amd64'),
    ('5.0.0', 'linux/amd64,linux/arm64'),
])
def test_a_push_selects_the_release_platforms(tmp_path, version, platforms):
    result, commands = _buildx(tmp_path, version, '--push')
    assert result.returncode == 0, result.stderr
    assert {_platform(c) for c in commands} == {platforms}


@pytest.mark.parametrize('platform,arch', [('linux/arm64', 'arm64'), ('linux/amd64', 'amd64')])
def test_a_per_architecture_push_pushes_only_the_architecture_tags(tmp_path, platform, arch):
    for version in ('5.0.0rc2', '5.0.0'):
        result, commands = _buildx(
            tmp_path, version, '--push', '--platform', platform, '--arch-tags',
        )
        assert result.returncode == 0, result.stderr
        assert [_tags(c) for c in commands] == [
            [f'{IMAGE}:{version}-{name}-{arch}'] for name in ('minimal', 'full', 'slim', 'rust', 'rust-full')
        ], 'a release must not move latest/full/slim/api from one architecture'
        assert {_platform(c) for c in commands} == {platform}
        assert all(' --push ' in c for c in commands)


def test_the_merge_of_a_prerelease_makes_only_version_tags(tmp_path):
    result, commands = _buildx(tmp_path, '5.0.0rc2', '--merge')
    assert result.returncode == 0, result.stderr
    assert commands == [
        f'docker buildx imagetools create -t {IMAGE}:5.0.0rc2-{name} '
        + (f'-t {IMAGE}:5.0.0rc2-api ' if name == 'full' else '')
        + f'{IMAGE}:5.0.0rc2-{name}-amd64'
        for name in ('minimal', 'full', 'slim', 'rust', 'rust-full')
    ]


def test_the_merge_of_a_release_moves_the_floating_tags(tmp_path):
    result, commands = _buildx(tmp_path, '5.0.0', '--merge')
    assert result.returncode == 0, result.stderr
    by_name = {c.split('-t ')[1].split(' ')[0].split(':5.0.0-', 1)[1]: c for c in commands}
    assert set(by_name) == {'minimal', 'full', 'slim', 'rust', 'rust-full'}
    assert _tags(by_name['full']) == [
        f'{IMAGE}:5.0.0-full', f'{IMAGE}:5.0.0-api', f'{IMAGE}:full', f'{IMAGE}:latest',
        f'{IMAGE}:5.0.0', f'{IMAGE}:api',
    ]
    assert _tags(by_name['minimal']) == [f'{IMAGE}:5.0.0-minimal', f'{IMAGE}:minimal']
    assert _tags(by_name['slim']) == [f'{IMAGE}:5.0.0-slim', f'{IMAGE}:slim']
    for variant in ('rust', 'rust-full'):
        assert _tags(by_name[variant]) == [f'{IMAGE}:5.0.0-{variant}', f'{IMAGE}:{variant}']
    for command in commands:
        assert command.split(' ')[-2].endswith('-amd64') and command.split(' ')[-1].endswith('-arm64')


@pytest.mark.parametrize('args', [
    ('--arch-tags',),
    ('--arch-tags', '--push'),
    ('--arch-tags', '--push', '--platform', 'linux/amd64,linux/arm64'),
    ('--arch-tags', '--platform', 'linux/arm64'),
    ('--merge', '--push'),
    ('--merge', '--platform', 'linux/arm64'),
    ('--bogus',),
])
def test_inconsistent_options_are_refused_before_any_command(tmp_path, args):
    result, commands = _buildx(tmp_path, '5.0.0rc2', *args)
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert commands == []
    assert 'buildx.sh:' in result.stderr


def test_a_v4_version_is_never_pushed(tmp_path):
    for args in (('--push',), ('--merge',)):
        result, commands = _buildx(tmp_path, '4.1.7', *args)
        assert result.returncode == 1 and commands == []
        assert 'refusing to push' in result.stderr


def test_the_final_release_platforms_are_amd64_and_arm64():
    text = (REPO / 'scripts' / 'config.sh').read_text()
    assert 'export platforms="linux/amd64,linux/arm64"' in text


def test_a_sealed_sdk_context_replaces_source_compilation(tmp_path):
    sdk = tmp_path / 'sdk'
    (sdk / 'wheels').mkdir(parents=True)
    (sdk / 'mrsm').write_bytes(b'verified-program')
    result, commands = _buildx(tmp_path, '5.0.0rc25', '--sdk-artifacts', str(sdk))
    assert result.returncode == 0, result.stderr
    assert len(commands) == 5
    assert all(f'--build-context sdk={sdk}' in c and '--build-arg sdk_source=artifacts' in c for c in commands)
    assert all('--label org.opencontainers.image.revision=' in c for c in commands)


def _dockerfile_stages() -> dict[str, str]:
    text = (REPO / 'Dockerfile').read_text(encoding='utf-8')
    stages: dict[str, list[str]] = {}
    current = None
    for line in text.splitlines():
        match = re.match(r'^FROM\s+\S+\s+AS\s+(\S+)\s*$', line)
        if match:
            current = match.group(1)
            stages[current] = [line]
        elif current is not None:
            stages[current].append(line)
    return {name: '\n'.join(lines) for name, lines in stages.items()}


def test_dockerfile_separates_stable_and_volatile_layers_for_caching():
    stages = _dockerfile_stages()

    # 1. Confined compiler snapshot must not depend on root Cargo.toml version bumps.
    assert 'COPY Cargo.toml' not in stages['confined-compiler']
    assert '--manifest ' not in stages['confined-compiler']
    assert '--snapshot-only' in stages['confined-compiler']
    assert '--policy-only' in stages['compiler-policy']

    # 2. Third-party Python extras must not depend on the volatile Meerschaum wheel.
    assert 'wheel-selected' not in stages['py-full-deps']
    assert 'requirements/full.txt' in stages['py-full-deps']
    assert '--no-deps' in stages['py-slim']

    # 3. Rust OS rootfs stages must not copy `mrsm` before installing build tools.
    assert 'native-selected' not in stages['rust-os-rootfs']
    assert 'native-selected' not in stages['rust-full-os-rootfs']

    # 4. Final `FROM scratch` stages layer stable assets before volatile release assets
    #    so Docker Hub deduplicates unchanged layers across releases and variants.
    def copy_sources(stage: str) -> list[str]:
        return re.findall(r'^COPY\b[^\n]*--from=(\S+)', stages[stage], flags=re.M)

    full_sources = copy_sources('full')
    rust_full_sources = copy_sources('rust-full')
    rust_sources = copy_sources('rust')
    slim_sources = copy_sources('slim')

    assert full_sources.index('py-full-deps') < full_sources.index('py-slim')
    assert full_sources.index('confined-compiler') < full_sources.index('native-selected')
    assert full_sources.index('native-selected') < full_sources.index('compiler-policy')
    assert rust_full_sources.index('confined-compiler') < rust_full_sources.index('native-selected')
    assert rust_sources.index('rust-os-rootfs') < rust_sources.index('native-selected')
    assert slim_sources.index('slim-os-rootfs') < slim_sources.index('py-slim')

    # Shared layers between variants must use identical COPY lines for registry deduplication.
    for shared_copy in (
        'COPY --from=confined-compiler /confined-compiler/rootfs/usr/local/lib/meerschaum/compiler/ /usr/local/lib/meerschaum/compiler/',
        'COPY --from=confined-compiler /confined-compiler/compat/ /',
        'COPY --from=rust-toolchain /usr/local/cargo/bin/ /usr/local/cargo/bin/',
        'COPY --from=native-selected /out/mrsm /usr/local/bin/mrsm',
        'COPY --from=compiler-policy /compiler-policy/etc/meerschaum/ /etc/meerschaum/',
    ):
        assert shared_copy in stages['full']
        assert shared_copy in stages['rust-full']
    assert 'COPY --from=native-selected /out/mrsm /usr/local/bin/mrsm' in stages['rust']
    assert 'COPY --from=py-slim /install/ /usr/local/' in stages['slim']
    assert 'COPY --from=py-slim /install/ /usr/local/' in stages['full']
