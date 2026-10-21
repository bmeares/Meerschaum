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
    assert len(commands) == 3
    for command in commands:
        assert ' --load ' in command and '--push' not in command
        assert _platform(command) is None, 'a loaded image cannot be a manifest list'
    assert [_tags(c) for c in commands] == [
        [f'{IMAGE}:5.0.0rc2-minimal'],
        [f'{IMAGE}:5.0.0rc2-full'],
        [f'{IMAGE}:5.0.0rc2-slim'],
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
            [f'{IMAGE}:{version}-{name}-{arch}'] for name in ('minimal', 'full', 'slim')
        ], 'a release must not move latest/full/slim/api from one architecture'
        assert {_platform(c) for c in commands} == {platform}
        assert all(' --push ' in c for c in commands)


def test_the_merge_of_a_prerelease_makes_only_version_tags(tmp_path):
    result, commands = _buildx(tmp_path, '5.0.0rc2', '--merge')
    assert result.returncode == 0, result.stderr
    assert commands == [
        f'docker buildx imagetools create -t {IMAGE}:5.0.0rc2-{name} '
        f'{IMAGE}:5.0.0rc2-{name}-amd64'
        for name in ('minimal', 'full', 'slim')
    ]


def test_the_merge_of_a_release_moves_the_floating_tags(tmp_path):
    result, commands = _buildx(tmp_path, '5.0.0', '--merge')
    assert result.returncode == 0, result.stderr
    by_name = {c.split('-t ')[1].split(' ')[0].rsplit('-', 1)[-1]: c for c in commands}
    assert set(by_name) == {'minimal', 'full', 'slim'}
    assert _tags(by_name['full']) == [
        f'{IMAGE}:5.0.0-full', f'{IMAGE}:full', f'{IMAGE}:latest', f'{IMAGE}:5.0.0',
        f'{IMAGE}:api', f'{IMAGE}:5.0.0-api',
    ]
    assert _tags(by_name['minimal']) == [f'{IMAGE}:5.0.0-minimal', f'{IMAGE}:minimal']
    assert _tags(by_name['slim']) == [f'{IMAGE}:5.0.0-slim', f'{IMAGE}:slim']
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
