"""`ci.sh setup` keeps a runner's rustup cargo on the PATH of later workflow steps."""

import os
import pathlib
import shutil
import subprocess

import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]
CI = REPO / 'scripts' / 'rust' / 'ci.sh'
BASH = shutil.which('bash')

pytestmark = pytest.mark.skipif(
    os.name == 'nt' or BASH is None, reason='ci.sh runs under bash on Unix runners',
)

FAKE_CARGO = 'cargo 0.0.0 (preinstalled fake)'
FAKES = {
    'cargo': f'echo "{FAKE_CARGO}"',
    'rustc': 'echo "rustc 0.0.0 (preinstalled fake)"',
    'rustup': 'case "$1" in toolchain) exit 0 ;; show) echo "fake-toolchain" ;; *) exit 64 ;; esac',
}


def _fake_toolchain(directory):
    directory.mkdir(parents=True)
    for name, body in FAKES.items():
        path = directory / name
        path.write_text('#!/bin/sh\n' + body + '\n')
        path.chmod(0o755)


@pytest.fixture
def runner(tmp_path):
    """
    A CARGO_HOME holding a preinstalled (fake) rustup toolchain, and a PATH with only the tools
    `ci.sh setup` itself runs: no real cargo, rustup or installer (`curl`, `sh`) is reachable.
    """
    cargo_home = tmp_path / 'cargo'
    _fake_toolchain(cargo_home / 'bin')
    tools = tmp_path / 'tools'
    tools.mkdir()
    for name in ('bash', 'dirname', 'uname'):
        (tools / name).symlink_to(shutil.which(name))
    env = {
        'HOME': os.environ.get('HOME', str(tmp_path)),
        'PATH': str(tools),
        'CARGO_HOME': str(cargo_home),
        'MRSM_CI_ACCEL': '0',
    }
    return cargo_home, tools, env


def _setup(env):
    return subprocess.run(
        [BASH, str(CI), 'setup'], env=env, capture_output=True, text=True, timeout=60,
    )


def _later_step(env, exported, base_path):
    """A fresh step's shell: GitHub prepends each exported directory to the runner's PATH."""
    path = os.pathsep.join([*reversed(exported), base_path])
    return subprocess.run(
        [BASH, '-c', 'cargo --version'], env={**env, 'PATH': path},
        capture_output=True, text=True, timeout=30,
    )


def test_a_preinstalled_cargo_reaches_later_steps(runner, tmp_path):
    cargo_home, tools, env = runner
    github_path = tmp_path / 'github_path'
    github_path.touch()
    result = _setup({**env, 'GITHUB_PATH': str(github_path)})
    assert result.returncode == 0, result.stdout + result.stderr
    assert 'Installing rustup' not in result.stdout
    exported = [line for line in github_path.read_text().splitlines() if line]
    assert exported == [str(cargo_home / 'bin')]
    later = _later_step(env, exported, str(tools))
    assert later.returncode == 0, later.stderr
    assert later.stdout.strip() == FAKE_CARGO


def test_a_cargo_outside_cargo_home_is_not_exported(runner, tmp_path):
    """A system cargo is already on the runner's PATH: exporting its directory would reorder it."""
    cargo_home, tools, env = runner
    shutil.rmtree(cargo_home / 'bin')
    (cargo_home / 'bin').mkdir()
    system = tmp_path / 'system'
    _fake_toolchain(system)
    github_path = tmp_path / 'github_path'
    github_path.touch()
    path = os.pathsep.join([str(system), str(tools)])
    result = _setup({**env, 'PATH': path, 'GITHUB_PATH': str(github_path)})
    assert result.returncode == 0, result.stdout + result.stderr
    assert github_path.read_text() == ''


def test_without_github_setup_only_checks_the_toolchain(runner):
    _, _, env = runner
    result = _setup(env)
    assert result.returncode == 0, result.stdout + result.stderr
    assert FAKE_CARGO in result.stdout
    assert 'Installing rustup' not in result.stdout
