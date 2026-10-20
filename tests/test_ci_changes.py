"""
Which CI jobs a change needs (`scripts/ci/changes.py`, the `changes` job of rust.yaml and pytest.yaml).

The classifier is the one place that decides what a push runs, so the cases that matter are pinned
here: a docs-only change runs nothing heavy, a Python-only change skips Rust, parity follows only its
inputs, and anything unknown runs the safe default.
"""

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
SCRIPT = REPO / 'scripts' / 'ci' / 'changes.py'


@pytest.fixture(scope='module')
def changes():
    spec = importlib.util.spec_from_file_location('mrsm_ci_changes', SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _on(changes, *files):
    return {name for name, value in changes.classify(list(files)).items() if value}


@pytest.mark.parametrize('files', [
    ('docs/zensical/reference/migrating-to-v5.md',),
    ('docs/design/v5-build.md', 'CHANGELOG.md', 'README.md', 'CLAUDE.md'),
    ('.claude/skills/release/SKILL.md', '.agents/skills/x/SKILL.md'),
    ('scripts/bench/results/2026-10-19-x.json',),
    ('LICENSE', 'NOTICE', '.gitignore'),
    (),
])
def test_docs_only_changes_run_nothing(changes, files):
    assert _on(changes, *files) == set()


def test_a_python_only_change_skips_rust(changes):
    assert _on(changes, 'meerschaum/core/Pipe/_sync.py', 'tests/test_sync.py') == {'pytest'}
    assert _on(changes, 'meerschaum/plugins/_page.py') == {'pytest'}


def test_python_the_cargo_tests_read_runs_the_rust_tests(changes):
    # crates/meerschaum-worker tests import `meerschaum`; doctor and execute_policy read two files as text.
    for path in ('meerschaum/worker/__init__.py', 'meerschaum/api/dash/execute.py',
                 'meerschaum/utils/packages/_packages.py', 'meerschaum/_core/__init__.py'):
        on = _on(changes, path)
        assert 'rust_test' in on and 'pytest' in on, path
        assert 'rust' not in on, f'{path}: nothing to format or lint'


def test_a_rust_change_runs_rust_and_the_python_suite_but_not_parity(changes):
    on = _on(changes, 'crates/meerschaum-kernel/src/enforce.rs')
    assert {'rust', 'rust_test', 'pytest'} <= on
    assert on.isdisjoint({'parity', 'frontcrate', 'package', 'duckdb'}), on


def test_parity_follows_the_generators_and_fixtures_only(changes):
    # v4.1.4 produces the fixtures: the Rust sources cannot change the result.
    assert 'parity' not in _on(changes, 'crates/meerschaum-core/src/lib.rs')
    assert 'parity' not in _on(changes, 'crates/meerschaum-connector-sql/src/lib.rs')
    for path in ('crates/meerschaum-core/tests/fixtures/gen_dtypes.py',
                 'crates/meerschaum-core/tests/fixtures/dtypes.json',
                 'crates/meerschaum-worker/tests/plugins/wk_custom/wk_custom.py',
                 'scripts/parity/requirements.txt', 'scripts/parity/run.sh'):
        assert 'parity' in _on(changes, path), path


def test_frontcrate_package_and_duckdb_follow_their_inputs(changes):
    assert 'frontcrate' in _on(changes, 'crates/meerschaum/src/lib.rs')
    assert 'frontcrate' in _on(changes, 'crates/meerschaum-server/Cargo.toml')
    assert 'frontcrate' not in _on(changes, 'crates/meerschaum-server/src/lib.rs')
    assert 'package' in _on(changes, 'crates/meerschaum-core/Cargo.toml')
    assert 'package' in _on(changes, 'crates/meerschaum-core/README.md')
    assert 'package' not in _on(changes, 'crates/meerschaum-core/src/lib.rs')
    assert 'duckdb' in _on(changes, 'crates/meerschaum-connector-sql/src/driver/duckdb/mod.rs')
    assert 'duckdb' not in _on(changes, 'crates/meerschaum-kernel/src/lib.rs')


def test_the_workflows_and_ci_scripts_are_inputs(changes):
    assert {'rust', 'rust_test', 'pytest'} <= _on(changes, 'scripts/rust/ci.sh')
    assert {'rust', 'rust_test', 'pytest'} <= _on(changes, 'scripts/ci/changes.py')
    assert 'pytest' in _on(changes, '.github/workflows/wheels.yaml')  # tests/test_workflows_packaging.py
    assert 'rust' not in _on(changes, '.github/workflows/wheels.yaml')
    assert {'rust', 'parity'} <= _on(changes, '.github/workflows/rust.yaml')


def test_an_unknown_file_runs_the_safe_default(changes):
    assert {'rust', 'rust_test', 'pytest'} <= _on(changes, 'some/new/place/file.bin')


def test_a_leading_dot_slash_and_dotfiles_survive(changes):
    assert 'pytest' in _on(changes, './.github/workflows/pytest.yaml')


def _run(*args, stdin=''):
    result = subprocess.run(
        [sys.executable, '-I', str(SCRIPT), *args], input=stdin, capture_output=True, text=True,
        cwd=REPO, timeout=60,
    )
    assert result.returncode == 0, result.stderr
    return dict(line.split('=') for line in result.stdout.splitlines())


@pytest.mark.parametrize('args', [
    ('--event', 'workflow_dispatch'),
    ('--event', 'push', '--ref', 'refs/tags/v5.0.0rc3'),
    ('--event', 'schedule'),
])
def test_a_dispatch_a_tag_or_a_schedule_runs_everything(args):
    flags = _run(*args)
    assert set(flags.values()) == {'true'}, flags
    assert flags['full'] == 'true'


def test_an_unknown_base_runs_everything():
    flags = _run('--event', 'push', '--before', '0' * 40, '--after', 'HEAD')
    assert set(flags.values()) == {'true'}, flags


def test_the_command_line_prints_the_flags_for_a_file_list():
    flags = _run('--event', 'push', '--files', '-', stdin='docs/a.md\nmeerschaum/x.py\n')
    assert flags['pytest'] == 'true' and flags['rust'] == 'false' and flags['full'] == 'false'
