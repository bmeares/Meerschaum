"""Catch stale native dependency metadata before compiling the release artifacts."""
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import subprocess
import sys

import pytest

REPO = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize('existing', [None, '// stale dependency data\n'])
def test_packaging_rejects_and_repairs_stale_dependency_groups(tmp_path, monkeypatch, capsys, existing):
    spec = spec_from_file_location('check_packaging_generated', REPO / 'scripts/check_packaging.py')
    checker = module_from_spec(spec)
    spec.loader.exec_module(checker)
    target = tmp_path / 'packages_table.rs'
    if existing is not None:
        target.write_text(existing)
    monkeypatch.setattr(checker, 'PACKAGES_TABLE', target)
    monkeypatch.setattr(sys, 'argv', ['check_packaging.py'])
    assert checker.main() == 1
    assert 'packages_table.rs differs' in capsys.readouterr().err
    monkeypatch.setattr(sys, 'argv', ['check_packaging.py', '--write'])
    assert checker.main() == 0
    expected = subprocess.check_output(
        [sys.executable, '-I', str(REPO / 'scripts/rust/gen_packages_table.py')], text=True,
    )
    assert target.read_text() == expected
    monkeypatch.setattr(sys, 'argv', ['check_packaging.py'])
    assert checker.main() == 0
