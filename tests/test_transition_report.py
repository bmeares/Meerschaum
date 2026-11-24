"""A failed snapshot comparison and unexpected differences must be visible to release validation."""

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

TRANSITION = Path(__file__).resolve().parents[1] / 'scripts' / 'transition'


@pytest.mark.parametrize('code,output', [(2, ''), (0, ''), (0, 'DIFF a rowcount\ndiffs: 0 expected: 0')])
def test_incomplete_comparison_fails(tmp_path, monkeypatch, code, output):
    monkeypatch.syspath_prepend(str(TRANSITION))
    from common import Run
    from stage_v5 import snapshots, Findings
    run = Run(tmp_path)
    monkeypatch.setattr(run, 'py', lambda _, script, *args, **kw: subprocess.CompletedProcess(
        [], code, output, 'comparison failure',
    ) if script.name == 'compare.py' else subprocess.CompletedProcess([], 0, '', ''))
    findings = Findings(run)
    snapshots(run, findings, 'test')
    assert [item['status'] for item in findings.items] == ['FAIL']


@pytest.mark.parametrize('status,strict_status', [('DIFF', 1), ('EXPECTED', 0), ('PASS', 0)])
def test_strict_report_classifies_differences(tmp_path, status, strict_status):
    out = tmp_path / 'work' / 'run-test' / 'out'
    out.mkdir(parents=True)
    (out / 'findings.json').write_text(json.dumps([
        {'area': 'pipes', 'name': 'row values', 'status': status, 'detail': ''},
    ]))
    for flags, expected in [([], 0), (['--strict'], strict_status)]:
        result = subprocess.run(
            [sys.executable, TRANSITION / 'report.py', *flags, 'run-test'],
            env={**os.environ, 'MRSM_TRANSITION_DIR': str(tmp_path)},
            capture_output=True, text=True,
        )
        assert result.returncode == expected, result.stdout + result.stderr
