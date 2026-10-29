"""Image budgets compare exact byte counts even when rounded display values are equal."""

import os
from pathlib import Path
import subprocess

import pytest


@pytest.mark.parametrize('size,expected', [
    (249999999, 0), (250000000, 0), (250000001, 1), (250040000, 1),
])
def test_image_size_budget_uses_bytes(tmp_path, size, expected):
    docker = tmp_path / 'docker'
    docker.write_text('#!/bin/sh\nprintf "%s\\n" "$MRSM_SIZE_TEST_BYTES"\n')
    docker.chmod(0o755)
    script = Path(__file__).resolve().parents[1] / 'scripts/docker/size_gate.sh'
    result = subprocess.run(
        ['bash', str(script), '--slim-image', 'size-test', '--full-image', ''],
        env={**os.environ, 'PATH': str(tmp_path) + os.pathsep + os.environ['PATH'],
             'MRSM_SIZE_TEST_BYTES': str(size)},
        capture_output=True, text=True, timeout=10,
    )
    assert result.returncode == expected, result.stdout + result.stderr
