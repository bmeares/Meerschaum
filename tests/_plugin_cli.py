#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Run the `mrsm` CLI (through the extension, `python -m meerschaum`) against synthetic plugins on a
throwaway root: what the plugin-parity tests drive.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional

REPO = Path(__file__).resolve().parent.parent


def run_mrsm(
    tmp_path: Path,
    plugins: Dict[str, str],
    args: List[str],
    extra_env: Optional[Dict[str, str]] = None,
    timeout: int = 180,
) -> subprocess.CompletedProcess:
    """
    Write `plugins` (file name -> source) into a fresh plugins directory under `tmp_path` and run
    `mrsm <args>` there. A private SQLite instance is the default so that nothing reaches a
    developer's `sql:main`.
    """
    plugins_dir = tmp_path / 'plugins'
    root_dir = tmp_path / 'root'
    plugins_dir.mkdir(exist_ok=True)
    root_dir.mkdir(exist_ok=True)
    for name, source in plugins.items():
        (plugins_dir / name).write_text(source, encoding='utf-8')
    env = {
        **os.environ,
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(root_dir),
        'MRSM_PLUGINS_DIR': str(plugins_dir),
        'MRSM_PYTHON': sys.executable,
        'MRSM_SQL_PLUGINCLI': f"sqlite:///{tmp_path / 'plugincli.db'}",
        'MRSM_CONFIG': '{"meerschaum": {"instance": "sql:plugincli"}}',
        **(extra_env or {}),
    }
    return subprocess.run(
        [sys.executable, '-m', 'meerschaum', *args],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(tmp_path),
        timeout=timeout,
    )


def marked_line(result: subprocess.CompletedProcess, marker: str) -> str:
    """The first output line that starts with `marker`."""
    for line in (result.stdout + result.stderr).splitlines():
        if line.startswith(marker):
            return line
    raise AssertionError(
        f"No '{marker}' line (exit {result.returncode}):\n{result.stdout}\n{result.stderr}"
    )
