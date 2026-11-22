#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Shared pytest fixtures for `tests/api_host/`:
- `api_host_deps_ready` (`scope='session'`): probes the host/console dependency set ONCE per
  pytest session instead of once per test module.
- `sandbox` (`scope='module'`): creates isolated `(root, plugins)` directories per module using
  the cached session probe.
"""

from __future__ import annotations

import pathlib
import shutil
import tempfile

import pytest

from tests.api_host._helpers import ensure_api_host_deps


@pytest.fixture(scope='session')
def api_host_deps_ready():
    """Verify the API host + Dash console dependencies once per pytest session."""
    return ensure_api_host_deps()


@pytest.fixture(scope='module')
def sandbox(api_host_deps_ready):
    """A temporary root and plugins directory (depends on the session-scoped dependency probe)."""
    scratch = pathlib.Path.home() / '.cache' / 'mrsm-api-host-tests'
    scratch.mkdir(parents=True, exist_ok=True)
    root = pathlib.Path(tempfile.mkdtemp(prefix='mrsm-apihost-', dir=scratch))
    plugins = pathlib.Path(tempfile.mkdtemp(prefix='mrsm-apihost-plugins-', dir=scratch))
    try:
        yield root, plugins
    finally:
        shutil.rmtree(root, ignore_errors=True)
        shutil.rmtree(plugins, ignore_errors=True)
