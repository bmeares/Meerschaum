#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Generate random names for jobs (native: `meerschaum-jobs`, `names.rs`).
"""

from __future__ import annotations


def generate_random_name(separator: str = '-') -> str:
    """Return a random `adjective-noun` name."""
    from meerschaum.jobs import _native
    name = _native.core().jobs_new_daemon_name()
    return name.replace('-', separator) if separator != '-' else name


def get_new_daemon_name() -> str:
    """
    Return a random name which is not used by an existing job.
    """
    from meerschaum.jobs import _native
    return _native.core().jobs_new_daemon_name()
