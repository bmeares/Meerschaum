#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Verify Rust-produced password hashes with the Python `verify_password`.

Reads `[{"password": ..., "hash": ...}, ...]` as JSON on stdin and prints a JSON list of booleans.
Run with `PYTHONPATH=<repo root>` (see `tests/users_parity.rs`).
"""

import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
import _stubs  # noqa: E402

_stubs.isolate()

from meerschaum.core.User._User import verify_password  # noqa: E402

if __name__ == '__main__':
    cases = json.load(sys.stdin)
    print(json.dumps([verify_password(c['password'], c['hash']) for c in cases]))
