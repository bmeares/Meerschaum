def _mrsm_preflight():
    """
    Run by the Rust host after `_mrsm_guard()` (`meerschaum-core/src/python_guard.py`) and before
    it starts a worker: answers in milliseconds whether this interpreter can host a worker, so a
    missing or stale `meerschaum` is one clear message instead of a worker that never connects.

    The guard refuses (exit 113, message on stderr) another major version, a copy that looks like
    v4 and another worker protocol, reading files only. This adds the case the guard lets through
    (no `meerschaum` at all). On success one JSON line on stdout names the package found.
    """
    import json
    import os
    import re
    import sys

    _mrsm_guard()
    try:
        import importlib.util
        spec = importlib.util.find_spec('meerschaum')
    except Exception:
        spec = None
    origin = getattr(spec, 'origin', None) if spec is not None else None
    python = sys.executable or 'python3'
    expected = os.environ.get('MRSM_WORKER_EXPECT_VERSION') or ''
    if not origin or origin == 'namespace':
        v = sys.version_info
        print(
            f"The Python interpreter `{python}` (Python {v[0]}.{v[1]}.{v[2]}) cannot import the "
            f"`meerschaum` package, which this program ({expected or 'this release'}) starts for "
            f"plugins, `plugin:` connectors and Python actions. Install it into that interpreter "
            f"(`{python} -m pip install 'meerschaum=={expected}'`) or set MRSM_PYTHON to an "
            "interpreter that has it. Then run `mrsm reload` (or restart this program); "
            "`mrsm doctor` shows the details.",
            file=sys.stderr,
        )
        sys.exit(113)
    directory = os.path.dirname(origin)
    version = None
    try:
        with open(os.path.join(directory, 'config', '_version.py')) as f:
            m = re.search(r"__version__\s*=\s*['\"]([^'\"]+)", f.read())
        version = m.group(1) if m else None
    except Exception:
        pass
    print(json.dumps({'version': version, 'directory': directory}))
