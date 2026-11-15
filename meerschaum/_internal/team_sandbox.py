#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The start of an isolated team's worker under its filesystem sandbox (5.1).

A worker the API server starts for an isolated team (and every worker a team job starts) has
`MRSM_TEAM_SANDBOX` in its environment: the rules the server wrote for the team. Before anything
of the `meerschaum` package, a plugin or user code is imported, the worker's host program loads
this file by its path (not through the package) and calls `boot()`, which loads the extension
directly and asks it to restrict this process (`_core.team_sandbox_enforce`) to those rules plus
the interpreter's own prefixes and import path. The report (enforced or not, the Landlock ABI and
features, the canary's result) is kept on `sys._mrsm_team_sandbox` and sent in the worker's
handshake, where the Rust host checks it.

Only the standard library may be imported here, and nothing at module level.
"""

SPEC_VAR = 'MRSM_TEAM_SANDBOX'


def _spec_mode(path):
    """The mode of the spec at `path` (`require` when it cannot be read: a sandbox was asked)."""
    import json
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f).get('mode', 'require')
    except Exception:
        return 'require'


def _not_enforced(path, reason):
    return {'mode': _spec_mode(path), 'enforced': False, 'reason': reason}


def _load_native(package_dir):
    """The extension module, loaded from `package_dir/_core` without importing the package."""
    import importlib.machinery
    import importlib.util
    import os
    import sys
    name = 'meerschaum._core._native'
    if name in sys.modules:
        return sys.modules[name]
    core_dir = os.path.join(package_dir, '_core')
    for suffix in importlib.machinery.EXTENSION_SUFFIXES:
        path = os.path.join(core_dir, '_native' + suffix)
        if os.path.exists(path):
            break
    else:
        return None
    loader = importlib.machinery.ExtensionFileLoader(name, path)
    spec = importlib.util.spec_from_file_location(name, path, loader=loader)
    module = importlib.util.module_from_spec(spec)
    loader.exec_module(module)
    sys.modules[name] = module
    return module


def interpreter_paths(package_dir=None):
    """The paths this interpreter reads and executes: its prefixes and import path."""
    import os
    import sys
    paths = {sys.prefix, sys.base_prefix, sys.exec_prefix, sys.base_exec_prefix}
    paths.update(path for path in sys.path if path and os.path.isabs(path))
    if sys.executable:
        paths.add(os.path.dirname(os.path.realpath(sys.executable)))
    if package_dir:
        paths.add(package_dir)
        ### The top-level launcher module next to the package.
        launcher = os.path.join(os.path.dirname(package_dir), '_meerschaum_mrsm.py')
        if os.path.exists(launcher):
            paths.add(launcher)
    return sorted(path for path in paths if path)


def boot(socket=None):
    """
    Restrict this process to its team's sandbox (once), before any plugin or user code runs.
    Returns the report (a dict), or `None` outside a sandboxed team process.
    """
    import json
    import os
    import sys
    existing = getattr(sys, '_mrsm_team_sandbox', None)
    if existing is not None:
        return existing
    spec_path = os.environ.get(SPEC_VAR, '')
    if not spec_path:
        return None
    try:
        import importlib.util
        spec = importlib.util.find_spec('meerschaum')
        package_dir = os.path.dirname(spec.origin) if spec and spec.origin else None
        native = _load_native(package_dir) if package_dir else None
        enforce = getattr(native, 'team_sandbox_enforce', None)
        if enforce is None:
            report = _not_enforced(
                spec_path,
                "this interpreter's `meerschaum` package has no team sandbox "
                "(install the release of the server)",
            )
        else:
            text = enforce(interpreter_paths(package_dir), socket)
            report = json.loads(text) if text else _not_enforced(spec_path, 'no report')
    except Exception as e:
        report = _not_enforced(spec_path, f"the sandbox could not be applied: {e}")
    sys._mrsm_team_sandbox = report
    return report


def report():
    """This process's sandbox report (`None` when it was not started under one)."""
    import sys
    return getattr(sys, '_mrsm_team_sandbox', None)
