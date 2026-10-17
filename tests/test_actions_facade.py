#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The action façade (wave 16, D14): `meerschaum.actions`, `meerschaum.entry()` and `python -m
meerschaum` delegate to the native dispatcher. There are no Python action bodies to test, only the
delegation (what reaches `_core`) and what an end user sees.

Run with:
    PYTHONPATH=$(pwd) MRSM_ROOT_DIR=./test_root MRSM_PLUGINS_DIR=./tests/plugins \\
        python -m pytest tests/test_actions_facade.py -p no:cacheprovider
"""

import os
import pathlib
import subprocess
import sys

import pytest

import meerschaum as mrsm
from meerschaum import _core

pytestmark = pytest.mark.skipif(
    _core.load() is None,
    reason="The `_core` extension is not built (scripts/rust/build_py.sh).",
)

REPO = pathlib.Path(__file__).resolve().parent.parent


def test_actions_are_the_native_registry():
    """`actions` holds a façade for every native action, with its docstring and subactions."""
    from meerschaum.actions import actions, get_subactions

    native = {name: (doc, subs) for name, doc, _, subs in _core.core_actions()}
    for name, (doc, subs) in native.items():
        function = actions[name]
        assert function.__name__ == name
        assert function.__doc__ == (doc or None)
        assert list(get_subactions(name)) == [sub[0] for sub in subs]
    ### v4's names are all there.
    assert {'sync', 'show', 'install', 'start', 'delete', 'register', 'compose'} <= set(actions)
    assert set(get_subactions('install')) == {'packages', 'plugins', 'required'}


def test_get_action_resolves_subactions_by_v4_function_names():
    from meerschaum.actions import get_action, get_main_action_name

    assert get_action(['show', 'pipes']).__name__ == '_show_pipes'
    assert get_action(['show', 'pipe']).__name__ == '_show_pipes'
    assert get_action(['sync']).__name__ == 'sync'
    assert get_action(['not-an-action']) is None
    assert get_main_action_name(['show', 'pipes']) == 'show'


def test_executor_keys_appear_in_the_signature_of_actions_that_accept_them():
    import inspect
    from meerschaum.actions import actions, get_subactions

    assert 'executor_keys' in inspect.signature(actions['attach']).parameters
    assert 'executor_keys' not in inspect.signature(actions['sync']).parameters
    assert 'executor_keys' in inspect.signature(get_subactions('start')['jobs']).parameters


def test_actions_delegate_to_core_run_action(monkeypatch):
    """`actions['sync'](['pipes'], **kw)` is `_core.run_action(['sync', 'pipes'], kw)`."""
    from meerschaum.actions import actions, get_subactions

    calls = []

    def fake_run_action(action, kwargs=None, context=None):
        calls.append((list(action), dict(kwargs or {})))
        return True, "fake"

    monkeypatch.setattr(_core, 'run_action', fake_run_action, raising=False)
    assert actions['sync'](['pipes'], connector_keys=['sql:x'], yes=True) == (True, "fake")
    assert get_subactions('show')['pipes'](['x'], debug=True) == (True, "fake")
    assert actions['show'](action=['version']) == (True, "fake")
    assert calls == [
        (['sync', 'pipes'], {'connector_keys': ['sql:x'], 'yes': True}),
        (['show', 'pipes', 'x'], {'debug': True}),
        (['show', 'version'], {}),
    ]


def test_entry_delegates_to_core_entry(monkeypatch):
    """`mrsm.entry()` splits a string like a shell and passes `_patch_args` through."""
    from meerschaum._internal.entry import entry, entry_without_daemon

    calls = []

    def fake_entry(sysargs, patch=None, context=None):
        calls.append((list(sysargs), patch))
        return True, "fake"

    monkeypatch.setattr(_core, 'entry', fake_entry, raising=False)
    assert entry(['sync', 'pipes', '-i', 'sql:x']) == (True, "fake")
    assert entry("show pipes -c 'sql:main'", _patch_args={'yes': True}) == (True, "fake")
    assert entry_without_daemon(None) == (True, "fake")
    assert calls == [
        (['sync', 'pipes', '-i', 'sql:x'], None),
        (['show', 'pipes', '-c', 'sql:main'], {'yes': True}),
        ([], None),
    ]
    assert mrsm.entry is entry


def test_entry_runs_natively_and_prints_through_python_streams(capsys):
    success, message = mrsm.entry(['show', 'version', '--nopretty'])
    assert success, message
    assert capsys.readouterr().out.strip() == _core.__version__


def test_entry_with_patch_args_reaches_the_action(capsys):
    """`_patch_args` override the parsed arguments, as the shell's `entry(..., _patch_args)`."""
    success, _ = mrsm.entry(['show', 'version'], _patch_args={'nopretty': True})
    assert success
    assert capsys.readouterr().out.strip() == _core.__version__


def test_actions_run_natively_with_keyword_arguments(capsys):
    from meerschaum.actions import actions

    success, message = actions['show'](['version'], nopretty=True)
    assert success, message
    assert capsys.readouterr().out.strip() == _core.__version__


def test_a_failed_native_action_returns_its_tuple():
    success, message = mrsm.entry(['delete', 'users', 'no_such_user_w16', '-y'])
    assert success is False
    assert isinstance(message, str) and message


def test_removed_action_modules_name_their_replacement():
    with pytest.raises(ModuleNotFoundError, match='removed in Meerschaum 5.0'):
        import meerschaum.actions.sync  # noqa: F401
    ### (Importing `meerschaum.api.routes` would first import `meerschaum.api`, which needs the
    ### `api` extra.)
    from meerschaum._internal._removed import removal_message
    assert 'native' in removal_message('meerschaum.api.routes._pipes')
    with pytest.raises(ModuleNotFoundError, match='native'):
        import meerschaum._internal.term  # noqa: F401
    with pytest.raises(ImportError, match='native shell'):
        import meerschaum._internal.shell.updates  # noqa: F401


def test_the_plugin_sdk_names_survive():
    from meerschaum.actions import (
        make_action, pre_sync_hook, post_sync_hook, choose_subaction, choices_docstring,
        get_completer, original_actions, modules, entry, get_shell,
    )
    import meerschaum.plugins as plugins

    assert make_action is plugins.make_action
    assert pre_sync_hook is plugins.pre_sync_hook
    assert post_sync_hook is plugins.post_sync_hook
    assert entry is mrsm.entry
    assert callable(choose_subaction) and callable(choices_docstring) and callable(get_shell)
    assert get_completer(['sync', 'pipes']) is None
    assert 'sync' in original_actions
    assert isinstance(modules, list)


def test_a_plugin_action_is_added_to_actions_and_wins_nothing_over_core():
    from meerschaum.actions import actions, get_action
    import meerschaum.plugins as plugins

    def w16_facade_action(action=None, **kw):
        return True, "plugin"

    w16_facade_action.__module__ = 'plugins.w16_facade'
    try:
        plugins.make_action(w16_facade_action, skip_if_loaded=False)
        assert get_action(['w16_facade_action']) is w16_facade_action
        assert actions['w16_facade_action'](['x']) == (True, "plugin")

        def sync(action=None, **kw):
            return True, "plugin"

        sync.__module__ = 'plugins.w16_facade'
        ### `skip_if_loaded=True` (what `load_plugins()` uses) keeps the core action.
        plugins.make_action(sync, skip_if_loaded=True)
        assert actions['sync'] is not sync
    finally:
        plugins.unload_custom_actions(['w16_facade'])


def test_python_dash_m_runs_the_native_binary(tmp_path):
    """`python -m meerschaum ...` is `mrsm ...`: same output, same exit code."""
    from meerschaum._internal.binary import find_binary

    binary = find_binary()
    if binary is None:
        pytest.skip("No `mrsm` binary is built (cargo build -p meerschaum-cli).")
    env = {
        **os.environ,
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(tmp_path / 'root'),
        'MRSM_PLUGINS_DIR': str(tmp_path / 'plugins'),
        'MRSM_BIN': binary,
    }
    (tmp_path / 'plugins').mkdir()
    (tmp_path / 'root').mkdir()
    via_python = subprocess.run(
        [sys.executable, '-m', 'meerschaum', 'show', 'version', '--nopretty'],
        capture_output=True, text=True, env=env, timeout=60,
    )
    direct = subprocess.run(
        [binary, 'show', 'version', '--nopretty'],
        capture_output=True, text=True, env=env, timeout=60,
    )
    assert via_python.returncode == direct.returncode == 0
    assert via_python.stdout == direct.stdout == _core.__version__ + '\n'

    failing = subprocess.run(
        [sys.executable, '-m', 'meerschaum', 'delete', 'users', 'no_such_user_w16', '-y'],
        capture_output=True, text=True, env=env, timeout=60,
    )
    assert failing.returncode == 1


def test_python_dash_m_names_the_missing_binary(tmp_path):
    """Without a binary the error says which package to install."""
    (tmp_path / 'root').mkdir()
    env = {
        **os.environ,
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(tmp_path / 'root'),
        'MRSM_BIN': str(tmp_path / 'missing'),
    }
    code = (
        "import sys\n"
        "import meerschaum._internal.binary as b\n"
        "b.find_binary = lambda: None\n"
        "from meerschaum.__main__ import main\n"
        "main(['show', 'version'])\n"
    )
    result = subprocess.run(
        [sys.executable, '-c', code], capture_output=True, text=True, env=env, timeout=60,
    )
    assert result.returncode == 127
    assert 'pip install meerschaum-cli' in result.stderr


def test_a_python_shim_named_mrsm_is_not_mistaken_for_the_binary(tmp_path, monkeypatch):
    """A console script that runs `python -m meerschaum` would start itself again."""
    from meerschaum._internal import binary

    shim = tmp_path / binary.BINARY_NAME
    shim.write_text('#!/usr/bin/env python\nimport sys\n')
    shim.chmod(0o755)
    monkeypatch.setenv(binary.BINARY_ENV, str(shim))
    assert binary.find_binary() != str(shim)


def test_the_shell_facade_runs_the_native_binary(capfd):
    """`get_shell(sysargs).cmdloop()` is `mrsm <sysargs>`, here a one-shot command."""
    from meerschaum._internal.binary import find_binary
    from meerschaum._internal.entry import get_shell

    if find_binary() is None:
        pytest.skip("No `mrsm` binary is built (cargo build -p meerschaum-cli).")
    get_shell(['show', 'version', '--nopretty'], reload=True).cmdloop()
    assert capfd.readouterr().out.strip() == _core.__version__


def test_adhoc_connectors_are_visible_to_native_actions(tmp_path, capsys):
    """A SQL connector this process defined (not in the config) resolves in `mrsm.entry()`."""
    conn = mrsm.get_connector(
        'sql', 'w16_adhoc', flavor='sqlite', database=str(tmp_path / 'adhoc.db'),
    )
    try:
        pipe = mrsm.Pipe('w16', 'adhoc', instance=conn, columns={'datetime': 'dt'})
        success, message = pipe.sync({'dt': ['2026-01-01'], 'v': [1]})
        assert success, message
        success, message = mrsm.entry(
            ['show', 'rowcounts', '-i', str(conn), '--nopretty', '-c', 'w16', '-m', 'adhoc'],
        )
        capsys.readouterr()
        assert success, message
        success, message = mrsm.entry(
            ['delete', 'pipes', '-i', str(conn), '-c', 'w16', '-m', 'adhoc', '-y'],
        )
        assert success, message
        assert pipe.get_rowcount() == 0
    finally:
        mrsm.connectors.connectors.get('sql', {}).pop('w16_adhoc', None)


def _run_python(code: str, tmp_path, timeout: int = 120, extra_env=None):
    """Run `code` in a fresh interpreter on a throwaway root (nothing shared with this process)."""
    (tmp_path / 'root').mkdir(exist_ok=True)
    (tmp_path / 'plugins').mkdir(exist_ok=True)
    env = {
        **os.environ,
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(tmp_path / 'root'),
        'MRSM_PLUGINS_DIR': str(tmp_path / 'plugins'),
        **(extra_env or {}),
    }
    return subprocess.run(
        [sys.executable, '-c', code], capture_output=True, text=True, env=env, timeout=timeout,
    )


def test_sigint_after_a_native_entry_leaves_the_interpreter_alone(tmp_path):
    """
    An action that loops used to install a process-wide handler that ended the interpreter
    (`exit(130)`) on a later Ctrl-C. The extension installs none: every SIGINT raises
    `KeyboardInterrupt` and the process survives.
    """
    code = (
        "import os, signal, threading, time\n"
        "import meerschaum as mrsm\n"
        "print(mrsm.entry(['show', 'version', ':', 'x1']), flush=True)\n"
        "caught = 0\n"
        "for _ in range(4):\n"
        "    threading.Timer(0.3, lambda: os.kill(os.getpid(), signal.SIGINT)).start()\n"
        "    try:\n"
        "        time.sleep(2)\n"
        "    except KeyboardInterrupt:\n"
        "        caught += 1\n"
        "print('CAUGHT', caught, flush=True)\n"
    )
    result = _run_python(code, tmp_path)
    assert result.returncode == 0, result.stderr[-2000:]
    assert 'CAUGHT 4' in result.stdout, result.stdout


def test_sigint_stops_a_looping_entry_and_raises_keyboard_interrupt(tmp_path):
    """The first Ctrl-C is not swallowed: the loop winds down and the call raises."""
    code = (
        "import os, signal, threading, time\n"
        "import meerschaum as mrsm\n"
        "threading.Timer(2.0, lambda: os.kill(os.getpid(), signal.SIGINT)).start()\n"
        "start = time.time()\n"
        "try:\n"
        "    mrsm.entry(['show', 'version', '--nopretty', ':', 'x1000', '--min-seconds', '1'])\n"
        "    print('RETURNED', flush=True)\n"
        "except KeyboardInterrupt:\n"
        "    print('INTERRUPTED', round(time.time() - start) < 10, flush=True)\n"
        "print('ALIVE', flush=True)\n"
    )
    result = _run_python(code, tmp_path)
    assert result.returncode == 0, result.stderr[-2000:]
    assert 'INTERRUPTED True' in result.stdout, result.stdout
    assert 'ALIVE' in result.stdout


def test_prompts_read_pythons_input_not_file_descriptor_zero(monkeypatch, capsys):
    """A patched `input()` (or `sys.stdin`, or Jupyter's stdin) answers the native prompts."""
    import builtins

    asked = []

    def fake_input(prompt=''):
        asked.append(prompt)
        return 'n'

    monkeypatch.setattr(builtins, 'input', fake_input)
    success, message = mrsm.entry(['delete', 'cache'])
    capsys.readouterr()
    assert asked and 'Are you sure' in asked[0], asked
    assert success is False and message == 'Nothing was deleted.'


def test_prompts_end_at_the_end_of_pythons_stdin(monkeypatch, capsys):
    import builtins

    def eof(prompt=''):
        raise EOFError

    monkeypatch.setattr(builtins, 'input', eof)
    success, _ = mrsm.entry(['delete', 'cache'])
    capsys.readouterr()
    assert success is False


def test_running_the_binary_does_not_change_this_processs_environment(monkeypatch):
    from meerschaum._internal import binary

    if binary.find_binary() is None:
        pytest.skip("No `mrsm` binary is built (cargo build -p meerschaum-cli).")
    monkeypatch.delenv('MRSM_PYTHON', raising=False)
    assert binary.run_binary(['show', 'version', '--nopretty']) == 0
    assert 'MRSM_PYTHON' not in os.environ
    assert binary._environment()['MRSM_PYTHON'] == sys.executable


def test_an_invalid_mrsm_bin_is_reported(tmp_path, monkeypatch):
    from meerschaum._internal import binary

    monkeypatch.setenv(binary.BINARY_ENV, str(tmp_path / 'nothing-here'))
    with pytest.warns(RuntimeWarning, match=binary.BINARY_ENV):
        binary.find_binary()


def test_probing_a_removed_module_does_not_raise():
    import importlib.util

    spec = importlib.util.find_spec('meerschaum.actions.sync')
    assert spec is not None
    assert importlib.util.find_spec('meerschaum.no_such_module_w16') is None


def test_importing_the_api_package_outside_the_host_is_quiet_and_installs_nothing(tmp_path):
    result = _run_python("import meerschaum.api\nprint('IMPORTED')", tmp_path, timeout=300)
    output = result.stdout + result.stderr
    assert 'Installing packages' not in output and 'semver' not in output, output[-2000:]
    assert 'FileNotFoundError' not in output, output[-2000:]
    if result.returncode != 0:
        ### Without the `api` extra it is one clear error, not a pile of tracebacks.
        assert output.count('Traceback') == 1 and 'meerschaum[api]' in output, output[-2000:]
    else:
        assert 'IMPORTED' in result.stdout


@pytest.mark.skipif(os.name == 'nt', reason="POSIX environments can hold undecodable bytes")
def test_an_undecodable_environment_variable_does_not_panic_the_bindings(tmp_path):
    code = (
        "from meerschaum import _core\n"
        "print(sorted(_core.compose_env_dict({'environment': {'W16': 'x'}}))[:1] != [])\n"
    )
    env = {**os.environ, 'PYTHONPATH': str(REPO), 'MRSM_ROOT_DIR': str(tmp_path / 'root')}
    (tmp_path / 'root').mkdir(exist_ok=True)
    env_bytes = {k.encode(): v.encode() for k, v in env.items()}
    env_bytes[b'W16_BAD'] = b'\xff\xfe'
    ### The environment layers of the config (`MRSM_SQL_*` connectors, `MRSM_CONFIG`) scan every
    ### variable at import: they must skip the one that is not valid UTF-8.
    env_bytes[b'MRSM_SQL_W16ENV'] = f"sqlite:///{tmp_path / 'env.db'}".encode()
    env_bytes[b'MRSM_CONFIG'] = b'{"meerschaum": {"instance": "sql:w16env"}}'
    result = subprocess.run(
        [sys.executable, '-c', code], capture_output=True, env=env_bytes, timeout=60,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    assert b'True' in result.stdout


### Descendant processes of this one, from `/proc` (Linux): no `psutil`, which no extra provides.
_PROC_TREE = (
    "import os\n"
    "def descendants(pid=None):\n"
    "    parents = {}\n"
    "    for entry in os.listdir('/proc'):\n"
    "        if not entry.isdigit():\n"
    "            continue\n"
    "        try:\n"
    "            with open(f'/proc/{entry}/stat') as f:\n"
    "                data = f.read()\n"
    "        except OSError:\n"
    "            continue\n"
    "        name = data.split('(', 1)[1].rsplit(')', 1)[0]\n"
    "        parents[int(entry)] = (int(data.rsplit(')', 1)[1].split()[1]), name)\n"
    "    found, stack = [], [pid or os.getpid()]\n"
    "    while stack:\n"
    "        current = stack.pop()\n"
    "        for child, (parent, name) in parents.items():\n"
    "            if parent == current:\n"
    "                found.append((child, name))\n"
    "                stack.append(child)\n"
    "    return found\n"
)

linux_only = pytest.mark.skipif(sys.platform != 'linux', reason="counts processes through /proc")


@linux_only
def test_a_plugin_that_calls_entry_when_imported_does_not_start_workers_without_end(tmp_path):
    """
    A plugin module whose import calls `mrsm.entry()` is imported by the worker, whose nested
    call must not start a worker of its own (each would import the plugin again). The nested
    call runs natively, the process count stays bounded and the outer call returns promptly.
    """
    (tmp_path / 'plugins').mkdir()
    (tmp_path / 'plugins' / 'w16rec.py').write_text(
        "import meerschaum as mrsm\n"
        "mrsm.entry(['show', 'version', '--nopretty'])\n"
        "__version__ = '0.0.1'\n"
    )
    code = _PROC_TREE + (
        "import threading, time\n"
        "import meerschaum as mrsm\n"
        "peak = [0]\n"
        "done = threading.Event()\n"
        "def watch():\n"
        "    while not done.is_set():\n"
        "        try:\n"
        "            peak[0] = max(peak[0], len(descendants()))\n"
        "        except Exception:\n"
        "            pass\n"
        "        time.sleep(0.05)\n"
        "threading.Thread(target=watch, daemon=True).start()\n"
        "start = time.time()\n"
        "result = mrsm.entry(['show', 'version', '--nopretty'])\n"
        "done.set()\n"
        "print('RESULT', result[0], 'PEAK', peak[0], 'SECONDS', round(time.time() - start), flush=True)\n"
    )
    result = _run_python(
        code, tmp_path, timeout=120, extra_env={'MRSM_PLUGINS_DIR': str(tmp_path / 'plugins')},
    )
    assert result.returncode == 0, result.stderr[-2000:]
    line = next(l for l in result.stdout.splitlines() if l.startswith('RESULT'))
    _, ok, _, peak, _, seconds = line.split()
    assert ok == 'True', result.stdout
    assert int(peak) <= 8, line
    assert int(seconds) < 60, line


_COUNTING_STDOUT = (
    "import sys, time\n"
    "class Counting:\n"
    "    def __init__(self, real):\n"
    "        self.real, self.writes = real, []\n"
    "    def write(self, text):\n"
    "        self.writes.append((time.time(), text))\n"
    "        return self.real.write(text)\n"
    "    def flush(self):\n"
    "        self.real.flush()\n"
    "    def __getattr__(self, name):\n"
    "        return getattr(self.real, name)\n"
    "out = Counting(sys.stdout)\n"
    "sys.stdout = out\n"
)


@linux_only
def test_after_a_second_ctrl_c_a_new_entry_does_not_revive_the_old_loop(tmp_path):
    """
    The stop flag belongs to the call. Each step of this loop is a 1 s shell command that does not
    look at the flag, so the second Ctrl-C abandons the call while its thread still runs; a new
    `entry()` must not clear the abandoned loop's stop (a process-wide flag was cleared by it).
    """
    code = _PROC_TREE + (
        "import os, signal, sys, threading, time\n"
        "import meerschaum as mrsm\n"
        "kill = lambda: os.kill(os.getpid(), signal.SIGINT)\n"
        "threading.Timer(0.4, kill).start()\n"
        "threading.Timer(0.7, kill).start()\n"
        "try:\n"
        "    mrsm.entry(['sleep', '1', ':', 'x1000', '--min-seconds', '0'])\n"
        "except KeyboardInterrupt:\n"
        "    print('ABANDONED', flush=True)\n"
        "### A new call must not clear the abandoned loop's stop.\n"
        "print(mrsm.entry(['show', 'version', '--nopretty']), flush=True)\n"
        "time.sleep(2)\n"
        "seen = 0\n"
        "end = time.time() + 3\n"
        "while time.time() < end:\n"
        "    seen += sum(1 for _, name in descendants() if name == 'sleep')\n"
        "    time.sleep(0.05)\n"
        "print('NEW_STEPS', seen, flush=True)\n"
    )
    result = _run_python(code, tmp_path)
    assert result.returncode == 0, result.stderr[-2000:]
    assert 'ABANDONED' in result.stdout, result.stdout
    assert 'NEW_STEPS 0' in result.stdout, result.stdout


def test_concurrent_entries_have_their_own_stop(tmp_path):
    """Interrupting the main thread's loop does not stop (or restart) a loop on another thread."""
    code = _COUNTING_STDOUT + (
        "import os, signal, threading\n"
        "import meerschaum as mrsm\n"
        "results = []\n"
        "other = threading.Thread(target=lambda: results.append(mrsm.entry(\n"
        "    ['show', 'version', '--nopretty', ':', 'x3', '--min-seconds', '1'])))\n"
        "other.start()\n"
        "time.sleep(0.5)\n"
        "threading.Timer(1.0, lambda: os.kill(os.getpid(), signal.SIGINT)).start()\n"
        "try:\n"
        "    mrsm.entry(['show', 'version', '--nopretty', ':', 'x1000', '--min-seconds', '1'])\n"
        "except KeyboardInterrupt:\n"
        "    pass\n"
        "other.join(30)\n"
        "text = ''.join(t for _, t in out.writes)\n"
        "print('OTHER', results[0][0] if results else None, 'Ran pipeline 3 times' in text,\n"
        "      file=sys.stderr, flush=True)\n"
    )
    result = _run_python(code, tmp_path)
    assert result.returncode == 0, result.stderr[-2000:]
    assert 'OTHER True True' in result.stderr, result.stderr[-2000:]


_NESTED_PLUGINS = {
    'w16other.py': (
        "from meerschaum.plugins import make_action\n"
        "__version__ = '0.0.1'\n"
        "@make_action\n"
        "def w16other(**kw):\n"
        "    return True, 'other plugin ran'\n"
    ),
    'w16outer.py': (
        "from meerschaum.plugins import make_action\n"
        "__version__ = '0.0.1'\n"
        "@make_action\n"
        "def w16outer(**kw):\n"
        "    import meerschaum as mrsm\n"
        "    return mrsm.entry(['w16other'])\n"
    ),
    'w16src.py': (
        "from datetime import datetime\n"
        "__version__ = '0.0.1'\n"
        "def fetch(pipe, **kw):\n"
        "    return {'dt': [datetime(2026, 1, 1), datetime(2026, 1, 2)], 'v': [1, 2]}\n"
    ),
    'w16sync.py': (
        "from meerschaum.plugins import make_action\n"
        "__version__ = '0.0.1'\n"
        "@make_action\n"
        "def w16sync(**kw):\n"
        "    import meerschaum as mrsm\n"
        "    return mrsm.entry([\n"
        "        'sync', 'pipes', '-c', 'plugin:w16src', '-m', 'm', '-i', 'sql:w16', '--nopretty',\n"
        "    ])\n"
    ),
}

_NESTED_SCRIPT = (
    "import meerschaum as mrsm\n"
    "pipe = mrsm.Pipe('plugin:w16src', 'm', instance='sql:w16', columns={'datetime': 'dt'})\n"
    "pipe.register()\n"
    "print('OUTER', mrsm.entry(['w16outer']), flush=True)\n"
    "print('SYNC', mrsm.entry(['w16sync']), flush=True)\n"
    "print('ROWS', pipe.get_rowcount(), flush=True)\n"
)


def _nested_run(python_args, cwd, tmp_path, tree: bool):
    import re
    plugins = tmp_path / 'plugins'
    plugins.mkdir(parents=True, exist_ok=True)
    for name, text in _NESTED_PLUGINS.items():
        (plugins / name).write_text(text)
    (tmp_path / 'root').mkdir(parents=True, exist_ok=True)
    env = {
        k: v for k, v in os.environ.items()
        if not k.startswith('MRSM_') and k not in ('PYTHONPATH',)
    }
    env.update({
        'MRSM_ROOT_DIR': str(tmp_path / 'root'),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_SQL_W16': f"sqlite:///{tmp_path / 'w16.db'}",
    })
    if tree:
        env['PYTHONPATH'] = str(REPO)
    result = subprocess.run(
        [*python_args, '-c', _NESTED_SCRIPT],
        capture_output=True, text=True, env=env, cwd=cwd, timeout=600,
    )
    assert result.returncode == 0, result.stderr[-3000:]
    ansi = re.compile(r'\x1b\[[0-9;]*m')
    lines = {}
    for line in ansi.sub('', result.stdout).splitlines():
        for key in ('OUTER', 'SYNC', 'ROWS'):
            if line.startswith(key + ' '):
                lines[key] = re.sub(r'[\d.]+', 'N', line)
    return lines, result


### What the released v4.1.4 returns for `_NESTED_SCRIPT` (recorded with `.venv-ref`; the digits are
### masked, and `seconds` is `second` for a sync under one second).
_V4_NESTED = {
    'OUTER': "OUTER (True, 'other plugin ran')",
    'SYNC': "SYNC (True, 'It took N seconds to sync N pipe\\n    (N succeeded, N failed)N')",
    'ROWS': 'ROWS N',
}


def _same_as_v4(lines):
    assert lines['OUTER'] == _V4_NESTED['OUTER']
    assert lines['ROWS'] == _V4_NESTED['ROWS']
    assert lines['SYNC'].replace('second ', 'seconds ') == _V4_NESTED['SYNC']


def test_a_plugin_action_can_run_another_plugin_action_and_sync_a_plugin_source(tmp_path):
    """
    Inside a worker, `mrsm.entry()` is not refused (only a call made while a plugin is being
    imported is): a plugin action runs another plugin's action and `sync pipes -c plugin:<src>`,
    with the results the released v4.1.4 gives.
    """
    lines, result = _nested_run([sys.executable], REPO, tmp_path, tree=True)
    assert 'not starting a Python worker' not in result.stderr
    _same_as_v4(lines)


@pytest.mark.skipif(
    not os.environ.get('MRSM_TEST_V4_LIVE'),
    reason="Set MRSM_TEST_V4_LIVE=1 to re-record the v4.1.4 results with `.venv-ref` (minutes).",
)
def test_the_recorded_v4_results_are_current(tmp_path):
    reference = REPO / '.venv-ref' / 'bin' / 'python'
    if not reference.exists():
        pytest.skip("No reference interpreter (scripts/parity/setup_reference.sh).")
    lines, _ = _nested_run([str(reference), '-s', '-E'], tmp_path, tmp_path, tree=False)
    _same_as_v4({**lines, 'SYNC': lines['SYNC'].replace('second ', 'seconds ')})


def _marked_pids(marker: str):
    """The processes whose environment holds `W16_MARK=<marker>` (Linux; our own user's)."""
    needle = f'W16_MARK={marker}'.encode()
    found = []
    for entry in os.scandir('/proc'):
        if not entry.name.isdigit():
            continue
        try:
            with open(f'/proc/{entry.name}/environ', 'rb') as f:
                if needle in f.read().split(b'\0'):
                    found.append(int(entry.name))
        except OSError:
            continue
    return found


def _kill_marked(marker: str) -> None:
    import signal
    for pid in _marked_pids(marker):
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass


@pytest.mark.skipif(not os.path.isdir('/proc'), reason="Counts processes through /proc")
def test_a_plugin_that_syncs_a_plugin_pipe_when_imported_keeps_the_process_count_bounded(tmp_path):
    """
    A plugin whose import syncs a `plugin:` pipe starts a worker for the source, which imports
    the plugin again: without the guard around plugin imports the workers never end. The
    watchdog kills every process carrying this test's marker (env `W16_MARK`) the moment there
    are too many, so a regression cannot run away in CI.
    """
    import threading
    import time
    import uuid

    marker = uuid.uuid4().hex
    plugins = tmp_path / 'plugins'
    plugins.mkdir()
    (plugins / 'w16src.py').write_text(
        "from datetime import datetime\n"
        "__version__ = '0.0.1'\n"
        "def fetch(pipe, **kw):\n"
        "    return {'dt': [datetime(2026, 1, 1)], 'v': [1]}\n"
    )
    (plugins / 'w16syncimp.py').write_text(
        "import meerschaum as mrsm\n"
        "__version__ = '0.0.1'\n"
        "mrsm.Pipe('plugin:w16src', 'imp', instance='sql:w16').sync()\n"
    )
    (tmp_path / 'root').mkdir()
    env = {
        **os.environ,
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(tmp_path / 'root'),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_SQL_W16': f"sqlite:///{tmp_path / 'w16.db'}",
        'W16_MARK': marker,
    }
    peak = [0]
    runaway = threading.Event()
    done = threading.Event()
    limit = 30

    def watchdog():
        while not done.is_set():
            pids = _marked_pids(marker)
            peak[0] = max(peak[0], len(pids))
            if len(pids) > limit:
                runaway.set()
                _kill_marked(marker)
            time.sleep(0.1)

    thread = threading.Thread(target=watchdog, daemon=True)
    thread.start()
    started = time.time()
    try:
        result = subprocess.run(
            [sys.executable, '-c',
             "import meerschaum as mrsm\n"
             "print('RESULT', mrsm.entry(['show', 'version', '--nopretty'])[0], flush=True)\n"],
            capture_output=True, text=True, env=env, timeout=120,
        )
    finally:
        done.set()
        thread.join(5)
        _kill_marked(marker)
    assert not runaway.is_set(), f"workers multiplied (peak {peak[0]})"
    assert result.returncode == 0, result.stderr[-2000:]
    assert 'RESULT True' in result.stdout, result.stdout
    assert peak[0] <= 12, peak[0]
    assert time.time() - started < 90
    assert _marked_pids(marker) == []


def test_the_plugin_import_guard_does_nothing_outside_a_worker():
    import meerschaum.plugins as plugins

    assert plugins._IN_WORKER is False
    with plugins.plugin_import_guard():
        pass


def test_an_undecodable_environment_variable_does_not_panic_verify_or_the_systemd_text(tmp_path):
    """`Pipe.verify()` (the runtime's `vars`) and the systemd unit text read the environment."""
    code = (
        "import meerschaum as mrsm\n"
        "from meerschaum.jobs.systemd import SystemdExecutor\n"
        "pipe = mrsm.Pipe('w16', 'vars', instance='sql:w16env', columns={'datetime': 'dt'})\n"
        "print('SYNC', pipe.sync({'dt': ['2026-01-01'], 'v': [1]})[0], flush=True)\n"
        "print('VERIFY', pipe.verify()[0], flush=True)\n"
        "text = SystemdExecutor().get_service_file_text('w16job', ['show', 'version'])\n"
        "print('UNIT', 'ExecStart' in text, flush=True)\n"
    )
    (tmp_path / 'root').mkdir()
    (tmp_path / 'plugins').mkdir()
    env = {k.encode(): v.encode() for k, v in os.environ.items()}
    env.update({
        b'PYTHONPATH': str(REPO).encode(),
        b'MRSM_ROOT_DIR': str(tmp_path / 'root').encode(),
        b'MRSM_PLUGINS_DIR': str(tmp_path / 'plugins').encode(),
        b'MRSM_SQL_W16ENV': f"sqlite:///{tmp_path / 'env.db'}".encode(),
        b'W16_BAD': b'\xff\xfe',
    })
    result = subprocess.run(
        [sys.executable, '-c', code], capture_output=True, env=env, timeout=120,
    )
    assert result.returncode == 0, result.stderr[-2000:]
    assert b'VERIFY True' in result.stdout and b'UNIT True' in result.stdout, result.stdout
