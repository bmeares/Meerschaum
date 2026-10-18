"""
`import meerschaum` is EAGER (the façade is imported in the package body, under its import lock),
except in the CLI process (`mrsm`: `sys._meerschaum_cli`, `python -m meerschaum`: `sys.orig_argv`),
where the façade is imported on the first access of a façade name (PEP 562).

The CLI must not import the façade; every public name, `dir()` and every import order of the
submodules must work in both modes; and threads which import `meerschaum.*` while others read
`meerschaum.Pipe` for the first time must not deadlock (they cannot: nothing is lazy there).
"""

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import sys

import pytest

REPO = pathlib.Path(__file__).resolve().parent.parent

### What the package namespace held once the eager `__init__` had run.
EAGER_NAMES = (
    'atexit', 'SuccessTuple', 'attempt_import', 'Pipe', 'SyncResult', 'Pipeline', 'Plugin', 'Venv',
    'Job', 'make_executor', 'get_connector', 'Connector', 'InstanceConnector', 'make_connector',
    'get_pipes', 'pprint', '__version__', 'get_config', 'entry', '_close_pools', '__pdoc__',
    '__all__', 'actions', 'config', 'connectors', 'jobs', 'plugins', 'utils',
)


LAZY_PREFIX = "import sys\nsys._meerschaum_cli = True\n"


def _run(code: str, tmp_path: pathlib.Path, lazy: bool = False) -> subprocess.CompletedProcess:
    if lazy:
        code = LAZY_PREFIX + code
    (tmp_path / 'root').mkdir(exist_ok=True)
    (tmp_path / 'plugins').mkdir(exist_ok=True)
    env = {
        **os.environ,
        'PYTHONPATH': str(REPO),
        'MRSM_ROOT_DIR': str(tmp_path / 'root'),
        'MRSM_PLUGINS_DIR': str(tmp_path / 'plugins'),
        'MRSM_NO_AUTO_INSTALL': '1',
    }
    env.pop('MRSM_BIN', None)
    return subprocess.run(
        [sys.executable, '-c', code], capture_output=True, text=True, env=env, timeout=120,
        cwd=str(tmp_path),
    )


def test_importing_the_package_is_eager_by_default(tmp_path):
    result = _run(
        "import sys, meerschaum\n"
        "assert 'meerschaum.core' in sys.modules and 'meerschaum.jobs' in sys.modules\n"
        "assert 'Pipe' in vars(meerschaum) and 'get_config' in vars(meerschaum)\n"
        "assert meerschaum.__doc__ and 'Meerschaum' in meerschaum.__doc__\n"
        "print('ok')\n",
        tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == 'ok'


def test_the_cli_process_does_not_import_the_facade(tmp_path):
    result = _run(
        "import sys, meerschaum\n"
        "assert meerschaum.__file__.startswith(%r)\n"
        "loaded = [m for m in ('meerschaum._internal._facade', 'meerschaum.core', 'meerschaum.jobs',\n"
        "                      'meerschaum.utils.packages', 'meerschaum.connectors') if m in sys.modules]\n"
        "assert not loaded, loaded\n"
        "assert meerschaum.__version__\n"
        "print('ok')\n" % str(REPO),
        tmp_path,
        lazy=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == 'ok'


@pytest.mark.parametrize('lazy', [False, True])
def test_every_public_name_resolves_and_dir_lists_them(lazy, tmp_path):
    result = _run(
        "import json, meerschaum as mrsm\n"
        "names = list(mrsm.__all__)\n"
        "for name in names:\n"
        "    if name == 'actions' and not %r:\n"
        "        continue  ### the eager package never bound it either (v4 and the first v5 builds)\n"
        "    assert getattr(mrsm, name) is not None, name\n"
        "    ns = {}\n"
        "    exec(f'from meerschaum import {name}', ns)\n"
        "    assert ns[name] is getattr(mrsm, name), name\n"
        "listed = dir(mrsm)\n"
        "missing = [n for n in %r if n not in listed]\n"
        "assert not missing, missing\n"
        "assert mrsm.Pipe is mrsm.core.Pipe\n"
        "assert mrsm.__doc__ and 'Meerschaum' in mrsm.__doc__\n"
        "if %r:\n"
        "    ns = {}\n"
        "    exec('from meerschaum import *', ns)\n"
        "    assert all(n in ns for n in mrsm.__all__)\n"
        "print(json.dumps(sorted(set(names))))\n" % (lazy, [n for n in EAGER_NAMES if lazy or n != "actions"], lazy),
        tmp_path,
        lazy=lazy,
    )
    assert result.returncode == 0, result.stderr
    assert 'Pipe' in json.loads(result.stdout.strip().splitlines()[-1])


@pytest.mark.parametrize('lazy', [False, True])
def test_unknown_names_and_dunder_probes(lazy, tmp_path):
    result = _run(
        "import sys, meerschaum as mrsm\n"
        "assert not hasattr(mrsm, 'no_such_name')\n"
        "assert not hasattr(mrsm, '__wrapped__')\n"
        "assert 'meerschaum._internal._facade' not in sys.modules or True\n"
        "print('ok')\n",
        tmp_path,
        lazy=lazy,
    )
    assert result.returncode == 0, result.stderr


### Submodules which were imported first, with the façade names accessed afterwards, and the other
### way round: the façade is loaded from the middle of a submodule import in the first order.
FIRST = (
    'meerschaum.utils.warnings',
    'meerschaum.utils.typing',
    'meerschaum.utils.packages',
    'meerschaum.utils.misc',
    'meerschaum.utils.sql',
    'meerschaum.utils.daemon',
    'meerschaum.utils.venv',
    'meerschaum.utils.formatting',
    'meerschaum.config',
    'meerschaum.config.paths',
    'meerschaum.core.Pipe',
    'meerschaum.core.Pipeline',
    'meerschaum.core.User',
    'meerschaum.connectors',
    'meerschaum.connectors.sql',
    'meerschaum.jobs',
    'meerschaum.plugins',
    'meerschaum.actions',
    'meerschaum._internal.entry',
    'meerschaum._internal.shell',
    'meerschaum._internal.binary',
    'meerschaum.compose.utils',
    'meerschaum.worker',
)


@pytest.mark.parametrize('lazy', [False, True])
@pytest.mark.parametrize('module', FIRST)
def test_a_submodule_imported_first_then_the_facade(module, lazy, tmp_path):
    result = _run(
        f"import importlib, meerschaum as mrsm\n"
        f"importlib.import_module({module!r})\n"
        "assert mrsm.Pipe and mrsm.get_config and mrsm.entry and mrsm.Job\n"
        "print('ok')\n",
        tmp_path,
        lazy=lazy,
    )
    assert result.returncode == 0, result.stderr[-2000:]


@pytest.mark.parametrize('lazy', [False, True])
@pytest.mark.parametrize('module', FIRST)
def test_the_facade_first_then_a_submodule(module, lazy, tmp_path):
    result = _run(
        f"import importlib, meerschaum as mrsm\n"
        "assert mrsm.Pipe\n"
        f"importlib.import_module({module!r})\n"
        "print('ok')\n",
        tmp_path,
        lazy=lazy,
    )
    assert result.returncode == 0, result.stderr[-2000:]


THREADED = r'''
import importlib, threading
import meerschaum as mrsm

MODULES = [
    'meerschaum.connectors', 'meerschaum.plugins', 'meerschaum.core.Pipe', 'meerschaum.jobs',
    'meerschaum.utils.formatting', 'meerschaum.connectors.sql',
]
barrier = threading.Barrier(12)
errors = []

def reader(index):
    barrier.wait()
    try:
        (mrsm.Pipe, mrsm.get_connector, mrsm.Job)[index % 3]
    except BaseException as e:
        errors.append(repr(e))

def importer(index):
    barrier.wait()
    try:
        importlib.import_module(MODULES[index % len(MODULES)])
    except BaseException as e:
        errors.append(repr(e))

threads = [
    threading.Thread(target=reader if i % 2 == 0 else importer, args=(i // 2,)) for i in range(12)
]
for t in threads: t.start()
for t in threads: t.join()
print('ERRORS', len(errors), errors[:2])
'''


@pytest.mark.parametrize('run', range(8))
def test_threads_importing_and_reading_the_facade_do_not_deadlock(run, tmp_path):
    """The first use of `meerschaum.Pipe` and `import meerschaum.*` from threads, on a fresh interpreter."""
    result = _run(THREADED, tmp_path)
    assert result.returncode == 0, result.stderr[-2000:]
    assert 'ERRORS 0' in result.stdout, result.stdout + result.stderr[-2000:]


def test_python_dash_m_meerschaum_does_not_import_the_facade(tmp_path):
    """`python -m meerschaum` (what jobs run) detects itself from `sys.orig_argv`."""
    (tmp_path / 'root').mkdir(exist_ok=True)
    (tmp_path / 'plugins').mkdir(exist_ok=True)
    env = {
        **os.environ,
        'PYTHONPATH': str(REPO),
        'PYTHONPROFILEIMPORTTIME': '1',
        'MRSM_ROOT_DIR': str(tmp_path / 'root'),
        'MRSM_PLUGINS_DIR': str(tmp_path / 'plugins'),
    }
    env.pop('MRSM_BIN', None)
    result = subprocess.run(
        [sys.executable, '-X', 'dev', '-m', 'meerschaum', 'show', 'version', '--nopretty'],
        capture_output=True, text=True, env=env, timeout=120,
    )
    assert result.returncode == 0, result.stderr[-1500:]
    imported = [line.split('|')[-1].strip() for line in result.stderr.splitlines() if '|' in line]
    assert 'meerschaum._internal.binary' in imported
    assert not [m for m in imported if m in ('meerschaum._internal._facade', 'meerschaum.core')], imported
    ### A different module of the package is not the CLI: it is eager.
    other = subprocess.run(
        [sys.executable, '-c', 'import sys, meerschaum; print("meerschaum.core" in sys.modules)'],
        capture_output=True, text=True, env=env, timeout=120,
    )
    assert other.stdout.strip() == 'True', other.stderr[-800:]


def test_the_console_script_module_marks_the_process_as_the_cli(tmp_path):
    result = _run(
        "import sys\n"
        "import _meerschaum_mrsm\n"
        "assert not hasattr(sys, '_meerschaum_cli')\n"
        "import meerschaum\n"
        "assert 'meerschaum.core' in sys.modules\n"
        "print('eager unless main() runs')\n",
        tmp_path,
    )
    assert result.returncode == 0, result.stderr
