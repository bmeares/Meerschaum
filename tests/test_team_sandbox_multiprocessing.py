"""
5.1: where Unix sockets are denied to isolated teams' code by the seccomp filter, a team worker's
bootstrap (`meerschaum/_internal/team_sandbox.py`) keeps `multiprocessing` to what works:
'spawn' by default, a clear error for 'forkserver', and `Manager()` over loopback TCP. Each case
runs in a fresh interpreter (the hook patches the standard library for good).
"""

import json
import os
import subprocess
import sys
import textwrap

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BOOT = os.path.join(REPO, 'meerschaum', '_internal', 'team_sandbox.py')

PRELUDE = f"""
import importlib.util, json, sys
sys.path.insert(0, {REPO!r})
spec = importlib.util.spec_from_file_location('_tsb', {BOOT!r})
tsb = importlib.util.module_from_spec(spec)
spec.loader.exec_module(tsb)
"""


def _run(body: str) -> dict:
    code = PRELUDE + textwrap.dedent(body)
    out = subprocess.run(
        [sys.executable, '-I', '-c', code],
        capture_output=True, text=True, timeout=120, cwd=REPO,
    )
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout.strip().splitlines()[-1])


def test_the_hook_keeps_multiprocessing_to_what_works():
    result = _run("""
        tsb.restrict_multiprocessing()
        tsb.restrict_multiprocessing()
        import multiprocessing as mp
        from concurrent.futures import ProcessPoolExecutor
        out = {'method': mp.get_start_method()}
        if __name__ == '__main__':
            with mp.Pool(2) as pool:
                out['pool'] = pool.map(abs, [-1, -2])
            with ProcessPoolExecutor(2) as ex:
                out['executor'] = list(ex.map(abs, [-3]))
            with mp.Manager() as manager:
                out['manager_address'] = list(manager.address)
            for attempt in ('get', 'set'):
                try:
                    if attempt == 'get':
                        mp.get_context('forkserver')
                    else:
                        mp.set_start_method('forkserver', force=True)
                    out[attempt] = 'allowed'
                except ValueError as e:
                    out[attempt] = str(e)
            out['fork'] = type(mp.get_context('fork')).__name__
        print(json.dumps(out))
    """)
    assert result['method'] == 'spawn'
    assert result['pool'] == [1, 2] and result['executor'] == [3]
    assert result['manager_address'][0] == '127.0.0.1'
    for attempt in ('get', 'set'):
        assert "'forkserver' start method" in result[attempt]
        assert 'Unix sockets are denied' in result[attempt]
    assert result['fork'] == 'ForkContext'


@pytest.mark.parametrize('unix_sockets, restricted', [('seccomp', True), ('landlock', False)])
def test_boot_restricts_multiprocessing_only_under_the_filter(tmp_path, unix_sockets, restricted):
    spec_file = tmp_path / 'sandbox.json'
    spec_file.write_text('{"mode": "auto"}')
    report = {'mode': 'auto', 'enforced': True, 'unix_sockets': unix_sockets}
    result = _run(f"""
        import os, types
        os.environ['MRSM_TEAM_SANDBOX'] = {str(spec_file)!r}
        native = types.SimpleNamespace(team_sandbox_enforce=lambda paths, socket: {json.dumps(report)!r})
        tsb._load_native = lambda package_dir: native
        report = tsb.boot()
        import multiprocessing.context as context
        print(json.dumps({{
            'report': report,
            'restricted': bool(getattr(context, '_mrsm_restricted', False)),
        }}))
    """)
    assert result['report']['unix_sockets'] == unix_sockets
    assert result['restricted'] is restricted
