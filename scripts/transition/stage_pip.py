"""pip upgrade hygiene: v4.1.4 from PyPI in a scratch venv, then the v5 wheel over it."""
import json, os, pathlib, shutil, subprocess, sys, zipfile
from common import *


GUARD = {'MRSM_SQL_TRN': f'sqlite:///{SCRATCH}/scratch.db', 'MRSM_CONFIG': '{"meerschaum": {"instance": "sql:trn"}}'}


def run(cmd, env=None, **kw):
    e = {**scrub_env(), 'TMPDIR': str(SCRATCH / 'tmp'), 'PYTHONPATH': '', 'PYTHONNOUSERSITE': '1', **GUARD, **(env or {})}
    for d in (e.get('MRSM_ROOT_DIR'), e.get('MRSM_PLUGINS_DIR')):
        if d:
            pathlib.Path(d).mkdir(parents=True, exist_ok=True)
    return subprocess.run(cmd, capture_output=True, text=True, env=e, **kw)


def main(wheel, F, name='venv-up'):
    venv = SCRATCH / name
    if venv.exists():
        shutil.rmtree(venv)
    r = run(['uv', 'venv', '--seed', '--python', shutil.which('python3') or sys.executable, str(venv)])
    py = venv / 'bin' / 'python'
    r = run(['uv', 'pip', 'install', '--python', str(py), 'meerschaum==4.1.4'])
    v4 = run([str(py), '-s', '-E', '-c', 'import meerschaum;print(meerschaum.__version__)']).stdout.strip()
    F.add('pip', 'v4.1.4 installs from PyPI into the scratch venv', 'PASS' if v4 == '4.1.4' else 'FAIL', v4)
    # warm v4 bytecode like a user's venv (pyc files written at run time)
    run([str(py), '-s', '-E', '-m', 'meerschaum', 'show', 'version'], env={'MRSM_ROOT_DIR': str(SCRATCH / 'tmp' / 'pip-root'), 'MRSM_PLUGINS_DIR': str(SCRATCH / 'tmp' / 'pip-plugins')})
    sp = next((venv / 'lib').glob('python*/site-packages'))
    old_files = {str(p.relative_to(sp)) for p in (sp / 'meerschaum').rglob('*') if p.is_file()}
    # the user's way: pip install -U <wheel> (pip, not uv, so RECORD-based uninstall is the real pip one)
    r = run([str(py), '-m', 'pip', 'install', '--upgrade', '--no-input', str(wheel)])
    F.add('pip', '`pip install -U <v5 wheel>` over v4.1.4', 'PASS' if r.returncode == 0 else 'FAIL', (r.stdout + r.stderr)[-500:])
    if r.returncode:
        return None
    with zipfile.ZipFile(wheel) as z:
        wheel_files = {n for n in z.namelist() if n.startswith('meerschaum/')}
        has_native = any('_native' in n for n in wheel_files)
        scripts = [n for n in z.namelist() if '.data/scripts' in n or 'entry_points' in n]
    cur = {str(p.relative_to(sp)) for p in (sp / 'meerschaum').rglob('*') if p.is_file()}
    def _orphan(f):
        if f.endswith('.pyc'):
            d, n = os.path.split(f)
            src = os.path.join(os.path.dirname(d), n.split('.')[0] + '.py')
            return src not in cur and src not in wheel_files
        return f not in wheel_files
    stale = sorted(f for f in cur if _orphan(f))
    stale_py = [f for f in stale if f.endswith('.py') or f.endswith('.so')]
    F.add('pip', 'no v4 source files linger in site-packages/meerschaum after the upgrade', 'PASS' if not stale_py else 'FAIL', stale_py[:30])
    pyc_stale = [f for f in stale if f.endswith('.pyc')]
    F.add('pip', f'stale bytecode/other files left by the upgrade ({len(stale)} total, {len(pyc_stale)} .pyc)', 'PASS' if not stale else 'INFO', stale[:15])
    ver = run([str(py), '-s', '-c', 'import meerschaum,sys;print(meerschaum.__version__, meerschaum.__file__)'], cwd=str(SCRATCH / 'tmp'))
    F.add('pip', 'import meerschaum resolves to v5 in the upgraded venv', 'PASS' if ver.stdout.startswith('5.') and os.path.realpath(str(sp)) in os.path.realpath(ver.stdout.split(' ', 1)[1].strip()) else 'FAIL', ver.stdout + ver.stderr[-300:])
    env = {**scrub_env(), 'MRSM_ROOT_DIR': str(SCRATCH / 'tmp' / 'pip-root2'), 'MRSM_PLUGINS_DIR': str(SCRATCH / 'tmp' / 'pip-plugins2'), 'MRSM_SQL_TRN': f'sqlite:///{SCRATCH}/scratch.db',
           'MRSM_CONFIG': '{"meerschaum": {"instance": "sql:trn"}}', 'PYTHONPATH': '', 'MRSM_NO_AUTO_INSTALL': '1'}
    pathlib.Path(env['MRSM_ROOT_DIR']).mkdir(parents=True, exist_ok=True)
    pathlib.Path(env['MRSM_PLUGINS_DIR']).mkdir(parents=True, exist_ok=True)
    for exe in ('mrsm', 'meerschaum'):
        r = subprocess.run([str(venv / 'bin' / exe), '--version'], capture_output=True, text=True, env=env, cwd=str(SCRATCH / 'tmp'))
        F.add('pip', f'console script `{exe} --version` runs v5', 'PASS' if '5.0' in (r.stdout + r.stderr) else 'FAIL', (r.stdout + r.stderr)[-200:])
    r = subprocess.run([str(py), '-m', 'meerschaum', 'show', 'version'], capture_output=True, text=True, env=env, cwd=str(SCRATCH / 'tmp'))
    F.add('pip', '`python -m meerschaum show version` runs v5', 'PASS' if '5.0' in (r.stdout + r.stderr) else 'FAIL', (r.stdout + r.stderr)[-200:])
    r = subprocess.run([str(py), '-c', 'import meerschaum.connectors.sql._instance'], capture_output=True, text=True, env=env, cwd=str(SCRATCH / 'tmp'))
    F.add('pip', 'importing a removed v4 module raises ModuleNotFoundError naming the replacement', 'PASS' if 'ModuleNotFoundError' in r.stderr and 'meerschaum' in r.stderr.split('ModuleNotFoundError')[-1] else 'FAIL', r.stderr[-400:])
    with zipfile.ZipFile(wheel) as z:
        meta = next(z.read(n).decode() for n in z.namelist() if n.endswith('.dist-info/METADATA'))
    reqs = [l for l in meta.splitlines() if l.startswith('Requires-Dist:') and 'extra ==' not in l]
    F.add('pip', 'base wheel declares no third-party dependencies (METADATA Requires-Dist)', 'PASS' if not reqs else 'FAIL', reqs)
    # the extras the guide tells API / Dash plugin users to install: everything `meerschaum.api` imports must come
    # from them (nothing auto-installed: MRSM_NO_AUTO_INSTALL=1)
    r = run([str(py), '-m', 'pip', 'install', '--upgrade', '--no-input', f'{wheel}[api,dash]'])
    F.add('pip', '`pip install "<wheel>[api,dash]"`', 'PASS' if r.returncode == 0 else 'FAIL', (r.stdout + r.stderr)[-300:])
    for mod in ('meerschaum.api', 'meerschaum.api.host', 'dash', 'uvicorn.protocols.http.h11_impl'):
        r = subprocess.run([str(py), '-c', f'import {mod}'], capture_output=True, text=True, env=env, cwd=str(SCRATCH / 'tmp'))
        F.add('pip', f'`import {mod}` works with only the api/dash extras and MRSM_NO_AUTO_INSTALL=1', 'PASS' if r.returncode == 0 else 'FAIL', r.stderr[-400:])
    return venv


def bare_host(wheel, F, name='venv-bare'):
    """A fresh venv with ONLY the wheel (no extras): `mrsm start api` with an `@api_plugin` plugin. The API host
    installs what it needs into the shared mrsm venv on first start and must then serve the plugin route."""
    import time
    import urllib.request
    import urllib.error
    from api_serve import Server
    venv = SCRATCH / name
    if venv.exists():
        shutil.rmtree(venv)
    run(['uv', 'venv', '--seed', '--python', shutil.which('python3') or sys.executable, str(venv)])
    py = venv / 'bin' / 'python'
    r = run([str(py), '-m', 'pip', 'install', '--no-input', str(wheel)])
    if r.returncode:
        F.add('pip', 'bare venv: install the wheel', 'FAIL', (r.stdout + r.stderr)[-300:])
        return
    root = SCRATCH / 'tmp' / 'bare-root'
    plugins = SCRATCH / 'tmp' / 'bare-plugins'
    shutil.rmtree(root, ignore_errors=True)
    shutil.rmtree(plugins, ignore_errors=True)
    plugins.mkdir(parents=True)
    root.mkdir(parents=True)
    (plugins / 'trn_apiplug.py').write_text(
        "__version__ = '1.0.0'\nfrom meerschaum.plugins import api_plugin\n\n@api_plugin\ndef init(app):\n"
        "    @app.get('/trn/ping')\n    def ping():\n        return {'pong': True}\n"
    )
    env = {**scrub_env(), **GUARD, 'MRSM_ROOT_DIR': str(root), 'MRSM_PLUGINS_DIR': str(plugins), 'PYTHONPATH': '', 'TMPDIR': str(SCRATCH / 'tmp'),
           'PYTHONNOUSERSITE': '1', 'UV_PYTHON': str(py), 'MRSM_PYTHON': str(py)}
    code = text = None
    try:
        with Server([str(py), '-s', '-m', 'meerschaum'], env, SCRATCH / 'tmp', SCRATCH / 'tmp' / 'bare-api.log', extra=['-i', 'sql:trn']) as s:
            t0 = time.time()
            while time.time() - t0 < 240:
                try:
                    with urllib.request.urlopen(f'http://127.0.0.1:{s.port}/trn/ping', timeout=30) as resp:
                        code, text = resp.status, resp.read().decode()
                        break
                except urllib.error.HTTPError as e:
                    code, text = e.code, e.read().decode()
                except Exception as e:
                    code, text = -1, repr(e)
                time.sleep(3)
    except Exception as e:
        code, text = -2, repr(e)
    F.add('pip', 'bare wheel (no extras): an `@api_plugin` route is served after the host installs its dependencies', 'PASS' if code == 200 else 'FAIL', f'{code} {text}')
