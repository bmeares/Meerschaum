"""Stage 2: continue the v4 state with v5 and compare. Each check records PASS / FAIL / DIFF (needs classification)."""
import json, pathlib, re, shutil, subprocess, sys, time, difflib, os, signal
from common import *
from api_serve import Server
import venvstate



class Findings:
    def __init__(self, run):
        self.run = run
        self.items = []

    def add(self, area, name, status, detail=''):
        self.items.append({'area': area, 'name': name, 'status': status, 'detail': str(detail)[:1500]})
        print(f"[{status}] {area}: {name}" + (f" :: {str(detail)[:300]}" if status != 'PASS' and detail else ''), flush=True)
        (self.run.out / 'findings.json').write_text(json.dumps(self.items, indent=1))


def jdiff(a, b, p=''):
    out = []
    if isinstance(a, dict) and isinstance(b, dict):
        for k in sorted(set(a) | set(b)):
            if k not in a:
                out.append(f"only v5 {p}/{k}")
            elif k not in b:
                out.append(f"only v4 {p}/{k}")
            else:
                out += jdiff(a[k], b[k], p + '/' + k)
    elif a != b:
        out.append(f"DIFF {p}: {str(a)[:80]} != {str(b)[:80]}")
    return out


def config(run, F, flavor):
    for which in ('v4', 'v5'):
        r = run.py(which, HERE / 'dumpcfg.py', run.out / f'cfg_{which}.json')
        if r.returncode:
            F.add('config', f'dump under {which}', 'FAIL', r.stderr[-500:])
    d = jdiff(json.load(open(run.out / 'cfg_v4.json')), json.load(open(run.out / 'cfg_v5.json')))
    F.add('config', 'get_config() identical under v4 and v5 on the v4-written files (connectors, MRSM{} refs, env connectors incl. a postgresql:// URI label)', 'PASS' if not d else 'DIFF', d)
    # v5 writes (edit config through $VISUAL), v4 reads
    before = (run.root / 'config' / 'meerschaum.yaml').read_text()
    r = run.mrsm('v5', 'edit', 'config', 'meerschaum', extra_env={'VISUAL': str(HERE / 'editor.sh')}, timeout=60)
    after = (run.root / 'config' / 'meerschaum.yaml').read_text()
    added = [l for l in difflib.unified_diff(before.splitlines(), after.splitlines(), lineterm='', n=0) if l[:1] in '+-' and l[:3] not in ('+++', '---')]
    F.add('config', 'v5 `edit config` changes only the edited lines (YAML preserved)', 'PASS' if len(added) == 3 and all(l[0] == '+' for l in added) else 'FAIL', added)
    for which in ('v4', 'v5'):
        run.py(which, HERE / 'dumpcfg.py', run.out / f'cfg2_{which}.json')
    d = jdiff(json.load(open(run.out / 'cfg2_v4.json')), json.load(open(run.out / 'cfg2_v5.json')))
    F.add('config', 'after a v5 write, v4 reads the same config as v5', 'PASS' if not d else 'FAIL', d)
    ok = 'trn_edited' in json.load(open(run.out / 'cfg2_v4.json')).get('meerschaum', {}).get('connectors', {}).get('sql', {})
    F.add('config', 'v4 sees the connector v5 wrote', 'PASS' if ok else 'FAIL')


def snapshots(run, F, tag, ignore=()):
    outs = {}
    for which in ('v4', 'v5'):
        p = run.py(which, HERE / 'snapshot.py', run.work / 'spec_pipes.json', run.out / f'snap_{tag}_{which}.pkl', timeout=900)
        if p.returncode:
            F.add('pipes', f'snapshot {tag} under {which}', 'FAIL', p.stderr[-600:])
    r = run.py('v5', HERE / 'compare.py', run.out / f'snap_{tag}_v4.pkl', run.out / f'snap_{tag}_v5.pkl')
    lines = r.stdout.splitlines()
    nd = len([l for l in lines if l.startswith('DIFF')])
    ne = len([l for l in lines if l.startswith('EXPECTED')])
    detail = '\n'.join(l for l in lines if l.startswith(('DIFF', 'EXPECTED', '   ')))
    name = f'[{tag}] get_data()/dtypes/rowcount/sync_time/parameters/columns_types: v4 vs v5 on the same tables'
    if nd:
        F.add('pipes', name, 'DIFF', detail)
    elif ne:
        F.add('pipes', name + ' (only the dtype backends differ: v4.1.4 gives NumPy dtypes when polars is absent, v5 always the polars-path dtypes)',
              'EXPECTED', 'PARITY W17D2-006\n' + detail)
    else:
        F.add('pipes', name, 'PASS')


CLI_CMDS = [
    'show pipes -i sql:trn_sqlite', 'show columns -i sql:trn_sqlite', 'show rowcounts -i sql:trn_sqlite',
    'show data -i sql:trn_sqlite -m kitchen', 'show data -i sql:trn_sqlite -m static', 'show connectors', 'show plugins',
    'show users -i sql:trn_sqlite', 'show tokens -i sql:trn_sqlite', 'show jobs', 'show version', 'show environment',
]


def norm(s):
    s = strip_ansi(s)
    s = re.sub(r'[ \t]+$', '', s, flags=re.M)
    return s.strip('\n')


V4_ACTIVE_CONNECTORS = "{'api': {}, 'sql': {}, 'plugin': {}, 'valkey': {}}"  # v4.1.4, fresh CLI process


def _cells(line):
    return [x.strip() for x in line.strip().strip('│').split('│')]


def _shown(cell, full):
    """Whether a table cell displays `full`, whole or truncated with a trailing ellipsis."""
    if cell == full:
        return True
    shown = cell[:-1]
    return cell.endswith('…') and 8 <= len(shown) < len(full) and full.startswith(shown)


def _pipe_table(text):
    """`(before, header, rows, after)` of a `|`-delimited table, rejoining rows whose raw cell bytes broke
    the line. Splits on newlines only (`str.splitlines()` also breaks on other control characters) and keeps
    cell text minus padding spaces. `header` is None when the table cannot be parsed."""
    lines = text.split('\n')
    start = next((i for i, l in enumerate(lines) if l.startswith('|')), None)
    if start is None or start + 1 >= len(lines):
        return lines, None, [], []
    header = [c.strip(' ') for c in lines[start].strip(' ').strip('|').split('|')]
    width = len(header) + 2
    rows, current, i = [], None, start + 2
    while i < len(lines):
        line = lines[i]
        if current is not None and current.count('|') < width - 1:
            current += '\n' + line
        elif line.startswith('|'):
            if current is not None:
                rows.append(current)
            current = line
        else:
            break
        i += 1
    if current is not None:
        rows.append(current)
    cells = []
    for row in rows:
        parts = row.split('|')
        if len(parts) != width or parts[0] != '' or parts[-1].strip(' ') != '':
            return lines, None, [], []
        cells.append([c.strip(' ') for c in parts[1:-1]])
    return lines[:start + 2], header, cells, lines[i:]


def _separator(line, columns):
    """The alignment pattern of a markdown separator row (`|---:|:---|`), or None when malformed."""
    if not re.fullmatch(r'\|(?::?-+:?\|)+', line) or line.count('|') != columns + 1:
        return None
    return re.sub(r'-+', '-', line)


def _border(line):
    """The junction pattern of a box-table border line (`╭───┬──╮`), or None when malformed."""
    if not re.fullmatch(r'╭─+(?:┬─+)*╮|├─+(?:┼─+)*┤|╰─+(?:┴─+)*╯', line):
        return None
    return re.sub(r'─+', '─', line)


def _without_mrsm_python(text, python):
    """`text` with its single `'MRSM_PYTHON'` entry for exactly `python` replaced, else None."""
    pattern = r"(    'MRSM_PYTHON':[ \n])'" + re.escape(str(python)) + r"',\n"
    found = re.findall(pattern, text)
    if len(found) != 1:
        return None
    return re.sub(pattern, r"\1'<mrsm-python>',\n", text)


def expected_cli(c, ao, bo, outs, facts):
    """Exact, per-command classification of a known v4/v5 output difference against the harness's
    own fixture facts (`facts`: `v5_version`, `tokens` {label: {id, user, expiration}}).

    Returns `(status, detail)`, or `None` to keep the DIFF. Every branch requires every other byte of
    the output to be identical.
    """
    import ast
    w = c.split()
    la, lb = ao.split('\n'), bo.split('\n')
    if w == ['show', 'version']:
        v5 = facts.get('v5_version')
        if v5 and ao == ' 💬 Meerschaum v4.1.4' and bo == f' 💬 Meerschaum v{v5}':
            return 'PASS', ''
        return None
    if w == ['show', 'environment']:
        a, b = _without_mrsm_python(ao, V4_PY), _without_mrsm_python(bo, V5_PY)
        return ('PASS', '') if a is not None and a == b else None
    if w == ['show', 'plugins']:
        names = [l.strip() for l in lb if re.fullmatch(r'  [A-Za-z_][A-Za-z0-9_]*', l.rstrip())]
        if sorted(la) == sorted(lb) and names and names == sorted(names) and len(la) == len(lb):
            return 'EXPECTED', 'PARITY W17T-CLI-004 (W13I-008): v5 lists plugins sorted by name, v4 in file-system order'
        return None
    if len(la) != len(lb):
        return None
    differing = [i for i, (x, y) in enumerate(zip(la, lb)) if x != y]
    if w == ['show', 'connectors']:
        if len(differing) == 1 and la[differing[0]] == V4_ACTIVE_CONNECTORS and lb[differing[0]] == '{}':
            return 'EXPECTED', ('PARITY W17T-CLI-003: the active-connector registry of a fresh CLI; v4 prints its '
                                'pre-seeded empty type keys, v5 an empty dict\n' + V4_ACTIVE_CONNECTORS)
        return None
    if w[:2] == ['show', 'tokens']:
        tokens = facts.get('tokens') or {}
        def same_box(x, y):
            if x.startswith('│') and y.startswith('│'):
                return x.endswith('│') and y.endswith('│') and len(_cells(x)) == len(_cells(y)) == 5
            return _border(x) is not None and _border(x) == _border(y) and _border(x).count('─') == 5
        if not tokens or any(not same_box(la[i], lb[i]) for i in differing):
            return None
        rows_a = [(i, _cells(l)) for i, l in enumerate(la) if l.startswith('│')]
        rows_b = [(i, _cells(l)) for i, l in enumerate(lb) if l.startswith('│')]
        if [i for i, _ in rows_a] != [i for i, _ in rows_b] or not rows_a:
            return None
        if rows_a[0][1] != ['ID', 'Label', 'User', 'Expiration', 'Valid'] or rows_b[0][1] != rows_a[0][1]:
            return None
        seen = set()
        for (_, a), (_, b) in zip(rows_a[1:], rows_b[1:]):
            fact = tokens.get(a[1])
            if fact is None or a[1] != b[1] or a[1] in seen or a[4] != b[4]:
                return None
            seen.add(a[1])
            if not (_shown(a[0], fact['id']) and _shown(b[0], fact['id'])):
                return None
            if not fact.get('expiration') or not (_shown(a[3], fact['expiration']) and _shown(b[3], fact['expiration'])):
                return None
            if a[2] != '' or b[2] != fact['user']:
                return None
        if seen != set(tokens):
            return None
        return 'EXPECTED', 'PARITY W17T-CLI-002: v5 fills the User column with the token owner; v4 leaves it blank'
    if w[:2] == ['show', 'data'] and '-m' in w and w[w.index('-m') + 1:w.index('-m') + 2] == ['kitchen']:
        before_a, head_a, rows_a, after_a = _pipe_table(ao)
        before_b, head_b, rows_b, after_b = _pipe_table(bo)
        if head_a is None or head_b is None or head_a != head_b or 'by' not in head_a or 'id' not in head_a:
            return None
        if before_a[:-2] != before_b[:-2] or after_a != after_b or len(rows_a) != len(rows_b) or not rows_a:
            return None
        ### Column widths may differ (the `by` cells are wider in v5); the alignment syntax may not.
        sep = _separator(before_a[-1], len(head_a))
        if sep is None or sep != _separator(before_b[-1], len(head_b)):
            return None
        k, key = head_a.index('by'), head_a.index('id')
        for a, b in zip(rows_a, rows_b):
            if [x for j, x in enumerate(a) if j != k] != [x for j, x in enumerate(b) if j != k]:
                return None
            try:
                expected = bytes([int(a[key]) % 256, 1, 2])  # the fixture's value (data.kitchen_df)
            except ValueError:
                return None
            ### v4's tabulate prints the decoded bytes and strips surrounding whitespace bytes.
            if b[k] != repr(expected) or a[k] != expected.decode('ascii').strip():
                return None
        return 'EXPECTED', ('PARITY W17T-CLI-001: the `bytes` column `by`; v4 writes the decoded bytes (control '
                            'characters; leading/trailing whitespace bytes dropped) to the terminal, v5 their repr')
    return None


def cli_diffs(run, F, flavor='sqlite', cmds=CLI_CMDS):
    outs = {}
    v4_out = json.loads((run.work / 'v4_out.json').read_text()) if (run.work / 'v4_out.json').exists() else {}
    facts = {'v5_version': V5_VERSION, 'tokens': (v4_out.get('tokens') or {}).get(flavor) or {}}
    for c in cmds:
        c = c.replace('sql:trn_sqlite', f'sql:trn_{flavor}')
        w = c.split()
        a = run.mrsm('v4', *w, timeout=300)
        b = run.mrsm('v5', *w, timeout=300)
        ao, bo = norm(a.stdout + a.stderr), norm(b.stdout + b.stderr)
        # v4 prints package-install chatter on a cold venv; drop it
        ao = '\n'.join(l for l in ao.splitlines() if not re.match(r'^(Installing packages|Successfully installed|Failed to install|  - |Using CPython|Resolved|Prepared|Installed| \+ |Downloading| Downloaded|Audited|Checked)', l))
        speed = f"v4 {a.elapsed:.1f}s v5 {b.elapsed:.1f}s"
        outs[c] = (ao, bo)
        known = expected_cli(c, ao, bo, outs, facts) if a.returncode == b.returncode == 0 and ao != bo else None
        if ao == bo and a.returncode == b.returncode:
            F.add('cli', f'`{c}` output identical ({speed})', 'PASS')
        elif known is not None:
            F.add('cli', f'`{c}` output matches the recorded v4/v5 difference ({speed})', known[0], known[1])
        else:
            d = '\n'.join(list(difflib.unified_diff(ao.splitlines(), bo.splitlines(), 'v4', 'v5', lineterm='', n=0))[:30])
            F.add('cli', f'`{c}` output differs (rc {a.returncode}/{b.returncode}; {speed})', 'DIFF', d)


def continue_pipes(run, F, flavor):
    (run.work / 'spec_v5.json').write_text(json.dumps({'flavor': flavor}))
    p = run.py('v5', HERE / 'v5_continue.py', run.work / 'spec_v5.json', run.out / 'continue.json', extra_env={'TRN_FETCH_N': '10'}, timeout=900)
    (run.out / 'continue.log').write_text(p.stdout + p.stderr)
    for line in p.stdout.splitlines():
        m = re.match(r'(PASS|FAIL) (.*?)( :: (.*))?$', line)
        if m:
            F.add('pipes', m.group(2), m.group(1), m.group(4) or '')
    if p.returncode:
        F.add('pipes', 'v5_continue.py exit', 'FAIL', p.stderr[-800:])


def copy_verify_drop(run, F, flavor):
    inst = f'sql:trn_{flavor}'
    dst = 'sql:trn_second'
    r = run.mrsm('v5', 'copy', 'pipes', dst, '-i', inst, '-m', 'kitchen', 'defn', '--sync-data', 'all', '-f', '-y', '--noask', timeout=300)
    F.add('pipes', '`copy pipes` v4 instance -> second instance with data', 'PASS' if r.returncode == 0 and 'Copied 2 pipes' in strip_ansi(r.stdout) else 'FAIL', strip_ansi(r.stdout)[-300:])
    sel = ['-i', dst, '-c', f'plugin:trn_plug', '-m', 'second', '-y', '--noask']
    r = run.mrsm('v5', 'verify', 'pipes', *sel, timeout=300)
    F.add('pipes', '`verify pipes` on a v4-created plugin pipe', 'PASS' if r.returncode == 0 and '1 succeeded' in strip_ansi(r.stdout) else 'FAIL', strip_ansi(r.stdout)[-300:])
    csel = ['-i', dst, '-c', inst, '-m', 'kitchen', '-f', '-y', '--noask']
    for action, expect in (('clear', 'clearing 1 pipe'), ('drop', 'dropping 1 pipe'), ('delete', 'deleting 1 pipes')):
        r = run.mrsm('v5', action, 'pipes', *csel, timeout=300)
        F.add('pipes', f'`{action} pipes` on the copy', 'PASS' if r.returncode == 0 and expect in strip_ansi(r.stdout) else 'FAIL', strip_ansi(r.stdout)[-200:])
    r = run.mrsm('v4', 'show', 'pipes', '-i', dst, '-m', 'kitchen')
    F.add('pipes', 'v4 sees the copy deleted by v5', 'PASS' if 'kitchen' not in strip_ansi(r.stdout) else 'FAIL', strip_ansi(r.stdout)[-200:])


def pyapi(run, F):
    outs = {}
    for which in ('v4', 'v5'):
        p = run.py(which, HERE / 'pyapi_script.py', timeout=300, extra_env={'TRN_FETCH_N': '10'})
        t = p.stdout
        try:
            outs[which] = json.loads(t[t.index('{\n'):t.rindex('}') + 1])
        except Exception:
            F.add('python-api', f'v4-API script under {which}', 'FAIL', (p.stdout + p.stderr)[-600:]); return
    d = jdiff(outs['v4'], outs['v5'])
    exc = [k for k, v in outs['v5'].items() if isinstance(v, dict) and 'EXC' in v]
    F.add('python-api', f'v4-API script (get_pipes/get_connector/Pipe/get_data(params)/SQLConnector.read,exec,value,to_sql,engine/entry/sync unpack) identical results ({len(outs["v5"])} probes)', 'PASS' if not d else 'DIFF', d)
    if exc:
        F.add('python-api', 'probes that raised under v5', 'INFO', exc)


def plugins(run, F, flavor):
    inst = f'sql:trn_{flavor}'
    before = venvstate.snap(run.root)
    run.state['venvs_before_v5'] = before
    for form in (['trn_hello'], ['trn', 'hello']):
        out = {}
        for which in ('v4', 'v5'):
            r = run.mrsm(which, *form, '--trn-flag', 'foo', timeout=120)
            out[which] = (r.returncode, 'hello flag=foo' in strip_ansi(r.stdout + r.stderr))
        ok = out['v5'][1]
        F.add('plugins', f'action `{" ".join(form)} --trn-flag foo` (v4: {out["v4"][1]})', 'PASS' if ok == out['v4'][1] else 'FAIL',
              f'v4 ok={out["v4"][1]}; v5 ok={out["v5"][1]} rc={out["v5"][0]}')
    log = (run.work / 'hooks.log').read_text()
    F.add('plugins', 'pre/post_sync hooks fired under v5 for the plugin pipe', 'PASS' if f"post_sync {{'connector_keys': 'plugin:trn_plug'" in log and 'pre_sync' in log else 'FAIL')
    sp = [l for l in log.splitlines() if 'tabulate=' in l]
    F.add('plugins', 'v5 plugin fetch imports `required` packages from the v4-created plugin venv', 'PASS' if sp and f"{run.root}/venvs/trn_plug" in sp[-1] else 'FAIL', sp[-1] if sp else 'no fetch line')
    r = run.mrsm('v5', 'sync', 'pipes', '-i', inst, '-c', 'plugin:trn_plain', '-m', 'plain', timeout=120)
    t = strip_ansi(r.stdout + r.stderr)
    F.add('plugins', 'plugin with `required` + plain `import pandas` in fetch (v4 exposed the shared mrsm venv to plugins): sync from the CLI',
          'FAIL' if 'No module named' in t or r.returncode else 'PASS', t[-300:])
    r = run.mrsm('v5', 'show', 'plugins', timeout=60)
    F.add('plugins', '`show plugins` lists the plugin', 'PASS' if 'trn_plug' in strip_ansi(r.stdout) else 'FAIL')
    after = venvstate.snap(run.root)
    same = before.get('trn_plug') == after.get('trn_plug')
    F.add('plugins', 'plugin venv reused untouched by v5 (no reinstall)', 'PASS' if same else 'FAIL', f"before={before.get('trn_plug', {}).get('n')} after={after.get('trn_plug', {}).get('n')}")
    mrsm_before, mrsm_after = set(before.get('mrsm', {}).get('dist', [])), set(after.get('mrsm', {}).get('dist', []))
    F.add('plugins', 'shared `mrsm` venv: packages added/removed by v5 so far', 'PASS' if mrsm_before == mrsm_after else 'INFO',
          f"added={sorted(mrsm_after - mrsm_before)} removed={sorted(mrsm_before - mrsm_after)}")


def api(run, F, flavor):
    inst = f'sql:trn_{flavor}'
    res = {}
    for which in ('v4', 'v5'):
        env = run.env(v5=(which != 'v4'))
        argv = run.argv(which)
        if which == 'v4':
            # The v4 baseline server runs in the pinned oracle venv (a fresh PyPI install lacks a working
            # python-multipart/fastapi pairing: its `POST /login` answers 400). Same root, same instance.
            ref = os.environ.get('MRSM_TRANSITION_V4_API_PY') or str(REPO / '.venv-ref' / 'bin' / 'python')
            if not os.path.exists(ref):
                ref = str(V4_PY)
            argv = [ref, '-s', '-E', '-m', 'meerschaum']
            env['UV_PYTHON'] = ref
        try:
            with Server(argv, env, run.cwd, run.out / f'api_{which}.log', extra=['-i', inst]) as s:
                pr = subprocess.run([sys.executable, str(HERE / 'api_probe.py'), f'http://127.0.0.1:{s.port}', str(run.work / 'v4_out.json'), flavor],
                                    capture_output=True, text=True, timeout=300)
                res[which] = json.loads(pr.stdout)
                # cross clients: the OTHER version's client against this server
                other = 'v5' if which == 'v4' else 'v4'
                u = json.loads((run.work / 'v4_out.json').read_text())['users'][flavor]['admin']
                cr = run.py(other, HERE / 'api_client.py', '127.0.0.1', s.port, u['username'], u['password'], timeout=300)
                try:
                    res[which + '_client_' + other] = json.loads(cr.stdout[cr.stdout.index('{\n'):cr.stdout.rindex('}') + 1])
                except Exception:
                    res[which + '_client_' + other] = {'EXC': (cr.stdout + cr.stderr)[-500:]}
        except Exception as e:
            F.add('api', f'{which} server start/probe', 'FAIL', repr(e)[:500])
            return
    (run.out / 'api_probe.json').write_text(json.dumps(res, default=str))
    a, b = res['v4'], res['v5']
    F.add('api', f'route set (v4 {len(a["paths"])}, v5 {len(b["paths"])})', 'INFO',
          f"only v4: {sorted(set(a['paths']) - set(b['paths']))} ; only v5: {sorted(set(b['paths']) - set(a['paths']))}")
    for x, y in zip(a['rows'], b['rows']):
        if x['code'] == y['code']:
            F.add('api', f"{x['label']} -> {x['code']} (same as v4)", 'PASS')
        else:
            F.add('api', f"{x['label']}: v4 {x['code']} vs v5 {y['code']}", 'DIFF', f"v4: {x['text'][:150]} | v5: {y['text'][:150]}")
    ping = [r for r in b['rows'] if 'ping' in r['label']][0]
    F.add('api', '`@api_plugin` route served by the v5 API host (GET /trn/ping)', 'PASS' if ping['code'] == 200 else 'FAIL', ping['text'])
    for k in ('v4_client_v5', 'v5_client_v4'):
        d = res.get(k, {})
        bad = [n for n, v in d.items() if isinstance(v, dict) and 'EXC' in v] if 'EXC' not in d else ['all']
        F.add('api', f'{k.replace("_client_", " server driven by ")} client ({"ok" if not bad else bad})', 'PASS' if not bad else 'FAIL', json.dumps(d)[:600])


def jobs(run, F, flavor):
    tag = job_tag(flavor)
    def status(name):
        r = run.mrsm('v5', 'show', 'jobs', timeout=60)
        t = strip_ansi(r.stdout)
        i = t.find(name)
        return t[i:i + 700] if i >= 0 else ''
    t = strip_ansi(run.mrsm('v5', 'show', 'jobs').stdout)
    for n, st in ((f'trn-{tag}-runner', 'running'), (f'trn-{tag}-looper', 'stopped'), (f'trn-{tag}-finished', 'stopped')):
        seg = status(n)
        F.add('jobs', f'v5 `show jobs` lists the v4 job {n} as {st}', 'PASS' if st in seg.split('\n')[0] + seg[:300] else 'FAIL', seg[:200])
    r = run.mrsm('v5', 'show', 'logs', f'trn-{tag}-runner', '--nopretty', timeout=60)
    F.add('jobs', 'v5 reads the v4 running job log', 'PASS' if 'Sleeping' in strip_ansi(r.stdout) or 'Successfully synced' in strip_ansi(r.stdout) else 'FAIL', strip_ansi(r.stdout)[-200:])
    pidfile = run.root / 'jobs' / f'trn-{tag}-runner' / 'process.pid'
    pid = int(pidfile.read_text()) if pidfile.exists() else None
    r = run.mrsm('v5', 'stop', 'jobs', f'trn-{tag}-runner', timeout=60)
    time.sleep(3)
    alive = pid and pathlib.Path(f'/proc/{pid}').exists()
    F.add('jobs', 'v5 stops the v4 daemon (documented: the v4 job keeps running until v5 stops it; v5 manages it by pid file)', 'PASS' if not alive and r.returncode == 0 else 'FAIL', f'pid {pid} alive={alive}')
    r = run.mrsm('v5', 'start', 'jobs', f'trn-{tag}-looper', extra_env={'TRN_FETCH_N': '10'}, timeout=60)
    time.sleep(6)
    pidfile = run.root / 'jobs' / f'trn-{tag}-looper' / 'process.pid'
    pid = int(pidfile.read_text()) if pidfile.exists() else None
    cmd = pathlib.Path(f'/proc/{pid}/cmdline').read_bytes().replace(b'\0', b' ').decode()[:160] if pid and pathlib.Path(f'/proc/{pid}').exists() else ''
    F.add('jobs', 'v5 starts the stopped v4 job (supervisor + child)', 'PASS' if f'trn-{tag}-looper' in segs(status(f'trn-{tag}-looper')) or cmd else 'FAIL', f'pid {pid}: {cmd[:120]}')
    seg = status(f'trn-{tag}-looper')
    F.add('jobs', 'job shows running', 'PASS' if 'running' in seg[:400] else 'FAIL', seg[:200])
    v4seg = strip_ansi(run.mrsm('v4', 'show', 'jobs').stdout)
    i = v4seg.find(f'trn-{tag}-looper')
    F.add('jobs', 'v4 `show jobs` reads the v5-started job state', 'PASS' if 'running' in v4seg[i:i + 300] else 'FAIL', v4seg[i:i + 300])
    r = run.mrsm('v5', 'stop', 'jobs', f'trn-{tag}-looper', timeout=60)
    time.sleep(2)
    r = run.mrsm('v5', 'delete', 'jobs', '-f', f'trn-{tag}-looper', f'trn-{tag}-runner', f'trn-{tag}-finished', timeout=60)
    F.add('jobs', 'v5 deletes all three v4 jobs', 'PASS' if not any((run.root / 'jobs').glob(f'trn-{tag}-*')) else 'FAIL', strip_ansi(r.stdout)[-200:])
    left = pgrep_roots(run.root)
    F.add('jobs', 'no processes left under the run root', 'PASS' if not left else 'FAIL', left)


def segs(x):
    return x


def compose(run, F):
    proj = pathlib.Path(run.state['compose_dir'])
    rcs = {sub: run.state.get('v4_compose_rc_' + sub) for sub in ('init', 'up', 'ps')}
    F.add('compose', 'v4 `compose init` / `up` / `ps` succeeded (the baseline v5 continues)',
          'PASS' if all(rc == 0 for rc in rcs.values()) else 'FAIL',
          f"{rcs}\n" + run.state.get('v4_compose_up', '')[-400:])
    r = run.mrsm('v5', 'compose', 'ps', cwd=proj, timeout=120)
    t = strip_ansi(r.stdout)
    F.add('compose', '`compose ps` shows the v4-started project job running', 'PASS' if 'running' in t and 'PID' in t else 'FAIL', t[-400:])
    r4 = run.mrsm('v4', 'compose', 'explain', cwd=proj, timeout=120)
    r5 = run.mrsm('v5', 'compose', 'explain', cwd=proj, timeout=120)
    a, b = norm(r4.stdout + r4.stderr), norm(r5.stdout + r5.stderr)
    F.add('compose', '`compose explain` output vs v4', 'PASS' if a == b else 'DIFF', '\n'.join(list(difflib.unified_diff(a.splitlines(), b.splitlines(), 'v4', 'v5', lineterm='', n=0))[:25]))
    r = run.mrsm('v5', 'compose', 'up', '--dry', cwd=proj, timeout=120)
    F.add('compose', '`compose up --dry`', 'PASS' if r.returncode == 0 else 'FAIL', strip_ansi(r.stdout + r.stderr)[-300:])
    jobdir = proj / 'root' / 'jobs'
    pidfiles = list(jobdir.glob('*/process.pid'))
    pid = int(pidfiles[0].read_text()) if pidfiles else None
    r = run.mrsm('v5', 'compose', 'down', cwd=proj, timeout=120)
    time.sleep(3)
    alive = pid and pathlib.Path(f'/proc/{pid}').exists()
    F.add('compose', '`compose down` stops the job v4 started', 'PASS' if not alive else 'FAIL', f'pid {pid} alive={alive}; ' + strip_ansi(r.stdout)[-200:])
    r = run.mrsm('v5', 'compose', 'up', cwd=proj, timeout=300)
    time.sleep(5)
    t = strip_ansi(run.mrsm('v5', 'compose', 'ps', cwd=proj).stdout)
    F.add('compose', '`compose up` under v5 starts the project job', 'PASS' if 'running' in t else 'FAIL', t[-300:] + strip_ansi(r.stdout + r.stderr)[-300:])
    r = run.mrsm('v5', 'compose', 'down', cwd=proj, timeout=120)
    time.sleep(2)
    left = [l for l in pgrep_roots(proj / 'root', run.work)]
    F.add('compose', 'no processes left after `compose down`', 'PASS' if not left else 'FAIL', left)


# ---------------------------------------------------------------------------------------------------
# DDL through `mrsm sql`, env-URI connectors, engine state of a v4 root, a v4-style Dash plugin
# ---------------------------------------------------------------------------------------------------

def _q(flavor, name, schema=None):
    if flavor in ('mysql', 'mariadb'):
        return f'`{name}`'
    if flavor == 'oracle':
        return f'"{name}"'  # the connector's user owns the schema
    if flavor == 'mssql':
        return f'[{schema}].[{name}]' if schema else f'[{name}]'
    return f'"{schema}"."{name}"' if schema else f'"{name}"'


def _norm_table(s):
    # compare the printed table, ignoring v4's package-install chatter and trailing spaces
    keep = [l.rstrip() for l in s.splitlines() if l.strip().startswith(('|', '[', '{')) or 'row' in l]
    return '\n'.join(keep)


def ddl(run, F, flavor):
    """`mrsm sql <label> "<DDL>"` under v4 and v5: same outcomes, and the same objects exist afterwards."""
    from flavors import flavor_attrs
    schema = flavor_attrs(flavor, '').get('schema')
    label = f'trn_{flavor}'
    dual = ' FROM DUAL' if flavor == 'oracle' else ''
    view, t1, t2 = (_q(flavor, n, schema) for n in ('trn_ddl_view', 'trn_ddl_t1', 'trn_ddl_t2'))
    steps = [
        ('create view', f'CREATE VIEW {view} AS SELECT 1 AS a{dual}'),
        ('select from view', f'SELECT COUNT(*) AS n FROM {view}'),
        ('create the same view again (must fail)', f'CREATE VIEW {view} AS SELECT 2 AS a{dual}'),
        ('select from view again', f'SELECT * FROM {view}'),
        ('drop view', f'DROP VIEW {view}'),
        ('create table', f'CREATE TABLE {t1} (a INT)'),
        ('insert', f'INSERT INTO {t1} (a) VALUES (1)'),
        ('select table rows', f'SELECT COUNT(*) AS n FROM {t1}'),
        ('select table rows --nopretty', f'SELECT COUNT(*) AS n FROM {t1}', ['--nopretty']),
        ('multi-statement', f'CREATE TABLE {t2} (a INT); INSERT INTO {t2} (a) VALUES (5)'),
        ('select multi-statement table', f'SELECT * FROM {t2}'),
        ('drop tables', f'DROP TABLE {t1}'),
        ('drop second table', f'DROP TABLE {t2}'),
    ]
    probes = {'view': view, 't1': t1, 't2': t2}

    def exists():
        out = {}
        for k, n in probes.items():
            p = run.py('v4', HERE / 'ddl_probe.py', flavor, n)
            out[k] = p.stdout.strip().splitlines()[-1] if p.stdout.strip() else 'ERR'
        return out

    results = {}
    for which in ('v4', 'v5'):
        seq = []
        for name, q, *flags in steps:
            r = run.mrsm(which, 'sql', label, q, *(flags[0] if flags else []), timeout=120)
            t = strip_ansi(r.stdout + r.stderr)
            # v4 prints this warning for every statement that returns no rows, including successful DDL
            t_chk = re.sub(r'This\s+result\s+object\s+does\s+not\s+return\s+rows\.\s+It\s+has\s+been\s+closed\s+automatically\.', '', t)
            failed = bool(re.search(r'already exists|error|failed|💢|🛑', t_chk, re.I)) or (r.returncode != 0 and t_chk == t)
            seq.append({'step': name, 'failed': failed, 'exists': exists(), 'out': t.strip()[-500:]})
        results[which] = seq
    (run.out / 'ddl.json').write_text(json.dumps(results, indent=1))
    _ddl_classify(F, flavor, results)



def _ddl_classify(F, flavor, results):
    keys = ('view', 't1')  # t2 only exists where the multi-statement string ran (v4 refuses it on sqlite)
    for a, b in zip(results['v4'], results['v5']):
        same_objs = {k: a['exists'][k] for k in keys} == {k: b['exists'][k] for k in keys}
        label_ = f"`mrsm sql` {a['step']}: v4 failed={a['failed']} v5 failed={b['failed']}; view/t1 exist after: v4 {[a['exists'][k] for k in keys]} v5 {[b['exists'][k] for k in keys]}"
        detail = f"v4: {a['out']}\nv5: {b['out']}"
        worse = (b['failed'] and not a['failed']) or any(a['exists'][k] == 'yes' and b['exists'][k] == 'no' for k in keys)
        if a['failed'] == b['failed'] and same_objs:
            if a['step'].startswith('select') and not a['failed']:
                same_out = _norm_table(a['out']) == _norm_table(b['out'])
                F.add('ddl', label_ + ' -- output equal' if same_out else label_ + ' -- OUTPUT DIFFERS', 'PASS' if same_out else 'DIFF', f"v4: {a['out']}\nv5: {b['out']}")
            else:
                F.add('ddl', label_, 'PASS')
        elif not worse and (flavor in ('postgis', 'timescaledb', 'postgresql') and 'view' in a['step'] or 'ResourceClosedError' in a['out'] and not b['failed']):
            # v4 runs DDL through `read()`: it raises ResourceClosedError after the statement ran, and on PostgreSQL the
            # rollback drops the view; v5 keeps the object and prints `None` after the same message
            F.add('ddl', label_ + ' (v4 `read()` of DDL: ResourceClosedError and, on PostgreSQL, a rolled-back view; v5 keeps it)', 'EXPECTED', 'PARITY W17O-007a\n' + detail)
        elif not worse and a['step'] in ('multi-statement', 'select multi-statement table', 'drop second table'):
            # v4 refuses multi-statement strings through its sqlite3 / pymysql drivers; v5 executes user-supplied SQL as written
            F.add('ddl', label_ + ' (v5 executes a multi-statement string that v4 refuses)', 'EXPECTED', 'PARITY W17X-001\n' + detail)
        elif not worse:
            F.add('ddl', label_ + ' (v5 accepts what v4 rejects)', 'DIFF', detail)
        else:
            F.add('ddl', label_, 'FAIL', detail)


def ddl_classify(run, F, flavor):
    """Re-classify the stored results of a ddl run (`run_stage.py <run> ddl_classify <flavor>`)."""
    _ddl_classify(F, flavor, json.loads((run.out / 'ddl.json').read_text()))


def _tcp_open(port):
    import socket
    with socket.socket() as s:
        s.settimeout(2)
        return s.connect_ex(('127.0.0.1', port)) == 0


def envuri(run, F, flavor):
    """`MRSM_SQL_MAIN` and a custom label set to URIs of the test servers, `main`'s host patched to a dead port so
    that a client which ignores the URI fails harmlessly instead of reaching the default host.

    Needs the postgresql (5559) and timescaledb-ha (5459) test containers; skipped (INFO) when they are down. The
    instance tables the Rust pool and the facade create on the env label go to schema `trn` of the 5559 container
    (dropped afterwards), never to `public`."""
    if not (_tcp_open(5459) and _tcp_open(5559)):
        F.add('env-uri', 'skipped: the postgresql (5559) and timescaledb-ha (5459) test containers are not both reachable', 'INFO')
        return
    main_uri = 'postgresql://test:test1234@127.0.0.1:5459/testdb'
    other_uri = 'postgresql://test:test1234@127.0.0.1:5559/testdb'
    cfg = json.dumps({'meerschaum': {'instance': 'sql:trn', 'connectors': {'sql': {
        'main': {'host': '127.0.0.1', 'port': 59998},
        'trnpg2': {'schema': 'trn'},
    }}}})
    ee = {'MRSM_SQL_MAIN': main_uri, 'MRSM_SQL_TRNPG2': other_uri, 'MRSM_CONFIG': cfg}
    q = 'SELECT system_identifier AS id FROM pg_control_system()'
    got = {}
    run.py('v4', HERE / 'v4_admin.py', 'create', 'postgresql')
    public_before = run.py('v4', HERE / 'public_probe.py').stdout.strip()
    try:
        for label in ('main', 'trnpg2'):
            for which in ('v4', 'v5'):
                r = run.mrsm(which, 'sql', label, q, extra_env=ee, timeout=120)
                t = strip_ansi(r.stdout + r.stderr)
                m = re.search(r'\b(\d{15,})\b', t)
                got[(label, which)] = (m.group(1) if m else None, t.strip()[-200:])
            a, b = got[(label, 'v4')], got[(label, 'v5')]
            F.add('env-uri', f'`mrsm sql {label}` follows the MRSM_SQL_{label.upper()} URI like v4 (cluster id v4={a[0]} v5={b[0]})',
                  'PASS' if a[0] and a[0] == b[0] else 'FAIL', f"v4: {a[1]}\nv5: {b[1]}")
        F.add('env-uri', 'the two labels reach different servers', 'PASS' if got[('main', 'v5')][0] and got[('main', 'v5')][0] != got[('trnpg2', 'v5')][0] else 'FAIL')
        for which in ('v4', 'v5'):
            r = run.mrsm(which, 'show', 'pipes', '-i', 'sql:trnpg2', extra_env=ee, timeout=120)
            got[('pipes', which)] = norm(r.stdout + r.stderr)
        F.add('env-uri', '`show pipes -i sql:<env label>` (Rust pool) output equals v4', 'PASS' if got[('pipes', 'v4')] == got[('pipes', 'v5')] else 'DIFF',
              '\n'.join(list(difflib.unified_diff(got[('pipes', 'v4')].splitlines(), got[('pipes', 'v5')].splitlines(), 'v4', 'v5', lineterm='', n=0))[:15]))
        p = run.py('v5', HERE / 'envuri_probe.py', extra_env=ee, timeout=120)
        F.add('env-uri', 'façade `get_connector(...).value()` and `Pipe.get_rowcount()` on the env label', 'PASS' if p.returncode == 0 and 'OK' in p.stdout else 'FAIL', (p.stdout + p.stderr)[-300:])
        public_after = run.py('v4', HERE / 'public_probe.py').stdout.strip()
        F.add('env-uri', 'no `mrsm_*` table was created in `public` of the shared test container', 'PASS' if public_before == public_after else 'FAIL', f'before={public_before} after={public_after}')
    finally:
        run.py('v4', HERE / 'v4_cleanup.py', 'postgresql')


def engine_free(run, F, flavor):
    """A root created by v4 has no engine state: `sync pipes --parents` under v5 creates it. Where does it land?"""
    inst = f'sql:trn_{flavor}'
    r = run.mrsm('v5', 'sync', 'pipes', '-i', inst, '-c', inst, '-m', 'defn', '--parents', '-y', '--noask', timeout=300)
    t = strip_ansi(r.stdout + r.stderr)
    F.add('engine', '`sync pipes --parents` on a pipe whose implicit parent has no fetch source (written by `sync(df)` elsewhere)',
          'PASS' if r.returncode == 0 and 'succeeded' in t and '0 succeeded' not in t else 'INFO',
          'the parent has no fetch definition: it fails and its child is `upstream_failed`; v4 `sync pipes` of that parent fails the same way\n' + t[-300:])
    r = run.mrsm('v5', 'sync', 'pipes', '-i', inst, '-c', inst, '-m', 'plugchild', '--parents', '-y', '--noask', timeout=300, extra_env={'TRN_FETCH_N': '10'})
    t = strip_ansi(r.stdout + r.stderr)
    F.add('engine', '`sync pipes --parents` (parent = plugin fetch pipe, child = SQL definition) once on a v4-created root', 'PASS' if r.returncode == 0 and 'succeeded' in t and '0 succeeded' not in t else 'FAIL', t[-400:])
    p = run.py('v4', HERE / 'engine_probe.py', flavor)
    where = p.stdout.strip().splitlines()[-1] if p.stdout.strip() else p.stderr[-300:]
    if flavor in ('sqlite', 'mysql', 'mariadb'):
        # no schemas: the engine state sits beside the instance tables (where v4 keeps its temporary tables)
        ok = 'mrsm_engine' in where or 'trndb' in where
        what = 'engine state (`mrsm_engine_*`) sits beside the instance tables (schema-less flavor)'
    else:
        ok = '_mrsm_internal' in where and "'trn'" not in where
        what = 'engine state (`mrsm_engine_*`) lands in `_mrsm_internal`, not in the instance schema'
    F.add('engine', what, 'PASS' if ok else 'FAIL', where)
    r = run.mrsm('v4', 'show', 'pipes', '-i', inst, timeout=120)
    F.add('engine', 'v4 still lists the pipes after v5 created engine state', 'PASS' if r.returncode == 0 and 'plugchild' in strip_ansi(r.stdout) else 'FAIL', strip_ansi(r.stdout + r.stderr)[-200:])
    r = run.mrsm('v4', 'sync', 'pipes', '-i', inst, '-c', inst, '-m', 'plugchild', timeout=300, extra_env={'TRN_FETCH_N': '10'})
    F.add('engine', 'v4 can still sync the pipe after v5 created engine state', 'PASS' if r.returncode == 0 and 'succeeded' in strip_ansi(r.stdout) else 'FAIL', strip_ansi(r.stdout + r.stderr)[-200:])


def dash(run, F, flavor):
    """A v4-style plugin Dash app (`@dash_plugin` + `@web_page`) served by `mrsm start api`: v4 and v5 (D21, 5.0.0rc2) both at /dash/<page>; rc1's /dash/<plugin>/<page> redirects."""
    import urllib.request, urllib.error
    shutil.copy(HERE / 'plugin' / 'trn_dash.py', run.plugins / 'trn_dash.py')
    inst = f'sql:trn_{flavor}'
    payload = {'output': 'trn-out.children', 'outputs': {'id': 'trn-out', 'property': 'children'},
               'inputs': [{'id': 'trn-btn', 'property': 'n_clicks', 'value': 3}], 'changedPropIds': ['trn-btn.n_clicks']}

    def get(url, data=None, tries=45):
        last = None
        for _ in range(tries):
            try:
                req = urllib.request.Request(url, data=json.dumps(data).encode() if data else None, headers={'Content-Type': 'application/json'} if data else {})
                with urllib.request.urlopen(req, timeout=60) as r:
                    return r.status, r.read().decode('utf-8', 'replace'), r.geturl()
            except urllib.error.HTTPError as e:
                last = (e.code, e.read().decode('utf-8', 'replace'), url)
                if e.code not in (502, 503, 404):
                    return last
            except Exception as e:
                last = (-1, repr(e), url)
            time.sleep(2)
        return last
    res = {}
    for which, base_paths in (('v4', ('/dash/counts', '/dash/_dash-update-component')), ('v5', ('/dash/counts', '/dash/_dash-update-component'))):
        env = run.env(v5=(which != 'v4'))
        argv = run.argv(which)
        if which == 'v4':
            ref = os.environ.get('MRSM_TRANSITION_V4_API_PY') or str(REPO / '.venv-ref' / 'bin' / 'python')
            if not os.path.exists(ref):
                ref = str(V4_PY)
            argv = [ref, '-s', '-E', '-m', 'meerschaum']
            env['UV_PYTHON'] = ref
        try:
            with Server(argv, env, run.cwd, run.out / f'dash_{which}.log', extra=['-i', inst]) as s:
                u = f'http://127.0.0.1:{s.port}'
                page = get(u + base_paths[0])
                cb = get(u + base_paths[1], payload)
                old = get(u + '/dash/trn_dash/counts') if which == 'v5' else None
                res[which] = {'page': (page[0], page[1][:120]), 'cb': (cb[0], cb[1][:300]), 'old': (old[0], old[2]) if old else None}
        except Exception as e:
            res[which] = {'error': repr(e)[:400]}
    (run.out / 'dash.json').write_text(json.dumps(res, indent=1))
    for which in ('v4', 'v5'):
        r = res[which]
        if 'error' in r:
            F.add('dash', f'{which} server with the Dash plugin', 'FAIL', r['error']); continue
        F.add('dash', f'{which}: plugin page served (HTTP {r["page"][0]})', 'PASS' if r['page'][0] == 200 else 'FAIL', r['page'][1])
        ok = r['cb'][0] == 200 and 'clicked 3' in r['cb'][1]
        F.add('dash', f'{which}: plugin callback answers "clicked 3" (HTTP {r["cb"][0]})', 'PASS' if ok else 'FAIL', r['cb'][1])
    if res['v5'].get('old'):
        F.add('dash', "v5: rc1's /dash/<plugin>/<page> redirects to the page", 'PASS' if res['v5']['old'][0] == 200 and res['v5']['old'][1].split('?')[0].rstrip('/').endswith('/dash/counts') else 'FAIL', res['v5']['old'])
