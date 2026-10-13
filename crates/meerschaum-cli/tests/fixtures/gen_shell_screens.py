#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Record v4.1.4's interactive shell for the Rust shell's parity tests.

Run with the reference harness (never with the working tree):

    scripts/parity/run.sh crates/meerschaum-cli/tests/fixtures/gen_shell_screens.py

Writes two fixtures next to this file:

- `shell_completions.json`: `ShellCompleter.get_completions()` (text, start position, display)
  for a table of inputs, recorded in-process against a temporary root with two stopped
  synthetic jobs and two SQLite connectors.
- `shell_screens.json`: screen snapshots of `python -m meerschaum` driven through a
  pseudo-terminal with `pexpect` and rendered with `pyte`, for scripted keystroke scenarios
  (startup with the bottom toolbar, the completion menu while typing, Tab cycling, accepting,
  auto-suggest from history, `instance`, Ctrl-C, Ctrl-D, resize, Ctrl-R, Ctrl-L, long output,
  no-ANSI, the update message, `daemon`).

Isolation: every process runs with `MRSM_ROOT_DIR` and `HOME` in a fresh temporary directory,
an empty temporary `MRSM_PLUGINS_DIR`, SQLite instances only (`sql:local`, `sql:x`; `sql:main`
is pointed at a temporary SQLite file too so nothing can reach a server), no update checks
(`shell.updates.check_remote: false`), and no actions that mutate anything. Every spawned
process is killed. Recorded screens contain no paths, pids or hostnames; the version string
is replaced by `{version}`.
"""

import json
import os
import pathlib
import shutil
import sys
import tempfile
import time

HERE = pathlib.Path(__file__).resolve().parent
TMP_BASE = pathlib.Path(tempfile.mkdtemp(prefix='mrsm-shell-gen-')).resolve()
COLS, ROWS = 100, 30
JOB_NAMES = ['alpha-job', 'beta-job']
HISTORY = [
    'show pipes -i sql:x',
    'notanaction foo bar',
    'sync pipes --loop --min-seconds 5',
    'show version',
]


def assert_temporary(path: pathlib.Path) -> None:
    """Refuse to run against anything outside the system temporary directory."""
    tmp = pathlib.Path(tempfile.gettempdir()).resolve()
    resolved = pathlib.Path(path).resolve()
    if tmp not in resolved.parents:
        sys.exit(f"gen_shell_screens: refusing to use non-temporary root {resolved}")


def make_env(name: str, extra_config=None) -> dict:
    """A scrubbed environment with a fresh temporary root, home and empty plugins directory."""
    base = TMP_BASE / name
    root, home, plugins = base / 'root', base / 'home', base / 'plugins'
    for p in (root, home, plugins):
        p.mkdir(parents=True, exist_ok=True)
    assert_temporary(root)
    assert_temporary(plugins)
    config = {
        'meerschaum': {'instance': 'sql:local'},
        'shell': {'updates': {'check_remote': False}},
    }
    for key, val in (extra_config or {}).items():
        config.setdefault(key, {}).update(val)
    env = {
        'PATH': os.path.dirname(sys.executable) + ':/usr/bin:/bin',
        'HOME': str(home),
        'TERM': 'xterm-256color',
        'LANG': 'C.UTF-8',
        'LC_ALL': 'C.UTF-8',
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_CONFIG': json.dumps(config),
        'MRSM_SQL_MAIN': 'sqlite:///' + str(base / 'main.db'),
        'MRSM_SQL_X': 'sqlite:///' + str(base / 'x.db'),
    }
    if os.environ.get('MRSM_VENVS_DIR'):
        env['MRSM_VENVS_DIR'] = os.environ['MRSM_VENVS_DIR']
    return env


###############################################################################
# Completions (in-process).
###############################################################################

COMPLETION_INPUTS = [
    '', ' ', 's', 'sh', 'sho', 'show', 'show ', 'show p', 'show pi', 'show pipes', 'show pipes ',
    'show pipes -', 'show pipes --', 'show pipes --i', 'show pipes --ins', 'show pipes -i',
    'show pipes -i ', 'show pipes -i sql', 'show pipes -i sql:', 'show pipes -i sql:l',
    'show pipes -i sql:x', 'show pipes -i sql:x ', 'show pipes -c ', 'show pipes -c s',
    'show pipes -r ', 'show pipes --mrsm-instance ', 'show pipes --instance sql:',
    'show pipes --dtypes ', 'show pipes --dtypes d', 'show pipes --dtypes _',
    'copy pipes --sync-data ', 'copy pipes --sync-data b', 'copy pipes --sync-data all',
    'copy pipes --sync-data all ', 'show pipes --nopretty', 'show pipes --nopretty ',
    'sync pipes --', 'sync pipes --l', 'sync pipes --loop', 'sync pipes --loop ',
    'sync pipes --loop --mi', 'sync', 'sync ', 'sync p', 'sync pipes', 'sync pipes + sh',
    'sync pipes + show p', 'show pipes : --lo', 'show pipes : -',
    'instance', 'instance ', 'instance s', 'instance sql', 'instance sql:', 'instance sql:x',
    'instance api', 'repo', 'repo ', 'repo a', 'executor', 'executor ', 'executor l',
    'executor s', 'debug', 'debug ', 'daemon', 'daemon ', 'help', 'help ', 'help s', 'help sh',
    'help show', 'help show ', 'help show p', 'help sync p', 'help foo', '?', '? ', '?sh',
    'show config', 'show config ', 'show config s', 'show config sys', 'show config system',
    'show config system ', 'edit config ', 'edit config m', 'delete config ', 'delete config s',
    'show connectors ', 'show connectors s', 'show connectors sql:', 'show connector sql:l',
    'show connectors sql:local', 'show packages ', 'show packages p', 'show jobs ',
    'show jobs a', 'show logs ', 'show job alpha-job ', 'show logs b',
    'show jobs alpha-job', 'compose', 'compose ', 'compose ex', 'compose up', 'start ',
    'start j', 'start job ', 'start jobs a', 'start jobs alpha-job ', 'start connectors ',
    'start connectors s', 'stop ', 'stop jobs ', 'stop jobs a', 'pause ', 'pause jobs ',
    'restart ', 'restart jobs ', 'attach ', 'attach job ', 'attach logs a', 'delete ',
    'delete jobs ', 'delete jobs a', 'delete jobs alpha-job ', 'delete connectors ',
    'delete connectors s', 'delete cache ', 'delete cache s', 'clear ', 'clear cache ',
    'clear cache sql:x', 'clear cache sql:x ', 'copy ', 'copy c', 'copy connectors ',
    'copy connectors sql:l', 'copy pipes ', 'copy pipes s', 'copy pipes sql:x',
    'copy pipes sql:x ', 'copy pipes sql:x s', 'copy pipes -c sql:x ', 'install ', 'install p',
    'install plugins ', 'install plugins n', 'install packages ', 'install required ',
    'uninstall ', 'uninstall plugins ', 'register ', 'register plugins ', 'register connectors ',
    'register connectors s', 'register pipes ', 'login ', 'login a', 'setup ', 'setup plugins ',
    'sql ', 'sql l', 'sql x', 'sql local', 'edit ', 'edit p', 'edit plugins ', 'edit jobs ',
    'edit pipes ', 'os', 'os ', 'sh ', 'sh l', 'foo', 'foo ', 'foo b', 'mrsm', 'mrsm ',
    'mrsm sh', 'mrsm show p', 'bootstrap ', 'bootstrap p', 'verify ', 'verify p', 'drop ',
    'drop p', 'tag ', 'tag p', 'index ', 'deduplicate ', 'vacuum ', 'analyze ', 'compress ',
    'decompress ', 'partition ', 'reload', 'reload ', 'upgrade ', 'upgrade p', 'stack ',
    'api ', 'api s', 'python ', 'show', 'exit', 'ex', 'q', 'pass', 'show pipes -c sql:x -m ',
    'show pipes -t', 'show pipes --tags ', 'show pipes --params ', 'show pipes -d',
    'show pipes --debug ', 'show pipes --debug --', 'show pipes -i sql:x + show p',
    'show actions', 'show actions ', 'show a', 'show c', 'show j', 'show l', 'show v',
    'show version ', 'sync pipes -c sql:x', 'start api ', 'start api --', 'start webterm ',
    'delete pipes ', 'delete p', 'show pipes -x', 'show pipes -xyz ', 'show pipes - ',
    'show pipes -- ', 'show pipes ---', 'show pipes --loop-', 'show pipes -c sql:x -c ',
]


def record_completions() -> dict:
    env = make_env('completions')
    # run.sh exports this; v4 would read it as an environment connector (`parity:reference`).
    reference = os.environ.pop('MRSM_PARITY_REFERENCE', None)
    os.environ.update(env)
    os.chdir(TMP_BASE)
    import meerschaum as mrsm
    import meerschaum.config.paths as paths
    from meerschaum.jobs import Job, get_jobs

    if reference and mrsm.__version__ != reference:
        sys.exit("gen_shell_screens: not the reference meerschaum")
    assert_temporary(paths.ROOT_DIR_PATH)

    for name in JOB_NAMES:
        job = Job(name, ['show', 'version'], executor_keys='local')
        job.daemon.write_properties()
        job.daemon.write_pickle()
    assert sorted(get_jobs('local', include_hidden=False)) == JOB_NAMES, get_jobs('local')

    from meerschaum.actions import get_shell
    from meerschaum._internal.shell.ShellCompleter import ShellCompleter
    from prompt_toolkit.document import Document
    from prompt_toolkit.formatted_text import to_formatted_text

    _ = get_shell()
    completer = ShellCompleter()
    cases = []
    for text in COMPLETION_INPUTS:
        completions = []
        for c in completer.get_completions(Document(text), None):
            completions.append({
                'text': c.text,
                'start_position': c.start_position,
                'display': c.display_text,
                'display_styles': [style for style, _ in to_formatted_text(c.display)],
            })
        cases.append({'input': text, 'completions': completions})

    return {
        'generator': 'gen_shell_screens.py',
        'reference': '{version}',
        'setup': {
            'instance': 'sql:local',
            'env_connectors': ['sql:main', 'sql:x'],
            'jobs': [{'name': n, 'sysargs': ['show', 'version'], 'status': 'stopped'} for n in JOB_NAMES],
            'plugins': [],
        },
        'cases': cases,
    }


###############################################################################
# Screens (pty).
###############################################################################

CTRL_C, CTRL_D, CTRL_L, CTRL_R = '\x03', '\x04', '\x0c', '\x12'
TAB, SHIFT_TAB, ENTER, RIGHT, END = '\t', '\x1b[Z', '\r', '\x1b[C', '\x1b[F'

SCENARIOS = [
    {'name': 'startup', 'steps': [{'label': 'startup'}]},
    {'name': 'menu_actions', 'steps': [
        {'label': 'startup'},
        {'type': 'sh', 'label': 'typed sh'},
        {'send': TAB, 'label': 'tab 1'},
        {'send': TAB, 'label': 'tab 2'},
        {'send': SHIFT_TAB, 'label': 'shift-tab'},
        {'send': SHIFT_TAB, 'label': 'shift-tab back to original'},
        {'send': '\x1b[B', 'label': 'down'},
        {'send': '\x1b[B', 'label': 'down 2'},
        {'send': '\x1b[A', 'label': 'up'},
        {'type': 'o', 'label': 'typed o after selection'},
        {'send': '\x7f', 'label': 'backspace'},
        {'send': CTRL_C, 'label': 'ctrl-c'},
    ]},
    {'name': 'menu_subactions', 'steps': [
        {'label': 'startup'},
        {'type': 'show p', 'label': 'typed show p'},
        {'type': 'i', 'label': 'typed show pi'},
        {'send': TAB, 'label': 'tab'},
        {'send': ENTER, 'label': 'enter'},
    ]},
    {'name': 'menu_space', 'steps': [
        {'label': 'startup'},
        {'type': 'show ', 'label': 'typed show space'},
        {'send': TAB, 'label': 'tab'},
        {'type': 'pipes ', 'label': 'typed pipes space (no menu)'},
        {'send': TAB, 'label': 'tab without menu'},
        {'send': CTRL_C, 'label': 'ctrl-c'},
        {'type': 'sho', 'label': 'typed sho'},
        {'send': TAB, 'label': 'tab single'},
    ]},
    {'name': 'menu_flags', 'steps': [
        {'label': 'startup'},
        {'type': 'sync pipes --', 'label': 'typed sync pipes --'},
        {'send': TAB, 'label': 'tab'},
        {'send': TAB, 'label': 'tab 2'},
    ]},
    {'name': 'menu_choices', 'steps': [
        {'label': 'startup'},
        {'type': 'copy pipes --sync-data ', 'label': 'typed flag with choices'},
        {'type': 'b', 'label': 'typed b'},
    ]},
    {'name': 'autosuggest', 'history': HISTORY, 'steps': [
        {'label': 'startup'},
        {'type': 'sho', 'label': 'typed sho'},
        {'send': RIGHT, 'label': 'right accepts'},
        {'send': CTRL_C, 'label': 'ctrl-c'},
        {'type': 'no', 'label': 'typed no (invalid history line)'},
        {'send': CTRL_C, 'label': 'ctrl-c 2'},
        {'type': 'sync pipes --loop', 'label': 'typed sync prefix'},
        {'send': END, 'label': 'end accepts'},
    ]},
    {'name': 'instance', 'steps': [
        {'label': 'startup'},
        {'type': 'instance sql:x', 'label': 'typed'},
        {'send': ENTER, 'label': 'switched'},
        {'type': 'instance', 'label': 'typed reset'},
        {'send': ENTER, 'label': 'reset'},
    ]},
    {'name': 'ctrl_c', 'steps': [
        {'label': 'startup'},
        {'type': 'show pip', 'label': 'typed'},
        {'send': CTRL_C, 'label': 'ctrl-c'},
    ]},
    {'name': 'ctrl_d', 'steps': [
        {'label': 'startup'},
        {'send': CTRL_D, 'label': 'ctrl-d', 'expect_exit': True},
    ]},
    {'name': 'exit_command', 'steps': [
        {'label': 'startup'},
        {'type': 'exit', 'label': 'typed'},
        {'send': ENTER, 'label': 'exit', 'expect_exit': True},
    ]},
    {'name': 'resize', 'steps': [
        {'label': 'startup'},
        {'resize': [20, 60], 'label': 'resized 60x20'},
        {'type': 'sh', 'label': 'typed sh'},
        {'resize': [30, 100], 'label': 'resized back'},
    ]},
    {'name': 'tiny', 'size': [8, 40], 'steps': [
        {'label': 'startup'},
        {'type': 'sh', 'label': 'typed sh'},
    ]},
    {'name': 'long_output', 'steps': [
        {'label': 'startup'},
        {'type': 'help', 'label': 'typed'},
        {'send': ENTER, 'label': 'after help'},
        {'type': 'sh seq 1 60', 'label': 'typed seq'},
        {'send': ENTER, 'label': 'after 60 lines'},
        {'type': 'sh', 'label': 'typed sh at bottom'},
        {'send': TAB, 'label': 'tab at bottom'},
        {'send': CTRL_C, 'label': 'ctrl-c at bottom'},
    ]},
    {'name': 'menu_scroll', 'steps': [
        {'label': 'startup'},
        {'type': 'show ', 'label': 'typed show space'},
        {'send': TAB * 17, 'label': 'tab x17'},
        {'send': TAB * 7, 'label': 'tab x24 (last)'},
        {'send': TAB, 'label': 'tab wraps to original'},
        {'send': SHIFT_TAB, 'label': 'shift-tab to last'},
    ]},
    {'name': 'history_nav', 'history': HISTORY, 'steps': [
        {'label': 'startup'},
        {'send': '\x1b[A', 'label': 'up'},
        {'send': '\x1b[A', 'label': 'up 2'},
        {'send': '\x1b[B', 'label': 'down'},
        {'send': '\x1b[B', 'label': 'down 2'},
        {'send': '\x1b[B', 'label': 'down 3 (back to empty)'},
        {'type': 'sy', 'label': 'typed sy'},
        {'send': '\x1b[A', 'label': 'up with prefix'},
    ]},
    {'name': 'clear', 'steps': [
        {'label': 'startup'},
        {'type': 'sh', 'label': 'typed'},
        {'send': CTRL_L, 'label': 'ctrl-l'},
    ]},
    {'name': 'history_search', 'history': HISTORY, 'steps': [
        {'label': 'startup'},
        {'send': CTRL_R, 'label': 'ctrl-r'},
        {'type': 'sync', 'label': 'typed sync'},
        {'send': ENTER, 'label': 'enter accepts match'},
    ]},
    {'name': 'backslash', 'steps': [
        {'label': 'startup'},
        {'type': 'show \\', 'label': 'typed'},
        {'send': ENTER, 'label': 'enter'},
    ]},
    {'name': 'daemon', 'steps': [
        {'label': 'startup'},
        {'type': 'daemon', 'label': 'typed'},
        {'send': ENTER, 'label': 'daemon on'},
        {'type': 'daemon', 'label': 'typed again'},
        {'send': ENTER, 'label': 'daemon off'},
    ]},
    {'name': 'no_ansi', 'config': {'formatting': {'ansi': False}}, 'steps': [
        {'label': 'startup'},
        {'type': 'sh', 'label': 'typed sh'},
    ]},
    {'name': 'update_message', 'updates_cache': {'remote_version': '99.0.0'}, 'steps': [
        {'label': 'startup'},
    ]},
    {'name': 'toolbar_disabled', 'config': {'shell': {'bottom_toolbar': {'enabled': False}}}, 'steps': [
        {'label': 'startup'},
    ]},
]


def snapshot(screen) -> dict:
    """Text lines, cursor and the non-default style runs of a pyte screen."""
    lines, styles = [], []
    for y in range(screen.lines):
        row = screen.buffer[y]
        text = ''.join(row[x].data for x in range(screen.columns))
        lines.append(text.rstrip())
        runs, cur = [], None
        for x in range(screen.columns):
            c = row[x]
            key = (c.fg, c.bg, c.bold, c.reverse, c.underscore)
            if key == ('default', 'default', False, False, False):
                cur = None
                continue
            if cur is not None and cur[4] == key and cur[1] == x:
                cur[1] = x + 1
                continue
            cur = [x, x + 1, c.fg, c.bg, key]
            runs.append(cur)
        styles.append([
            [r[0], r[1], r[2], r[3], ''.join(f for f, on in zip('BRU', r[4][2:]) if on)]
            for r in runs
        ])
    return {
        'lines': lines,
        'cursor': [screen.cursor.y, screen.cursor.x],
        'styles': styles,
    }


def scrub(obj, version):
    if isinstance(obj, str):
        return obj.replace(version, '{version}')
    if isinstance(obj, list):
        return [scrub(o, version) for o in obj]
    if isinstance(obj, dict):
        return {k: scrub(v, version) for k, v in obj.items()}
    return obj


def run_scenario(scenario: dict, rel_paths: dict) -> dict:
    import pexpect
    import pyte

    rows, cols = scenario.get('size', [ROWS, COLS])
    env = make_env('screen-' + scenario['name'], scenario.get('config'))
    root = pathlib.Path(env['MRSM_ROOT_DIR'])
    if scenario.get('history'):
        hist = root / rel_paths['SHELL_HISTORY_PATH']
        hist.parent.mkdir(parents=True, exist_ok=True)
        with open(hist, 'w', encoding='utf-8') as f:
            for i, line in enumerate(scenario['history']):
                f.write(f"\n# 2026-01-0{1 + i % 9} 00:00:00.000000\n+{line}\n")
    if scenario.get('updates_cache'):
        cache = root / rel_paths['UPDATES_CACHE_PATH']
        cache.parent.mkdir(parents=True, exist_ok=True)
        with open(cache, 'w', encoding='utf-8') as f:
            json.dump({'last_check_ts': '2026-01-01T00:00:00+00:00', **scenario['updates_cache']}, f)

    screen = pyte.Screen(cols, rows)
    stream = pyte.ByteStream(screen)
    child = pexpect.spawn(
        sys.executable, ['-s', '-E', '-m', 'meerschaum'],
        env=env, cwd=str(TMP_BASE), dimensions=(rows, cols), encoding=None, timeout=5,
    )
    screen.write_process_input = lambda data: child.send(data.encode('utf-8'))

    def pump(idle: float = 0.8, limit: float = 25.0) -> bool:
        """Feed output to the screen until it is idle; return whether the child exited."""
        start = last = time.time()
        while True:
            try:
                data = child.read_nonblocking(65536, timeout=0.05)
                if data:
                    stream.feed(data)
                    last = time.time()
            except pexpect.TIMEOUT:
                pass
            except pexpect.EOF:
                return True
            now = time.time()
            if now - last >= idle or now - start >= limit:
                return not child.isalive()

    snaps = []
    try:
        exited = pump(idle=2.0)
        for step in scenario['steps']:
            if 'resize' in step:
                r, c = step['resize']
                screen.resize(r, c)
                child.setwinsize(r, c)
            if 'send' in step:
                child.send(step['send'].encode('utf-8'))
            if 'type' in step:
                for ch in step['type']:
                    child.send(ch.encode('utf-8'))
                    pump(idle=0.15, limit=3.0)
            exited = pump(idle=1.0)
            if step.get('expect_exit'):
                child.expect(pexpect.EOF, timeout=10)
                pump(idle=0.2, limit=1.0)
                exited = True
            snap = snapshot(screen)
            snap['label'] = step['label']
            snap['exited'] = bool(exited)
            snaps.append(snap)
    finally:
        if child.isalive():
            child.terminate(force=True)
        child.close(force=True)
    return {
        'name': scenario['name'],
        'size': [rows, cols],
        'config': scenario.get('config') or {},
        'history': scenario.get('history') or [],
        'updates_cache': scenario.get('updates_cache') or {},
        'steps': [{k: v for k, v in s.items()} for s in scenario['steps']],
        'snapshots': snaps,
    }


def main():
    completions = record_completions()
    import meerschaum as mrsm
    import meerschaum.config.paths as paths
    version = mrsm.__version__
    rel_paths = {
        key: str(pathlib.Path(getattr(paths, key)).relative_to(paths.ROOT_DIR_PATH))
        for key in ('SHELL_HISTORY_PATH', 'UPDATES_CACHE_PATH', 'PLUGIN_UPDATES_CACHE_PATH')
    }
    only = set(sys.argv[1:]) or None
    screens = []
    for scenario in SCENARIOS:
        if only and scenario['name'] not in only:
            continue
        print(f"[gen_shell_screens] {scenario['name']}", file=sys.stderr)
        screens.append(run_scenario(scenario, rel_paths))

    if only and (HERE / 'shell_screens.json').exists():
        with open(HERE / 'shell_screens.json', 'r', encoding='utf-8') as f:
            existing = json.load(f)['scenarios']
        fresh = {s['name']: scrub(s, version) for s in screens}
        order = [s['name'] for s in SCENARIOS]
        merged = {s['name']: s for s in existing}
        merged.update(fresh)
        screens = [merged[n] for n in order if n in merged]
    with open(HERE / 'shell_completions.json', 'w', encoding='utf-8') as f:
        json.dump(scrub(completions, version), f, indent=1, ensure_ascii=False)
        f.write('\n')
    with open(HERE / 'shell_screens.json', 'w', encoding='utf-8') as f:
        json.dump(
            scrub({
                'generator': 'gen_shell_screens.py',
                'reference': version,
                'term': 'xterm-256color',
                'paths': rel_paths,
                'scenarios': screens,
            }, version),
            f, indent=1, ensure_ascii=False,
        )
        f.write('\n')


if __name__ == '__main__':
    try:
        main()
    finally:
        shutil.rmtree(TMP_BASE, ignore_errors=True)
