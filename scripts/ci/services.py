#!/usr/bin/env python3
"""Run an isolated pytest backend shard, or validate the complete collection partition.

Each run owns a new root and Compose project. Docker allocates database ports on loopback;
only that project's containers and volumes are removed. No legacy test stack is consulted.
"""

import argparse
import collections
import json
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import socket
import subprocess
import sys

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
from scripts.ci.shards import CATALOG, SHARDS

COMPOSE = REPO / 'tests' / 'docker-compose.yaml'
TARGET_PORTS = {
    'timescaledb': 5432, 'timescaledb_ha': 5432, 'postgis': 5432, 'postgresql': 5432,
    'mariadb': 3306, 'mysql': 3306, 'mssql': 1433, 'oracle': 1521, 'valkey': 6379,
}
FORBIDDEN_PORTS = {8000, 8998, 8989, 3000, 5432}
OWNER_FILE = '.ci-services.json'
PRESERVE = {
    'MRSM_TEST_BIN', 'MRSM_TEST_ARTIFACT_MANIFEST', 'MRSM_TEST_BIN_PROFILE',
    'MRSM_TEST_SKIP_INSTALL', 'MRSM_TEST_DUCKDB_LIB', 'MRSM_TEST_PYTHON',
    'MRSM_TEST_CURRENT_PYTHON', 'MRSM_TEST_CONSOLE', 'MRSM_TEST_CONSOLE_VENV_DIR',
}
PYTEST_IGNORES = ('portable/', 'test_root/', 'tests/data/', 'docs/', 'target/')


def clean_environment(source):
    return {
        key: value for key, value in source.items()
        if (not key.startswith('MRSM_') or key in PRESERVE)
        and key not in ('TMUX', 'TMUX_PANE')
    }


def free_api_port():
    for _ in range(30):
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        if port > 1024 and port not in FORBIDDEN_PORTS:
            return port
    raise RuntimeError('Could not allocate a high loopback API port')


def compose_command(project, *args):
    return ['docker', 'compose', '--project-name', project, '--file', str(COMPOSE), *args]


def project_name(shard, nonce):
    run = re.sub(r'[^a-z0-9-]', '-', os.environ.get('GITHUB_RUN_ID', 'local').lower())[:20]
    attempt = re.sub(r'[^a-z0-9-]', '-', os.environ.get('GITHUB_RUN_ATTEMPT', '1').lower())[:8]
    return f'mrsm-ci-{run}-{attempt}-{shard}-{nonce}'


def claim_root(root, shard):
    root = root.expanduser().resolve()
    if root == Path.home() or root == REPO or root in REPO.parents or root == Path('/'):
        raise ValueError(f'Refusing unsafe test root: {root}')
    root.mkdir(parents=True, exist_ok=True)
    if any(root.iterdir()):
        raise ValueError(f'Isolated test root must be new or empty: {root}')
    nonce = secrets.token_hex(8)
    state = {'root': str(root), 'shard': shard, 'nonce': nonce, 'active': False,
             'owned_project': False,
             'project': project_name(shard, nonce), 'ports': {}}
    with (root / OWNER_FILE).open('x') as stream:
        json.dump(state, stream, indent=2)
    return state


def owned_state(root):
    root = root.expanduser().resolve()
    state = json.loads((root / OWNER_FILE).read_text())
    project = state.get('project', '')
    nonce = state.get('nonce', '')
    if (state.get('root') != str(root) or state.get('shard') not in SHARDS
            or not re.fullmatch(r'[a-f0-9]{16}', nonce)
            or not re.fullmatch(r'mrsm-ci-[a-z0-9-]+-' + nonce, project)):
        raise ValueError(f'Refusing cleanup without a valid ownership record: {root}')
    return state


def environment(state, ports, source=None):
    env = clean_environment(os.environ if source is None else source)
    spec = SHARDS[state['shard']]
    env.update({
        'MRSM_ROOT_DIR': state['root'], 'MRSM_PLUGINS_DIR': str(REPO / 'tests' / 'plugins'),
        'MRSM_TEST_ISOLATED': '1', 'MRSM_TEST_SHARD': state['shard'],
        'MRSM_TEST_COMPOSE_PROJECT': state['project'],
        'MRSM_TEST_FLAVORS': ','.join(spec['flavors']),
        'MRSM_TEST_PORTS': json.dumps(ports), 'MRSM_TEST_API_PORT': str(ports['api']),
        'MRSM_NO_AUTO_INSTALL': '1', 'MRSM_NO_UPDATE_CHECK': '1',
        'MRSM_PYTHON': env.get('PYTHON_BIN', sys.executable),
        'PYTHONPATH': str(REPO),
    })
    for service, target in TARGET_PORTS.items():
        env[f'MRSM_TEST_{service.upper()}_PUBLISH'] = f'127.0.0.1::{target}'
    if 'postgresql' in ports:
        env['MRSM_SQL_TEST_POSTGRESQL'] = (
            f"postgresql://test:test1234@localhost:{ports['postgresql']}/testdb"
        )
    return env


def mapped_port(text):
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if len(lines) != 1 or not lines[0].startswith('127.0.0.1:'):
        raise ValueError(f'Expected one loopback Docker binding, got {text!r}')
    port = int(lines[0].rsplit(':', 1)[1])
    if not 1024 < port <= 65535 or port in FORBIDDEN_PORTS:
        raise ValueError(f'Refusing test service port {port}')
    return port


def checked(command, env, **kwargs):
    return subprocess.run(command, cwd=REPO, env=env, check=True, **kwargs)


def cleanup(root):
    root = root.expanduser().resolve()
    if not (root / OWNER_FILE).exists():
        return  # An always-cleanup step may run before this harness initialized anything.
    state = owned_state(root)
    env = clean_environment(os.environ)
    api_error = None
    if 'api' in SHARDS[state['shard']]['flavors'] and (root / 'jobs' / 'test_api').exists():
        # Also covers cancellation before test.sh's EXIT trap. The root and interpreter were
        # recorded before startup, so this can only address this run's API job and daemon.
        api_env = environment(state, state['ports'])
        try:
            python = state['python']
            checked([python, '-m', 'meerschaum', 'delete', 'jobs', 'test_api', '-f', '-y'],
                    api_env, timeout=60)
            checked([python, '-m', 'meerschaum', 'stop', 'daemon'], api_env, timeout=30)
        except (OSError, subprocess.SubprocessError) as error:
            api_error = error
    # The unique project name is the sole cleanup boundary. Never use a global prune/down.
    if state.get('owned_project') and SHARDS[state['shard']]['services']:
        checked(compose_command(state['project'], 'down', '--volumes', '--remove-orphans'), env)
        state['active'] = False
        (root / OWNER_FILE).write_text(json.dumps(state, indent=2) + '\n')
    if api_error:
        raise RuntimeError(f'Could not stop the owned API job in {root}: {api_error}')


def run(args):
    state = claim_root(args.root, args.shard)
    args.root = Path(state['root'])
    ports = {'api': free_api_port()}
    state['ports'] = ports
    (args.root / OWNER_FILE).write_text(json.dumps(state, indent=2) + '\n')
    env = environment(state, ports)
    python = env.get('PYTHON_BIN', sys.executable)
    state['python'] = str((REPO / python).absolute()) if '/' in python else shutil.which(python)
    if not state['python']:
        raise ValueError(f'Python interpreter does not exist: {python}')
    (args.root / OWNER_FILE).write_text(json.dumps(state, indent=2) + '\n')
    workers = args.workers or SHARDS[args.shard]['workers']
    if not 1 <= workers <= 16:
        raise ValueError('workers must be between 1 and 16')
    env['MRSM_TEST_WORKERS'] = str(workers)
    env['PYTEST_XDIST_AUTO_NUM_WORKERS'] = str(workers)
    services = SHARDS[args.shard]['services']
    child = None

    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f'received signal {signum}')

    previous = {sig: signal.signal(sig, interrupted) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        if services:
            # Refuse a project collision before starting or removing anything.
            existing = checked(compose_command(state['project'], 'ps', '--all', '--quiet'),
                               env, capture_output=True, text=True).stdout.strip()
            volumes = checked(['docker', 'volume', 'ls', '--quiet', '--filter',
                               'label=com.docker.compose.project=' + state['project']],
                              env, capture_output=True, text=True).stdout.strip()
            if existing or volumes:
                raise ValueError(f'Refusing existing Compose project {state["project"]}')
            state['active'] = True
            state['owned_project'] = True
            (args.root / OWNER_FILE).write_text(json.dumps(state, indent=2) + '\n')
            checked(compose_command(state['project'], 'up', '--detach', '--quiet-pull',
                                    '--wait', '--wait-timeout', '900', *services), env)
            for service in services:
                output = checked(compose_command(state['project'], 'port', service,
                                                str(TARGET_PORTS[service])),
                                 env, capture_output=True, text=True).stdout
                ports[service] = mapped_port(output)
            state['ports'] = ports
            (args.root / OWNER_FILE).write_text(json.dumps(state, indent=2) + '\n')
            env = environment(state, ports, env)
            env['MRSM_TEST_WORKERS'] = str(workers)
            env['PYTEST_XDIST_AUTO_NUM_WORKERS'] = str(workers)
        print(json.dumps({'project': state['project'], 'root': state['root'],
                          'shard': args.shard, 'workers': workers, 'ports': ports}), flush=True)
        forwarded = args.pytest_args[1:] if args.pytest_args[:1] == ['--'] else args.pytest_args
        child = subprocess.Popen([str(REPO / 'scripts' / 'test.sh'), *forwarded],
                                 cwd=REPO, env=env, start_new_session=True)
        return child.wait()
    finally:
        if child is not None and child.poll() is None:
            os.killpg(child.pid, signal.SIGTERM)
            try:
                child.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(child.pid, signal.SIGKILL)
                child.wait()
        try:
            cleanup(Path(state['root']))
        finally:
            for sig, handler in previous.items():
                signal.signal(sig, handler)


def collection(python, root, report, shard=None, pytest_args=()):
    root.mkdir(parents=True, exist_ok=False)
    env = clean_environment(os.environ)
    env.update(MRSM_ROOT_DIR=str(root), MRSM_PLUGINS_DIR=str(REPO / 'tests' / 'plugins'),
               MRSM_NO_AUTO_INSTALL='1', MRSM_TEST_FLAVORS=','.join(
                   CATALOG if shard is None else SHARDS[shard]['flavors']), PYTHONPATH=str(REPO))
    command = [python, '-m', 'pytest', '-p', 'scripts.ci.pytest_shard', '--collect-only', '-q',
               '-p', 'no:cacheprovider', '--mrsm-collection-report', str(report)]
    for ignore in PYTEST_IGNORES:
        command.append('--ignore=' + ignore)
    if shard:
        command.extend(['--mrsm-shard', shard])
    command.extend(pytest_args)
    # Reserve closed loopback endpoints during collection. Even a newly introduced import
    # that accidentally connects cannot reach an existing test or production database.
    sockets, ports = [], {}
    try:
        for name in (*TARGET_PORTS, 'api'):
            sock = socket.socket()
            sock.bind(('127.0.0.1', 0))
            sockets.append(sock)
            ports[name] = sock.getsockname()[1]
        env.update(MRSM_TEST_ISOLATED='1', MRSM_TEST_PORTS=json.dumps(ports),
                   MRSM_TEST_API_PORT=str(ports['api']))
        result = subprocess.run(command, env=env, cwd=REPO, capture_output=True, text=True)
    finally:
        for sock in sockets:
            sock.close()
    (report.with_suffix('.log')).write_text(result.stdout + result.stderr)
    if result.returncode:
        raise RuntimeError(f'Collection failed ({result.returncode}); see {report.with_suffix(".log")}')
    return json.loads(report.read_text())


def validate_partition(baseline, lanes):
    expected = {item['nodeid'] for item in baseline}
    if len(expected) != len(baseline):
        raise ValueError('Unsharded collection has duplicate node IDs')
    selected = [item['nodeid'] for lane, records in lanes.items()
                for item in records if item['shard'] == lane]
    counts = collections.Counter(selected)
    duplicate = sorted(node for node, count in counts.items() if count != 1)
    missing, extra = sorted(expected - counts.keys()), sorted(counts.keys() - expected)
    if duplicate or missing or extra:
        raise ValueError(f'Collection partition mismatch: duplicate={duplicate}, missing={missing}, extra={extra}')
    return {lane: sum(item['shard'] == lane for item in records) for lane, records in lanes.items()}


def validate(args):
    root = args.root.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=False)
    baseline = collection(args.python, root / 'full', root / 'full.json')
    lanes = {lane: collection(args.python, root / lane, root / f'{lane}.json', lane)
             for lane in SHARDS}
    counts = validate_partition(baseline, lanes)
    (root / 'partition.json').write_text(json.dumps({'total': len(baseline), 'shards': counts}, indent=2) + '\n')
    print(json.dumps({'total': len(baseline), 'shards': counts}, indent=2))
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    runner = sub.add_parser('run')
    runner.add_argument('--shard', choices=tuple(SHARDS), required=True)
    runner.add_argument('--root', type=Path, required=True)
    runner.add_argument('--workers', type=int)
    runner.add_argument('pytest_args', nargs=argparse.REMAINDER)
    validator = sub.add_parser('validate')
    validator.add_argument('--root', type=Path, required=True)
    validator.add_argument('--python', default=os.environ.get('PYTHON_BIN', sys.executable))
    cleaner = sub.add_parser('cleanup')
    cleaner.add_argument('--root', type=Path, required=True)
    owner = sub.add_parser('check')
    owner.add_argument('--root', type=Path, required=True)
    owner.add_argument('--project', required=True)
    owner.add_argument('--shard', choices=tuple(SHARDS), required=True)
    sub.add_parser('plan')
    args = parser.parse_args()
    try:
        if args.command == 'run':
            return run(args)
        if args.command == 'validate':
            return validate(args)
        if args.command == 'cleanup':
            cleanup(args.root)
            return 0
        if args.command == 'check':
            state = owned_state(args.root)
            if state['project'] != args.project or state['shard'] != args.shard:
                raise ValueError('Isolated root/project/shard ownership does not match')
            if state['ports'] != json.loads(os.environ['MRSM_TEST_PORTS']):
                raise ValueError('Isolated service ports differ from the ownership record')
            return 0
        print(json.dumps(SHARDS, indent=2))
        return 0
    except (ValueError, RuntimeError, subprocess.CalledProcessError) as error:
        print(f'services.py: {error}', file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == '__main__':
    raise SystemExit(main())
