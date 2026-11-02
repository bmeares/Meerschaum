#!/usr/bin/env python3
"""Run native account and copy regressions on owned databases after the Rust suite."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import sys
import time

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))
from scripts.ci.services import TARGET_PORTS, clean_environment, compose_command, mapped_port

SERVICES = ('postgresql', 'mysql', 'mariadb', 'valkey')
TESTS = {
    'instance_first_user': 'meerschaum-connector-sql',
    'instance_user_tokens': 'meerschaum-connector-sql',
    'instance_users': 'meerschaum-connector-sql',
    'instance_unique_keys': 'meerschaum-connector-sql',
    'instance_has_rows': 'meerschaum-connector-sql',
    'instance_pipe_data': 'meerschaum-connector-sql',
    'valkey_first_user': 'meerschaum-connector-valkey',
    'valkey_misc': 'meerschaum-connector-valkey',
    'pipeline_metadata_move': 'meerschaum-connector-valkey',
    'rc15_fail_closed': 'meerschaum-connector-valkey',
    'valkey_registration': 'meerschaum-connector-valkey',
    'w18_fail_fast': 'meerschaum-connector-valkey',
    'valkey_replay': 'meerschaum-connector-valkey',
}
FILTERS = {'instance_pipe_data': 'test_selected_columns_do_not_reintroduce_omitted_columns'}


def save_report(path, report):
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(report, indent=2) + '\n')
    temporary.replace(path)


def cleanup(root):
    root = root.expanduser().resolve()
    receipt = root / 'native-backends.json'
    if not receipt.exists():
        return
    report = json.loads(receipt.read_text())
    nonce = report.get('nonce', '')
    project = report.get('project', '')
    if (report.get('root') != str(root) or not re.fullmatch('[a-f0-9]{16}', nonce)
            or project != 'mrsm-native-' + nonce):
        raise ValueError('Refusing an unowned native backend project')
    if report.get('owned_project') and not report.get('cleaned_up'):
        command = compose_command(project) + ['--file', str(root / 'native-services.json'),
                                            'down', '--volumes', '--remove-orphans']
        run_child(command, native_environment(root, {}), root / 'services-down.log')
        report['cleaned_up'] = True
        save_report(receipt, report)


def select_tests(lines, repo=REPO):
    selected = {}
    for line in lines:
        try:
            row = json.loads(line)
        except ValueError:
            continue
        if row.get('reason') != 'compiler-artifact' or not row.get('executable'):
            continue
        target = row['target']
        name = target['name']
        if name not in TESTS or target['kind'] != ['test']:
            continue
        source = repo / 'crates' / TESTS[name] / 'tests' / (name + '.rs')
        # Other crates have test targets of the same name: only the intended source is selected.
        if Path(target['src_path']).resolve() != source.resolve():
            continue
        binary = Path(row['executable']).resolve()
        if not binary.is_relative_to((repo / 'target').resolve()):
            raise ValueError(f'Unexpected native test artifact: {name}')
        if name in selected and selected[name] != binary:
            raise ValueError(f'Ambiguous native test artifact: {name}')
        selected[name] = binary
    if set(selected) != set(TESTS):
        raise ValueError(f'Missing native tests: {sorted(set(TESTS) - set(selected))}')
    return selected


def native_environment(root, ports, source=None):
    env = clean_environment(os.environ if source is None else source)
    env.pop('CARGO_TARGET_DIR', None)
    for key in list(env):
        if key.startswith(('PG', 'REDIS', 'VALKEY')):
            env.pop(key)
    env.update(MRSM_ROOT_DIR=str(root / 'runtime'), MRSM_PLUGINS_DIR=str(root / 'plugins'),
               MRSM_NO_AUTO_INSTALL='1', MRSM_NO_UPDATE_CHECK='1', MRSM_TEST_ISOLATED='1')
    for service, target in TARGET_PORTS.items():
        env[f'MRSM_TEST_{service.upper()}_PUBLISH'] = f'127.0.0.1::{target}'
    if ports:
        checked = {name: mapped_port('127.0.0.1:' + str(ports[name])) for name in SERVICES}
        env.update({
            'MRSM_TEST_PG_URI': f"postgresql://test:test1234@127.0.0.1:{checked['postgresql']}/testdb",
            'MRSM_TEST_MYSQL_URI': f"mysql://test:test1234@127.0.0.1:{checked['mysql']}/testdb",
            'MRSM_TEST_MARIADB_URI': f"mariadb://test:test1234@127.0.0.1:{checked['mariadb']}/testdb",
            'MRSM_VALKEY_TEST_URL': f"redis://127.0.0.1:{checked['valkey']}/12",
            'MRSM_VALKEY_FIRST_USER_TEST_URL': f"redis://127.0.0.1:{checked['valkey']}/13",
        })
    return env


def run_child(command, env, log, timeout=900):
    with log.open('w') as stream:
        child = subprocess.Popen(command, cwd=REPO, env=env, stdout=stream,
                                 stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = child.wait(timeout=timeout)
        finally:
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait()
    if code:
        raise RuntimeError(f'Native backend command failed ({code}); see {log}')


def check_test_output(name, output):
    result = re.search(r'test result: ok\. (\d+) passed;', output)
    if result is None or int(result[1]) == 0:
        raise ValueError(f'No passing native tests were recorded: {name}')
    if TESTS.get(name) == 'meerschaum-connector-valkey' and 'SKIPPED' in output:
        raise ValueError(f'A required live Valkey check returned without running: {name}')
    if name.startswith('instance_') and re.search(r'Skipping (postgresql|mysql|mariadb):', output):
        raise ValueError(f'A required live SQL check returned without running: {name}')


def run(root, profile='test', focused_build=False):
    if subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=no'],
                               cwd=REPO, text=True).strip():
        raise ValueError('Freeze the source before qualifying native backend regressions')
    root = root.expanduser().resolve()
    if root == Path.home() or root == REPO or root in REPO.parents or root == Path('/'):
        raise ValueError(f'Unsafe native backend root: {root}')
    root.mkdir(parents=True, exist_ok=True)
    if any(root.iterdir()):
        raise ValueError(f'The native backend root must be empty: {root}')
    for name in ('runtime', 'plugins'):
        (root / name).mkdir()
    nonce = secrets.token_hex(8)
    project = 'mrsm-native-' + nonce
    receipt = root / 'native-backends.json'
    report = {'root': str(root), 'nonce': nonce, 'owned_project': False,
              'source_sha': subprocess.check_output(['git', 'rev-parse', 'HEAD'],
              cwd=REPO, text=True).strip(), 'project': project, 'ports': {},
              'build_mode': 'focused' if focused_build else 'workspace',
              'source_tree': subprocess.check_output(['git', 'rev-parse', 'HEAD^{tree}'],
              cwd=REPO, text=True).strip(), 'profile': profile,
              'services': list(SERVICES), 'tests': [], 'passed': False, 'cleaned_up': False}
    save_report(receipt, report)
    override = root / 'native-services.json'
    settings = {name: {'cpus': 2, 'mem_limit': '2g', 'labels': {'mrsm.native-run': nonce}}
                for name in SERVICES}
    settings['postgresql']['command'] = 'postgres -c max_connections=100 -c shared_buffers=128MB'
    settings['valkey']['mem_limit'] = '512m'
    override.write_text(json.dumps({'services': settings}) + '\n')
    def compose(*args):
        return compose_command(project) + ['--file', str(override), *args]
    env = native_environment(root, {})
    owned = False
    previous_handlers = {}

    def interrupted(signum, _frame):
        raise KeyboardInterrupt(f'received signal {signum}')

    for sig in (signal.SIGINT, signal.SIGTERM):
        previous_handlers[sig] = signal.signal(sig, interrupted)
    try:
        # Same workspace/features as ci.sh test: Cargo reuses the already built executables.
        build = root / 'native-test-artifacts.log'
        if focused_build:
            # Local iteration need not relink the whole workspace for these selected targets.
            logs = []
            for crate in sorted(set(TESTS.values())):
                command = ['cargo', 'test', '-p', crate, '--locked', '--no-run', '--message-format=json']
                for name, package in TESTS.items():
                    if package == crate:
                        command.extend(['--test', name])
                if profile != 'test':
                    command.extend(['--profile', profile])
                log = root / (crate + '-artifacts.log')
                run_child(command, env, log, timeout=1800)
                logs.append(log.read_text())
            build.write_text('\n'.join(logs))
        else:
            command = ['cargo', 'test', '--workspace', '--locked', '--no-run', '--message-format=json']
            if profile != 'test':
                command.extend(['--profile', profile])
            run_child(command, env, build, timeout=1800)
        binaries = select_tests(build.read_text().splitlines())
        containers = subprocess.check_output(compose('ps', '--all', '--quiet'),
                                             cwd=REPO, env=env, text=True).strip()
        volumes = subprocess.check_output(['docker', 'volume', 'ls', '--quiet', '--filter',
            'label=com.docker.compose.project=' + project], env=env, text=True).strip()
        if containers or volumes:
            raise ValueError(f'Refusing an existing Compose project: {project}')
        owned = True
        report['owned_project'] = True
        save_report(receipt, report)
        run_child(compose('up', '--detach', '--quiet-pull', *SERVICES),
                  env, root / 'services-up.log')
        ports = {}
        for service in SERVICES:
            output = subprocess.check_output(compose('port', service,
                str(TARGET_PORTS[service])), cwd=REPO, env=env, text=True)
            ports[service] = mapped_port(output)
        report['ports'] = ports
        commands = {
            'postgresql': ['pg_isready', '-h', '127.0.0.1', '-U', 'test', '-d', 'testdb'],
            # cytopia/mysql-5.7 initializes only root; MYSQL_USER / MYSQL_DATABASE are ignored.
            'mysql': ['mysql', '-h', '127.0.0.1', '-u', 'root', '-pmy-secret-pw', '-e', 'SELECT 1'],
            'mariadb': ['mariadb', '-h', '127.0.0.1', '-u', 'test', '-ptest1234', '-e', 'SELECT 1'],
            'valkey': ['valkey-cli', 'ping'],
        }
        for service, command in commands.items():
            deadline = time.monotonic() + 180
            while subprocess.run(compose('exec', '-T', service, *command),
                    cwd=REPO, env=env, stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL, timeout=10).returncode:
                if time.monotonic() >= deadline:
                    raise RuntimeError(f'The owned {service} did not become ready')
                time.sleep(.5)
        # Only these disposable fixtures grant database creation / lock inspection privileges.
        for service, client in (('mysql', 'mysql'), ('mariadb', 'mariadb')):
            run_child(compose('exec', '-T', service, client, '-u', 'root',
                '-pmy-secret-pw', '-e', "CREATE DATABASE IF NOT EXISTS testdb; "
                "CREATE USER IF NOT EXISTS 'test'@'%' IDENTIFIED BY 'test1234'; "
                "GRANT ALL PRIVILEGES ON *.* TO 'test'@'%';"),
                env, root / (service + '-test-grants.log'))
        env = native_environment(root, ports, env)
        for name in TESTS:
            binary = binaries[name]
            print(f'Native backend regression: {name}', flush=True)
            command = [str(binary), '--test-threads=1', '--nocapture']
            if name in FILTERS:
                command.append(FILTERS[name])
            run_child(command, env, root / (name + '.log'))
            check_test_output(name, (root / (name + '.log')).read_text())
            report['tests'].append({'name': name, 'filter': FILTERS.get(name),
                                    'sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
                                    'passed': True})
            save_report(receipt, report)
        report['passed'] = True
    finally:
        try:
            if owned:
                if not report['passed']:
                    # These logs belong only to this receipt's disposable project.
                    with (root / 'services-failure.log').open('w') as output:
                        try:
                            subprocess.run(compose('logs', '--no-color', '--tail', '100', *SERVICES),
                                cwd=REPO, env=env, stdout=output, stderr=subprocess.STDOUT, timeout=15)
                        except subprocess.TimeoutExpired:
                            output.write('\nTimed out collecting fixture diagnostics.\n')
                save_report(receipt, report)
                cleanup(root)
                report['cleaned_up'] = json.loads(receipt.read_text())['cleaned_up']
        finally:
            save_report(receipt, report)
            for sig, handler in previous_handlers.items():
                signal.signal(sig, handler)
    print('Native PostgreSQL, MySQL, MariaDB and Valkey account regressions passed.', flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True)
    parser.add_argument('--cleanup', action='store_true')
    parser.add_argument('--profile', choices=('test', 'ci', 'release'), default='test')
    parser.add_argument('--focused-build', action='store_true',
                        help='Build only the selected integration targets for local iteration.')
    args = parser.parse_args()
    if args.cleanup:
        cleanup(args.root)
    else:
        run(args.root, args.profile, args.focused_build)
