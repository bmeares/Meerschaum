"""
Wave 17c: `mrsm upgrade postgres`, `mrsm backup postgres` and `mrsm restore postgres` end to end,
against real containers, and the façade's view of them.

The Docker tests are opt-in (`MRSM_TEST_DOCKER=1`): they need Docker with Compose, the images
`timescale/timescaledb-ha:pg17` and `timescale/timescaledb-ha:pg18` (and
`timescale/timescaledb:latest-pg17` / `latest-pg18` for the official-layout scenario) already
pulled, a standalone `mrsm` (`cargo build -p meerschaum --bin mrsm`, or `MRSM_TEST_BIN`) and the
built extension (the façade populates the database the way Meerschaum users do).

Every resource is scratch and is removed afterwards by its exact name: a temporary root whose
`stack.yaml` names a random Compose project `<prefix><random>` (`MRSM_TEST_DOCKER_PREFIX`,
default `mrsmpgupgrade`), published ports bound to 127.0.0.1 on random free ports, and a random
password. The default project `mrsm` is refused. Before anything is started, the resolved compose
project is checked (its name and every published port), and once the database runs, `sql:main` is
checked to be the scratch container (the same `system_identifier` through `docker exec` and
through the connector).
"""

from __future__ import annotations

import hashlib
import json
import os
import pathlib
import re
import secrets
import signal
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request

import pytest

REPO = pathlib.Path(__file__).resolve().parent.parent
LABEL = 'io.meerschaum.upgrade-postgres'

docker_test = pytest.mark.skipif(
    os.environ.get('MRSM_TEST_DOCKER') != '1',
    reason='Set MRSM_TEST_DOCKER=1 to run the Docker end-to-end tests.',
)


def test_the_facade_reaches_the_stack_database_actions():
    """`meerschaum.actions.actions` has `upgrade postgres`, `backup postgres` and `restore postgres`."""
    from meerschaum import _core
    if _core.load() is None:
        pytest.skip("The `_core` extension is not built (scripts/rust/build_py.sh).")
    from meerschaum.actions import actions, get_subactions

    for action, function in (
        ('upgrade', '_upgrade_postgres'),
        ('backup', '_backup_postgres'),
        ('restore', '_restore_postgres'),
    ):
        assert action in actions, action
        subactions = get_subactions(action)
        assert 'postgres' in subactions, (action, list(subactions))
        assert subactions['postgres'].__name__ == function
        assert '(v5)' in (subactions['postgres'].__doc__ or '')
    # A call reaches the native subaction (it refuses an extra word before touching Docker).
    success, message = actions['backup'](['postgres', 'extra'])
    assert not success and 'takes no word' in message, message
    success, message = actions['restore'](['postgres', 'a', 'b'])
    assert not success and 'takes one word' in message, message


def _free_ports(count: int) -> list:
    sockets = []
    try:
        for _ in range(count):
            s = socket.socket()
            s.bind(('127.0.0.1', 0))
            sockets.append(s)
        return [s.getsockname()[1] for s in sockets]
    finally:
        for s in sockets:
            s.close()


def _docker(*args, check=True, timeout=600, **kw) -> subprocess.CompletedProcess:
    kw['env'] = {k: v for k, v in kw.get('env', os.environ).items() if k not in ('TMUX', 'TMUX_PANE')}
    result = subprocess.run(
        ['docker', *args], capture_output=True, text=True, timeout=timeout, **kw
    )
    if check and result.returncode != 0:
        raise AssertionError(f"docker {' '.join(args)} failed:\n{result.stderr}")
    return result


def _plain_output(result: subprocess.CompletedProcess) -> str:
    """Compare semantic guidance independently of ANSI styling and terminal wrapping."""
    text = re.sub(r'\x1b\[[0-?]*[ -/]*[@-~]', '', result.stdout + result.stderr)
    return ' '.join(text.split())


def _require_images(*images: str) -> None:
    for image in images:
        if _docker('image', 'inspect', '--format', '{{.Id}}', image, check=False).returncode != 0:
            pytest.skip(f"The image '{image}' is not pulled (the test never pulls).")


class ScratchStack:
    """A Meerschaum stack in a temporary root, with its own Compose project, ports and password."""

    def __init__(self, image: str, data_target: str, legacy_mount: bool, holder: bool):
        from tests._binary import binary_or_skip

        self.binary = binary_or_skip()
        prefix = os.environ.get('MRSM_TEST_DOCKER_PREFIX', 'mrsmpgupgrade')
        self.project = f"{prefix}{secrets.token_hex(3)}"
        assert self.project != 'mrsm' and self.project.startswith(prefix)
        parent = pathlib.Path(
            os.environ.get('MRSM_TEST_DOCKER_DIR', str(pathlib.Path.home() / '.cache' / 'mrsm-pgupgrade-tests'))
        )
        parent.mkdir(parents=True, exist_ok=True)
        self.scratch = pathlib.Path(tempfile.mkdtemp(prefix='mrsm-pgupgrade-', dir=parent))
        self.root = self.scratch / 'root'
        self.plugins = self.scratch / 'plugins'
        self.root.mkdir()
        self.plugins.mkdir()
        self.password = secrets.token_hex(12)
        self.db_port, self.api_port, self.valkey_port, self.grafana_port = _free_ports(4)
        self.image = image
        self.data_target = data_target
        self.legacy_mount = legacy_mount
        self.holder = holder
        self.write_config()

    # --- configuration --------------------------------------------------------------------

    def write_config(self) -> None:
        defaults = json.loads(
            (REPO / 'crates' / 'meerschaum-core' / 'src' / 'config' / 'default_config.json')
            .read_text()
        )
        stack = json.loads(
            json.dumps(defaults['stack']).replace('{__MRSM_ROOT_DIR__}', self.root.as_posix())
        )
        stack['project_name'] = self.project
        compose = stack['docker-compose.yaml']
        services = compose['services']
        db = services['db']
        db['image'] = self.image
        db['ports'] = ['127.0.0.1:MRSM{meerschaum:connectors:sql:main:port}:5432']
        setup = [v for v in db['volumes'] if 'initdb' in v]
        if self.legacy_mount:
            # The layout a v4-era `stack.yaml` pins: the setup directory mounted over the image's.
            setup = [f"{self.root.as_posix()}/db/initdb:/docker-entrypoint-initdb.d:z,ro"]
            initdb = self.root / 'db' / 'initdb'
            initdb.mkdir(parents=True)
            (initdb / 'create-extensions.sql').write_text(
                'CREATE EXTENSION IF NOT EXISTS timescaledb;\n'
                'CREATE EXTENSION IF NOT EXISTS postgis;\n'
                'CREATE EXTENSION IF NOT EXISTS timescaledb_toolkit;\n'
                'CREATE EXTENSION IF NOT EXISTS pg_stat_statements;\n'
            )
        db['volumes'] = [f"meerschaum_db_data:{self.data_target}", *setup]
        services['api']['ports'] = [
            '127.0.0.1:MRSM{meerschaum:connectors:api:main:port}:'
            'MRSM{meerschaum:connectors:api:main:port}'
        ]
        services['valkey']['ports'] = ['127.0.0.1:MRSM{meerschaum:connectors:valkey:main:port}:6379']
        services['grafana']['ports'] = [f"127.0.0.1:{self.grafana_port}:3000"]
        if self.holder:
            # A client of the database (like the API): it holds a connection open, so the
            # upgrade must stop it before the dump and start it again afterwards.
            services['holder'] = {
                'image': self.image,
                'command': ['psql', '-X', '-c', 'SELECT pg_sleep(86400)'],
                'environment': {
                    'PGHOST': 'db',
                    'PGUSER': '<DOLLAR>POSTGRES_USER',
                    'PGPASSWORD': '<DOLLAR>POSTGRES_PASSWORD',
                    'PGDATABASE': '<DOLLAR>POSTGRES_DB',
                },
                'networks': ['backend'],
                'depends_on': {'db': {'condition': 'service_healthy'}},
                'restart': 'no',
            }
        config = self.root / 'config'
        config.mkdir(exist_ok=True)
        (config / 'stack.yaml').write_text(json.dumps(stack, indent=2) + '\n')
        (config / 'meerschaum.yaml').write_text(json.dumps({
            'instance': 'sql:main',
            'connectors': {
                'sql': {'main': {
                    'flavor': 'timescaledb', 'host': '127.0.0.1', 'port': self.db_port,
                    'username': 'mrsm', 'password': self.password, 'database': 'meerschaum',
                }},
                'api': {'main': {
                    'host': '127.0.0.1', 'port': self.api_port, 'protocol': 'http',
                    'username': 'mrsm', 'password': self.password,
                }},
                'valkey': {'main': {
                    'host': '127.0.0.1', 'port': self.valkey_port, 'password': self.password,
                }},
            },
        }, indent=2) + '\n')

    def env(self, **extra) -> dict:
        env = {
            k: v for k, v in os.environ.items()
            if not k.startswith('MRSM_') and k not in ('TMUX', 'TMUX_PANE')
        }
        env.update({
            'MRSM_ROOT_DIR': str(self.root),
            'MRSM_PLUGINS_DIR': str(self.plugins),
            'MRSM_NO_AUTO_INSTALL': '1',
            'PYTHONPATH': str(REPO),
            'MRSM_PYTHON': sys.executable,
        })
        if os.environ.get('MRSM_DUCKDB_LIB'):
            env['MRSM_DUCKDB_LIB'] = os.environ['MRSM_DUCKDB_LIB']
        env.update(extra)
        return env

    # --- running things ---------------------------------------------------------------------

    def mrsm(self, *args, timeout=1800, **extra_env) -> subprocess.CompletedProcess:
        started = time.time()
        result = subprocess.run(
            [self.binary, *args], capture_output=True, text=True, timeout=timeout,
            env=self.env(**extra_env), cwd=str(self.scratch), stdin=subprocess.DEVNULL,
        )
        result.seconds = time.time() - started
        print(f"\n$ mrsm {' '.join(args)}  ({result.seconds:.1f}s, exit {result.returncode})")
        print(result.stdout[-6000:])
        print(result.stderr[-3000:])
        return result

    def python(self, code: str, *args):
        """Run `code` with the façade in this root; the last stdout line is JSON."""
        result = subprocess.run(
            [sys.executable, '-c', code, *args], capture_output=True, text=True, timeout=900,
            env=self.env(), cwd=str(self.scratch),
        )
        assert result.returncode == 0, result.stderr[-4000:]
        return json.loads(result.stdout.strip().splitlines()[-1])

    @property
    def db_container(self) -> str:
        return f"{self.project}-db-1"

    def psql(self, database: str, sql: str) -> str:
        result = _docker(
            'exec', '-i', self.db_container, 'sh', '-c',
            'exec psql -X -q -At -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$1"', 'sh', database,
            input=sql,
        )
        return result.stdout.strip()

    def resolved(self) -> dict:
        """`mrsm stack config --format json`: read-only, it starts nothing."""
        result = self.mrsm('stack', 'config', '--format', 'json', timeout=120)
        assert result.returncode == 0, result.stderr
        text = result.stdout
        return json.loads(text[text.index('{'):text.rindex('}') + 1])

    def check_isolation(self) -> None:
        """The resolved project is the scratch one and publishes only scratch loopback ports."""
        resolved = self.resolved()
        assert resolved['name'] == self.project, resolved['name']
        scratch_ports = {self.db_port, self.api_port, self.valkey_port, self.grafana_port}
        for name, service in resolved['services'].items():
            for port in service.get('ports', []):
                assert port.get('host_ip') == '127.0.0.1', (name, port)
                assert int(port['published']) in scratch_ports, (name, port)
        assert resolved['services']['db']['image'] == self.image

    def up(self, *services: str) -> None:
        result = self.mrsm('stack', 'up', '-d', *services, timeout=600)
        assert result.returncode == 0, result.stderr
        if 'db' in services:
            self.wait_db()

    def wait_db(self, timeout: int = 300) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            health = _docker(
                'inspect', '--format', '{{.State.Health.Status}}', self.db_container, check=False,
            ).stdout.strip()
            ready = _docker(
                'exec', '-i', self.db_container, 'sh', '-c',
                'psql -X -At -U "$POSTGRES_USER" -d template1', check=False,
                input="SELECT current_setting('listen_addresses') <> '';",
            ).stdout.strip()
            if health == 'healthy' and ready == 't':
                return
            time.sleep(1)
        raise AssertionError(f"{self.db_container} did not become healthy")

    def check_sql_main(self) -> None:
        """`sql:main` is this scratch container, not any other database on the host."""
        inside = self.psql('meerschaum', 'SELECT system_identifier FROM pg_control_system();')
        through = self.python(
            "import json, meerschaum as mrsm\n"
            "conn = mrsm.get_connector('sql:main')\n"
            "print(json.dumps(str(conn.value('SELECT system_identifier FROM pg_control_system()'))))"
        )
        assert inside == through, (inside, through)

    def volumes(self) -> set:
        listed = _docker(
            'volume', 'ls', '--format', '{{.Name}}',
            '--filter', f"label=com.docker.compose.project={self.project}",
        ).stdout.split()
        return {v for v in listed if v.startswith(self.project)}

    # --- cleanup ----------------------------------------------------------------------------

    def cleanup(self) -> None:
        compose_file = self.root / 'stack' / 'docker-compose.yaml'
        if compose_file.exists():
            _docker(
                'compose', '--project-name', self.project, '-f', str(compose_file),
                'down', '--volumes', '--remove-orphans', '--timeout', '10',
                check=False, env={**os.environ, 'POSTGRES_PASSWORD': self.password},
            )
        names = _docker(
            'ps', '-a', '--format', '{{.Names}}', '--filter', f"label={LABEL}", check=False,
        ).stdout.split()
        names += _docker(
            'ps', '-a', '--format', '{{.Names}}',
            '--filter', f"label=com.docker.compose.project={self.project}", check=False,
        ).stdout.split()
        for name in sorted(set(names)):
            if name.startswith(self.project):
                _docker('rm', '-f', '-v', name, check=False)
        for volume in sorted(self.volumes() | {
            f"{self.project}_meerschaum_db_data",
            f"{self.project}_meerschaum_db_data_pg18",
        }):
            assert volume.startswith(self.project)
            if _docker('volume', 'inspect', volume, check=False).returncode == 0:
                _docker('volume', 'rm', volume, check=False)
        networks = _docker(
            'network', 'ls', '--format', '{{.Name}}',
            '--filter', f"label=com.docker.compose.project={self.project}", check=False,
        ).stdout.split()
        for network in networks:
            if network.startswith(self.project):
                _docker('network', 'rm', network, check=False)
        shutil.rmtree(self.scratch, ignore_errors=True)


SNAPSHOT = r'''
import json, sys
import pandas as pd
import meerschaum as mrsm

conn = mrsm.get_connector('sql:main')
snap = {'server_version_num': int(conn.value('SHOW server_version_num'))}
pipes = {}
for pipe in mrsm.get_pipes(instance='sql:main', as_list=True):
    df = pipe.get_data()
    sort = [c for c in (pipe.columns.get('datetime'), pipe.columns.get('id')) if c]
    if df is not None and len(df) and sort:
        df = df.sort_values(sort).reset_index(drop=True)
    sample = df.head(25) if df is not None else None
    if sample is not None and 'geom' in sample.columns:
        wkt = [g.wkt if g is not None else None for g in sample['geom']]
        sample = pd.DataFrame(sample.drop(columns=['geom'])).assign(geom=wkt)
    pipes[str(pipe)] = {
        'rowcount': pipe.get_rowcount(),
        'sample': json.loads(sample.to_json(orient='records', date_format='iso', default_handler=str)) if sample is not None else None,
        'dtypes': pipe.dtypes,
    }
snap['pipes'] = pipes
snap['hypertables'] = conn.read(
    "SELECT hypertable_schema, hypertable_name, num_dimensions, compression_enabled "
    "FROM timescaledb_information.hypertables ORDER BY 1, 2"
).to_dict(orient='records')
snap['compressed_chunks'] = int(conn.value(
    "SELECT count(*) FROM timescaledb_information.chunks WHERE is_compressed"
))
snap['geometry'] = conn.read(
    'SELECT id, ST_AsText(geom) AS wkt FROM "sql_main_upg_geo" ORDER BY id'
).to_dict(orient='records')
snap['users'] = sorted(conn.get_users())
print(json.dumps(snap, sort_keys=True, default=str))
'''

POPULATE = r'''
import json, uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import meerschaum as mrsm
from meerschaum.core import User, Token

conn = mrsm.get_connector('sql:main')
now = datetime(2026, 10, 1, tzinfo=timezone.utc)
results = []

metrics = mrsm.Pipe('sql:main', 'upg_metrics', instance='sql:main',
    columns={'datetime': 'ts', 'id': 'id'},
    dtypes={'payload': 'json', 'uid': 'uuid', 'amount': 'numeric', 'blob': 'bytes'},
    upsert=True)
rows = [{
    'ts': now - timedelta(minutes=10 * i), 'id': i % 7, 'value': i * 0.5,
    'payload': {'i': i, 'tags': ['a', 'b']}, 'uid': uuid.UUID(int=i + 1),
    'amount': Decimal(i) / Decimal(3), 'blob': f'b{i}'.encode(),
} for i in range(3000)]
results.append(metrics.sync(rows))
results.append(metrics.sync([{**rows[0], 'value': -1.0}, {**rows[1], 'payload': {'updated': True}}]))

compressed = mrsm.Pipe('sql:main', 'upg_compressed', instance='sql:main',
    columns={'datetime': 'ts', 'id': 'id'})
results.append(compressed.sync(pd.DataFrame({
    'ts': [now - timedelta(minutes=30 * i) for i in range(6000)],
    'id': [i % 5 for i in range(6000)],
    'v': [float(i) for i in range(6000)],
})))
results.append(compressed.compress())

geo = mrsm.Pipe('sql:main', 'upg_geo', instance='sql:main',
    columns={'id': 'id'}, dtypes={'geom': 'geometry[Point, 4326]'})
results.append(geo.sync(gpd.GeoDataFrame(
    {'id': list(range(50)), 'geom': [Point(-82.4 + i / 100, 34.8 + i / 100) for i in range(50)]},
    geometry='geom', crs=4326,
)))

conn.exec("CREATE TABLE upg_source (ts TIMESTAMPTZ NOT NULL, id INT NOT NULL, value DOUBLE PRECISION)")
conn.exec("INSERT INTO upg_source SELECT '2026-09-01'::timestamptz + g * interval '1 hour', g % 3, g FROM generate_series(1, 500) g")
source = mrsm.Pipe('sql:main', 'source', instance='sql:main', columns={'datetime': 'ts', 'id': 'id'},
    parameters={'fetch': {'definition': 'SELECT ts, id, value FROM upg_source'}})
results.append(source.register())
derived = mrsm.Pipe('sql:main', 'derived', instance='sql:main', columns={'datetime': 'ts', 'id': 'id'},
    parameters={'fetch': {'definition': "SELECT ts, id, value * 2 AS doubled FROM {{ Pipe('sql:main', 'source', instance='sql:main') }}"}})
results.append(derived.register())
run = derived.sync(parents=True)
results.append((run.success, run.message))

user = User('upguser', 'upg-password-1', instance='sql:main')
results.append(conn.register_user(user))
token = Token(label='upgtoken', user=user, instance='sql:main')
results.append(token.register())
print(json.dumps({'results': [list(r) for r in results], 'client_id': str(token.id), 'secret': token.secret}))
'''


def _login(port: int, form: dict) -> int:
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/login", data=urllib.parse.urlencode(form).encode(),
    )
    try:
        return urllib.request.urlopen(request, timeout=30).status
    except urllib.error.HTTPError as e:
        return e.code


def _check_logins(stack: ScratchStack, credentials: dict) -> None:
    """A host-side API on the upgraded instance accepts the token and the user's password."""
    api = subprocess.Popen(
        [stack.binary, 'start', 'api', '-p', str(stack.api_port), '--no-dash', '--no-webterm',
         '-i', 'sql:main'],
        env=stack.env(), cwd=str(stack.scratch), stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL, stdin=subprocess.DEVNULL,
    )
    try:
        deadline = time.time() + 120
        while time.time() < deadline:
            try:
                urllib.request.urlopen(
                    f"http://127.0.0.1:{stack.api_port}/healthcheck", timeout=5
                )
                break
            except Exception:
                time.sleep(1)
        assert _login(stack.api_port, {
            'grant_type': 'client_credentials',
            'client_id': credentials['client_id'],
            'client_secret': credentials['secret'],
        }) == 200
        assert _login(stack.api_port, {
            'grant_type': 'client_credentials',
            'client_id': credentials['client_id'],
            'client_secret': 'not-the-secret',
        }) == 401
        assert _login(stack.api_port, {'username': 'upguser', 'password': 'upg-password-1'}) == 200
    finally:
        api.terminate()
        try:
            api.wait(timeout=30)
        except subprocess.TimeoutExpired:
            api.kill()
            api.wait(timeout=30)


@docker_test
def test_upgrade_rollback_and_failure_on_a_v4_era_stack():
    """
    timescale/timescaledb-ha:pg17 with the v4-era layout (`stack.yaml` pins the image and the
    setup directory mounted over the image's) -> pg18: the data, the TimescaleDB catalog, the
    PostGIS column, the engine state, the users and tokens survive; `rollback` brings back the
    old volume unchanged; a copy that does not verify is never switched to.
    """
    _require_images('timescale/timescaledb-ha:pg17', 'timescale/timescaledb-ha:pg18')
    stack = ScratchStack(
        'timescale/timescaledb-ha:pg17', '/home/postgres/pgdata', legacy_mount=True, holder=True,
    )
    try:
        stack.check_isolation()
        stack.up('db')
        stack.check_sql_main()

        # A second database on an older TimescaleDB than the new image's default: the dump
        # carries 2.29.2 and the restore must create exactly that version first.
        stack.psql('postgres', 'CREATE DATABASE second;')
        stack.psql('second', "CREATE EXTENSION timescaledb VERSION '2.29.2';")
        stack.psql('second', """
            CREATE TABLE readings (ts timestamptz NOT NULL, sensor int NOT NULL, v double precision);
            SELECT create_hypertable('readings', 'ts', chunk_time_interval => interval '1 day');
            INSERT INTO readings SELECT '2026-09-01'::timestamptz + g * interval '5 minutes', g % 4, g
              FROM generate_series(1, 4000) g;
            ALTER TABLE readings SET (timescaledb.compress, timescaledb.compress_segmentby = 'sensor');
            SELECT count(compress_chunk(c)) FROM show_chunks('readings', older_than => '2026-09-10'::timestamptz) c;
            CREATE SEQUENCE second_seq; SELECT setval('second_seq', 42);
        """)
        populated = stack.python(POPULATE)
        assert all(r[0] for r in populated['results']), populated['results']
        stack.up('holder')
        before = stack.python(SNAPSHOT)
        assert before['server_version_num'] // 10000 == 17
        stack_yaml = (stack.root / 'config' / 'stack.yaml').read_bytes()
        old_volume = f"{stack.project}_meerschaum_db_data"
        new_volume = f"{stack.project}_meerschaum_db_data_pg18"
        second = "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'; " \
            "SELECT count(*) FROM readings; " \
            "SELECT count(*) FILTER (WHERE is_compressed) FROM timescaledb_information.chunks; " \
            "SELECT last_value FROM second_seq;"
        assert stack.psql('second', second).split() == ['2.29.2', '4000', '9', '42']

        # The upgrade.
        result = stack.mrsm('upgrade', 'postgres', '18', '--yes')
        assert result.returncode == 0, result.stdout + result.stderr
        assert 'now runs PostgreSQL 18' in result.stdout
        after = stack.python(SNAPSHOT)
        assert after['server_version_num'] // 10000 == 18
        for key in ('pipes', 'hypertables', 'compressed_chunks', 'geometry', 'users'):
            assert after[key] == before[key], key
        assert stack.psql('second', second).split() == ['2.30.2', '4000', '9', '42']
        running = _docker(
            'compose', '--project-name', stack.project, 'ps', '--services', '--status', 'running',
            cwd=str(stack.root / 'stack'),
        ).stdout.split()
        assert 'holder' in running, running
        assert {old_volume, new_volume} <= {
            v for v in _docker('volume', 'ls', '--format', '{{.Name}}').stdout.split()
        }
        _check_logins(stack, populated)
        stack.psql(
            'meerschaum',
            "INSERT INTO upg_source SELECT '2026-09-01'::timestamptz + g * interval '1 hour', "
            "g % 3, g FROM generate_series(501, 600) g;",
        )
        synced = stack.mrsm(
            'sync', 'pipes', '-c', 'sql:main', '-m', 'derived', '--parents', '-i', 'sql:main',
        )
        assert synced.returncode == 0, synced.stderr
        assert stack.psql('meerschaum', 'SELECT count(*) FROM sql_main_derived;') == '600'

        # Rollback: the old volume, untouched by the upgrade, and the stack config byte for byte.
        result = stack.mrsm('upgrade', 'postgres', 'rollback', '--yes')
        assert result.returncode == 0, result.stdout + result.stderr
        rolled = stack.python(SNAPSHOT)
        assert rolled == before
        assert (stack.root / 'config' / 'stack.yaml').read_bytes() == stack_yaml
        assert stack.psql('second', second).split() == ['2.29.2', '4000', '9', '42']

        # A kept volume is never reused or suggested for deletion: choose a run-specific name.
        result = stack.mrsm('upgrade', 'postgres', '18', '--yes')
        assert result.returncode == 0, result.stdout + result.stderr
        assert stack.psql('meerschaum', 'SHOW server_version_num;').startswith('18')
        assert _docker('volume', 'inspect', new_volume, check=False).returncode == 0
        result = stack.mrsm('upgrade', 'postgres', 'rollback', '--yes')
        assert result.returncode == 0, result.stdout + result.stderr
        assert stack.python(SNAPSHOT) == before
        _docker('volume', 'rm', new_volume)

        # A copy that does not match is never switched to: the old stack stays as it was.
        result = stack.mrsm(
            'upgrade', 'postgres', '18', '--yes',
            MRSM_TEST_DOCKER='1',
            MRSM_TEST_UPGRADE_POSTGRES_FAIL=(
                'after_restore:meerschaum:DELETE FROM sql_main_upg_metrics '
                'WHERE ts = (SELECT max(ts) FROM sql_main_upg_metrics)'
            ),
        )
        assert result.returncode != 0
        out = result.stdout + result.stderr
        assert 'does not match' in out and 'sql_main_upg_metrics' in out, out
        assert 'Nothing was switched' in out
        assert (stack.root / 'config' / 'stack.yaml').read_bytes() == stack_yaml
        assert stack.python(SNAPSHOT) == before
        assert _docker('volume', 'inspect', new_volume, check=False).returncode != 0
        assert not [name for name in _docker(
            'ps', '-a', '--format', '{{.Names}}', '--filter', f"label={LABEL}",
        ).stdout.split() if name.startswith(stack.project)]
    finally:
        stack.cleanup()


@docker_test
def test_upgrade_moves_the_data_directory_of_the_official_image_layout():
    """
    timescale/timescaledb:latest-pg17 keeps its data in the volume at /var/lib/postgresql/data;
    PostgreSQL 18's official images use /var/lib/postgresql/18/docker under a /var/lib/postgresql
    volume: the new volume is mounted there.
    """
    _require_images('timescale/timescaledb:latest-pg17', 'timescale/timescaledb:latest-pg18')
    stack = ScratchStack(
        'timescale/timescaledb:latest-pg17', '/var/lib/postgresql/data',
        legacy_mount=False, holder=False,
    )
    try:
        stack.check_isolation()
        stack.up('db')
        stack.check_sql_main()
        stack.psql('meerschaum', """
            CREATE EXTENSION IF NOT EXISTS timescaledb;
            CREATE TABLE t (ts timestamptz NOT NULL, v int);
            SELECT create_hypertable('t', 'ts');
            INSERT INTO t SELECT now() - g * interval '1 hour', g FROM generate_series(1, 1000) g;
        """)
        result = stack.mrsm('upgrade', 'postgres', '--yes')
        assert result.returncode == 0, result.stdout + result.stderr
        assert stack.psql('meerschaum', 'SHOW server_version_num;').startswith('18')
        assert stack.psql('meerschaum', 'SELECT count(*), sum(v) FROM t;') == '1000|500500'
        mounts = json.loads(_docker(
            'inspect', '--format', '{{json .Mounts}}', stack.db_container,
        ).stdout)
        data = [m for m in mounts if m.get('Name') == f"{stack.project}_meerschaum_db_data_pg18"]
        assert data and data[0]['Destination'] == '/var/lib/postgresql', mounts
        config = (stack.root / 'config' / 'stack.yaml').read_text()
        assert 'meerschaum_db_data_pg18:/var/lib/postgresql' in config
        assert 'timescale/timescaledb:latest-pg18' in config
    finally:
        stack.cleanup()


def _container_starts(project: str) -> list:
    """Every container of the project: name, ID, start time and state."""
    ids = _docker(
        'ps', '-aq', '--filter', f"label=com.docker.compose.project={project}",
    ).stdout.split()
    return sorted(
        _docker(
            'inspect', '--format', '{{.Name}} {{.Id}} {{.State.StartedAt}} {{.State.Status}}', i,
        ).stdout.strip()
        for i in ids
    )


def _manifest(path: pathlib.Path) -> dict:
    return json.loads((path / 'manifest.json').read_text())


def _data_volume(stack: ScratchStack) -> str:
    mounts = json.loads(_docker(
        'inspect', '--format', '{{json .Mounts}}', stack.db_container,
    ).stdout)
    (volume,) = [m['Name'] for m in mounts if m.get('Type') == 'volume']
    return volume


@docker_test
def test_backup_restore_rollback_and_retention_on_a_running_stack():
    """
    `backup postgres` on a running pg18 stack (the default layout) stops nothing and writes a
    private, checksummed backup; after the data changes, `restore postgres` brings back exactly the
    backup's state in a new volume and `restore postgres rollback` the changed state; a damaged
    backup is refused before anything changes; `system:backups:postgres:keep` prunes only the
    oldest complete backups.
    """
    _require_images('timescale/timescaledb-ha:pg18')
    stack = ScratchStack(
        'timescale/timescaledb-ha:pg18', '/home/postgres/pgdata', legacy_mount=False, holder=True,
    )
    try:
        stack.check_isolation()
        stack.up('db')
        stack.check_sql_main()
        populated = stack.python(POPULATE)
        assert all(r[0] for r in populated['results']), populated['results']
        stack.up('holder')
        before = stack.python(SNAPSHOT)
        old_volume = f"{stack.project}_meerschaum_db_data"
        backups = stack.root / 'backups' / 'postgres'

        # An online backup: nothing is stopped or restarted (the holder keeps its connection).
        starts = _container_starts(stack.project)
        assert any('holder' in s and s.endswith('running') for s in starts), starts
        result = stack.mrsm('backup', 'postgres', '--noask')
        assert result.returncode == 0, result.stdout + result.stderr
        assert 'Backed up 2 databases of PostgreSQL 18' in result.stdout, result.stdout
        assert _container_starts(stack.project) == starts
        (first,) = [d for d in backups.iterdir() if d.is_dir()]
        manifest = _manifest(first)
        assert (manifest['kind'], manifest['state']) == ('backup', 'complete')
        assert manifest['server']['major'] == 18
        assert manifest['server']['image'] == 'timescale/timescaledb-ha:pg18'
        dumps = manifest['dumps']
        for entry in [dumps['globals'], *dumps['databases']]:
            path = first / entry['file']
            assert hashlib.sha256(path.read_bytes()).hexdigest() == entry['sha256']
            assert path.stat().st_size == entry['bytes']
        main = next(d for d in dumps['databases'] if d['name'] == 'meerschaum')
        assert main['timescaledb'] and main['row_estimate'] is not None
        assert main['extensions'] and main['entries'] > 0
        if os.name != 'nt':
            for path in (stack.root / 'backups', backups, first):
                assert path.stat().st_mode & 0o777 == 0o700, path
            for path in first.iterdir():
                assert path.stat().st_mode & 0o777 == 0o600, path
        assert not list(backups.glob('*.partial'))

        # The data changes after the backup.
        stack.psql('meerschaum', """
            DELETE FROM sql_main_upg_metrics WHERE id = 3;
            DELETE FROM sql_main_source WHERE id = 1;
            UPDATE sql_main_upg_geo SET geom = NULL WHERE id < 5;
        """)
        changed = stack.python(SNAPSHOT)
        assert changed != before

        # Restore: exactly the backup's state, in a new volume; the previous one is kept.
        result = stack.mrsm('restore', 'postgres', '--yes')
        assert result.returncode == 0, result.stdout + result.stderr
        assert stack.python(SNAPSHOT) == before
        restored_volume = _data_volume(stack)
        assert restored_volume != old_volume and restored_volume.startswith(stack.project)
        assert _docker('volume', 'inspect', old_volume, check=False).returncode == 0
        running = _docker(
            'compose', '--project-name', stack.project, 'ps', '--services', '--status', 'running',
            cwd=str(stack.root / 'stack'),
        ).stdout.split()
        assert 'holder' in running, running

        # Rollback: the changed state, on the previous volume.
        result = stack.mrsm('restore', 'postgres', 'rollback', '--yes')
        assert result.returncode == 0, result.stdout + result.stderr
        assert stack.python(SNAPSHOT) == changed
        assert _data_volume(stack) == old_volume

        # A damaged backup is refused before anything changes.
        damaged = backups / '20000101T000000Z'
        shutil.copytree(first, damaged)
        dump = damaged / main['file']
        data = bytearray(dump.read_bytes())
        data[len(data) // 2] ^= 0xFF
        dump.write_bytes(bytes(data))
        volumes = stack.volumes()
        result = stack.mrsm('restore', 'postgres', '20000101T000000Z', '--yes')
        out = result.stdout + result.stderr
        assert result.returncode != 0 and 'damaged' in out and main['file'] in out, out
        assert stack.volumes() == volumes
        assert _data_volume(stack) == old_volume
        shutil.rmtree(damaged)

        # Retention: the newest two complete backups stay; the restore's directory is never pruned.
        system = stack.root / 'config' / 'system.json'
        config = json.loads(system.read_text()) if system.exists() else {}
        config['backups'] = {'postgres': {'keep': 2}}
        system.write_text(json.dumps(config))
        for _ in range(2):
            result = stack.mrsm('backup', 'postgres')
            assert result.returncode == 0, result.stdout + result.stderr
        assert 'Removed 1 older backup' in result.stdout, result.stdout
        kinds = {
            d.name: _manifest(d)['kind']
            for d in backups.iterdir() if d.is_dir() and (d / 'manifest.json').exists()
        }
        assert sorted(kinds.values()) == ['backup', 'backup', 'restore'], kinds
        assert first.name not in kinds
    finally:
        stack.cleanup()


@docker_test
def test_post_dump_updates_are_refused_even_without_rowcount_changes():
    """An UPDATE after the snapshot must never be accepted as an unchanged source."""
    _require_images('timescale/timescaledb-ha:pg17', 'timescale/timescaledb-ha:pg18')
    stack = ScratchStack('timescale/timescaledb-ha:pg17', '/home/postgres/pgdata', False, False)
    try:
        stack.check_isolation()
        stack.up('db')
        stack.check_sql_main()
        stack.psql('meerschaum', 'CREATE TABLE write_guard (id int PRIMARY KEY, v int); INSERT INTO write_guard VALUES (1, 1);')
        original = (stack.root / 'config' / 'stack.yaml').read_bytes()
        result = stack.mrsm(
            'upgrade', 'postgres', '18', '--yes', MRSM_TEST_DOCKER='1',
            MRSM_TEST_UPGRADE_POSTGRES_FAIL='after_dump:meerschaum:UPDATE write_guard SET v = 2 WHERE id = 1',
        )
        assert result.returncode != 0, result.stdout + result.stderr
        assert 'old database changed after its dump' in result.stdout + result.stderr
        assert stack.psql('meerschaum', 'TABLE write_guard;') == '1|2'
        assert stack.psql('meerschaum', 'SHOW server_version_num;').startswith('17')
        assert (stack.root / 'config' / 'stack.yaml').read_bytes() == original
    finally:
        stack.cleanup()


@docker_test
def test_live_volume_collision_stack_guard_root_ownership_and_manifest_paths():
    """The live volume may already end in pg18; another root and a tampered path cannot move it."""
    _require_images('timescale/timescaledb-ha:pg17', 'timescale/timescaledb-ha:pg18')
    stack = ScratchStack('timescale/timescaledb-ha:pg17', '/home/postgres/pgdata', False, False)
    try:
        config_path = stack.root / 'config' / 'stack.yaml'
        config = json.loads(config_path.read_text())
        live_volume = f'{stack.project}_meerschaum_db_data_pg18'
        config['docker-compose.yaml']['volumes']['meerschaum_db_data'] = {'name': live_volume}
        config_path.write_text(json.dumps(config, indent=2) + '\n')
        stack.check_isolation()
        stack.up('db')
        stack.check_sql_main()
        stack.psql('meerschaum', 'CREATE TABLE sentinel (v int); INSERT INTO sentinel VALUES (42);')
        original = config_path.read_bytes()

        # A stopped old cluster must not be opened by a new-major image after config changes.
        assert stack.mrsm('stack', 'stop', 'db').returncode == 0
        config['docker-compose.yaml']['services']['db']['image'] = 'timescale/timescaledb-ha:pg18'
        config_path.write_text(json.dumps(config, indent=2) + '\n')
        guarded = stack.mrsm('stack', 'up', '-d', 'db')
        assert guarded.returncode != 0
        assert 'PG_VERSION' in guarded.stdout + guarded.stderr
        assert 'upgrade postgres 18' in _plain_output(guarded)
        config_path.write_bytes(original)
        stack.up('db')

        # The same Compose project in a different root belongs to the original root.
        foreign_root = stack.scratch / 'other-root'
        shutil.copytree(stack.root / 'config', foreign_root / 'config')
        for action in ('upgrade', 'backup', 'restore'):
            words = (action, 'postgres', 'rollback', '--yes') if action == 'restore' else (action, 'postgres', '--yes')
            refused = stack.mrsm(*words, MRSM_ROOT_DIR=str(foreign_root))
            assert refused.returncode != 0
            assert 'not created from this root' in _plain_output(refused)
        assert stack.psql('meerschaum', 'TABLE sentinel;') == '42'

        # An upgrade chooses another unused volume, never suggests deleting the live one.
        upgraded = stack.mrsm('upgrade', 'postgres', '18', '--yes')
        assert upgraded.returncode == 0, upgraded.stdout + upgraded.stderr
        assert stack.psql('meerschaum', 'TABLE sentinel;') == '42'
        manifests = sorted((stack.root / 'backups' / 'postgres').glob('*/manifest.json'))
        manifest_path = manifests[-1]
        manifest_text = manifest_path.read_text()
        manifest = json.loads(manifest_text)
        assert manifest['new']['volume'] != live_volume
        sentinel = stack.scratch / 'do-not-overwrite'
        sentinel.write_text('preserve this')
        manifest['config']['path'] = str(sentinel)
        manifest_path.write_text(json.dumps(manifest))
        refused = stack.mrsm('upgrade', 'postgres', 'rollback', '--yes')
        assert refused.returncode != 0
        assert "not this root's stack config" in _plain_output(refused)
        assert sentinel.read_text() == 'preserve this'
        assert stack.psql('meerschaum', 'SHOW server_version_num;').startswith('18')
        manifest_path.write_text(manifest_text)
        rolled = stack.mrsm('upgrade', 'postgres', 'rollback', '--yes')
        assert rolled.returncode == 0, rolled.stdout + rolled.stderr
        assert config_path.read_bytes() == original
        assert stack.psql('meerschaum', 'TABLE sentinel;') == '42'
    finally:
        stack.cleanup()


@docker_test
def test_existing_database_locale_and_nonpublic_timescaledb_schema_are_preserved():
    """Image-created databases must be recreated when their locale differs from the dump."""
    from meerschaum.utils.yaml import yaml

    _require_images('timescale/timescaledb-ha:pg17', 'timescale/timescaledb-ha:pg18')
    stack = ScratchStack('timescale/timescaledb-ha:pg17', '/home/postgres/pgdata', False, False)
    try:
        config_path = stack.root / 'config' / 'stack.yaml'
        # Exercise the minimal text edit with comments (the other tests use flow-style JSON).
        config = json.loads(config_path.read_text())
        text = yaml.dump(config, default_flow_style=False, sort_keys=False)
        text = '# keep this stack comment\n' + text.replace('image: timescale/timescaledb-ha:pg17', 'image: timescale/timescaledb-ha:pg17 # keep image comment', 1)
        config_path.write_text(text)
        stack.check_isolation()
        stack.up('db')
        stack.check_sql_main()
        stack.psql('meerschaum', "DROP DATABASE postgres WITH (FORCE); CREATE DATABASE postgres TEMPLATE template0 LC_COLLATE 'C' LC_CTYPE 'C'; CREATE DATABASE schema_test TEMPLATE template0;")
        stack.psql('schema_test', "CREATE SCHEMA analytics; CREATE EXTENSION timescaledb WITH SCHEMA analytics; CREATE TABLE measurements (ts timestamptz NOT NULL, v int); SELECT analytics.create_hypertable('measurements', 'ts'); INSERT INTO measurements VALUES (now(), 42);")
        locale_sql = "SELECT pg_encoding_to_char(encoding), datcollate, datctype FROM pg_database WHERE datname = 'postgres';"
        before = stack.psql('meerschaum', locale_sql)
        result = stack.mrsm('upgrade', 'postgres', '18', '--yes')
        assert result.returncode == 0, result.stdout + result.stderr
        assert stack.psql('meerschaum', locale_sql) == before
        assert stack.psql('schema_test', "SELECT n.nspname FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace WHERE e.extname = 'timescaledb';") == 'analytics'
        assert stack.psql('schema_test', 'SELECT v FROM measurements;') == '42'
        assert '# keep this stack comment' in config_path.read_text()
        assert '# keep image comment' in config_path.read_text()
        result = stack.mrsm('upgrade', 'postgres', 'rollback', '--yes')
        assert result.returncode == 0, result.stdout + result.stderr
        assert config_path.read_text() == text
    finally:
        stack.cleanup()


@docker_test
@pytest.mark.skipif(sys.platform == 'win32', reason='Unix signal recovery')
@pytest.mark.parametrize('stop_signal', ('SIGHUP', 'double_SIGINT', 'SIGKILL'))
@pytest.mark.parametrize('phase', ('after_isolate', 'after_restore'))
def test_interrupted_restore_phase_puts_back_the_old_stack(stop_signal, phase):
    """Signals and a later recovery after kill -9 retain the original data and restart dependents."""
    _require_images('timescale/timescaledb-ha:pg17', 'timescale/timescaledb-ha:pg18')
    stack = ScratchStack('timescale/timescaledb-ha:pg17', '/home/postgres/pgdata', False, True)
    process = None
    try:
        stack.check_isolation()
        stack.up('db')
        stack.check_sql_main()
        stack.psql('meerschaum', 'CREATE TABLE signal_sentinel (v int); INSERT INTO signal_sentinel VALUES (42);')
        stack.up('holder')
        original = (stack.root / 'config' / 'stack.yaml').read_bytes()
        log_path = stack.scratch / 'interruption.log'
        with log_path.open('w') as log:
            process = subprocess.Popen(
                [stack.binary, 'upgrade', 'postgres', '18', '--yes'],
                env=stack.env(MRSM_TEST_DOCKER='1', MRSM_TEST_UPGRADE_POSTGRES_FAIL=f'{phase}:meerschaum:SELECT pg_sleep(90)'),
                cwd=str(stack.scratch), stdout=log, stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL, start_new_session=True,
            )
            deadline = time.time() + 180
            manifest_path = None
            while time.time() < deadline:
                assert process.poll() is None, log_path.read_text()[-6000:]
                manifests = sorted((stack.root / 'backups' / 'postgres').glob('*/manifest.json'))
                if manifests:
                    candidate = manifests[-1]
                    manifest = json.loads(candidate.read_text())
                    # The old db stopped and the restore is deliberately waiting in its SQL hook.
                    if (phase == 'after_restore' and manifest.get('old_db_stopped')) or (phase == 'after_isolate' and manifest.get('state') == 'isolated'):
                        temporary = f"{stack.project}-pgupgrade-{manifest['run_id'].lower()}" if phase == 'after_restore' else stack.db_container
                        active = _docker(
                            'exec', temporary, 'psql', '-X', '-At', '-U', 'mrsm', '-d', 'template1',
                            '-c', "SELECT count(*) FROM pg_stat_activity WHERE query LIKE 'SELECT pg_sleep(90)%' AND state = 'active';",
                            check=False,
                        )
                        if active.returncode == 0 and active.stdout.strip() == '1':
                            manifest_path = candidate
                            break
                time.sleep(0.2)
            assert manifest_path is not None, log_path.read_text()[-6000:]
            if stop_signal == 'double_SIGINT':
                process.send_signal(signal.SIGINT)
                time.sleep(0.2)
                process.send_signal(signal.SIGINT)
            else:
                process.send_signal(getattr(signal, stop_signal))
            process.wait(timeout=180)
        assert process.returncode != 0, log_path.read_text()[-6000:]
        if stop_signal == 'SIGKILL':
            recovery = stack.mrsm('upgrade', 'postgres', 'rollback', '--yes')
            assert recovery.returncode == 0, recovery.stdout + recovery.stderr
            assert 'interrupted' in recovery.stdout.lower()
        stack.wait_db()
        assert stack.psql('meerschaum', 'SHOW server_version_num;').startswith('17')
        assert stack.psql('meerschaum', 'TABLE signal_sentinel;') == '42'
        assert (stack.root / 'config' / 'stack.yaml').read_bytes() == original
        assert json.loads(manifest_path.read_text())['state'] == 'failed'
        running = _docker('compose', '--project-name', stack.project, 'ps', '--services', '--status', 'running', cwd=str(stack.root / 'stack')).stdout.split()
        assert 'holder' in running
        leftovers = _docker('ps', '-a', '--format', '{{.Names}}', '--filter', f'label={LABEL}').stdout.split()
        assert not [name for name in leftovers if name.startswith(stack.project)]
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=30)
        stack.cleanup()


@docker_test
def test_network_fence_blocks_a_late_writer_after_the_last_check():
    """An external SQL connection after verification cannot write before old-server stop."""
    _require_images('timescale/timescaledb-ha:pg17', 'timescale/timescaledb-ha:pg18')
    stack = ScratchStack('timescale/timescaledb-ha:pg17', '/home/postgres/pgdata', False, False)
    process = None
    writer = f'{stack.project}-late-writer'
    try:
        stack.check_isolation()
        stack.up('db')
        stack.check_sql_main()
        stack.psql('meerschaum', 'CREATE TABLE late_write (v int); INSERT INTO late_write VALUES (1);')
        with (stack.scratch / 'late-write.log').open('w') as log:
            process = subprocess.Popen(
                [stack.binary, 'upgrade', 'postgres', '18', '--yes'],
                env=stack.env(MRSM_TEST_DOCKER='1', MRSM_TEST_UPGRADE_POSTGRES_FAIL='after_check:meerschaum:SELECT pg_sleep(15)'),
                cwd=str(stack.scratch), stdout=log, stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL, start_new_session=True,
            )
            deadline = time.time() + 180
            while time.time() < deadline:
                assert process.poll() is None, (stack.scratch / 'late-write.log').read_text()[-6000:]
                manifests = sorted((stack.root / 'backups' / 'postgres').glob('*/manifest.json'))
                if manifests and json.loads(manifests[-1].read_text()).get('state') == 'checked':
                    break
                time.sleep(.2)
            else:
                raise AssertionError('Upgrade did not reach its final old-server check')
            env = {k: v for k, v in os.environ.items() if k not in ('TMUX', 'TMUX_PANE')}
            env['PGPASSWORD'] = stack.password
            attempted = _docker(
                'run', '--rm', '--pull', 'never', '--name', writer, '--network', 'host',
                '-e', 'PGPASSWORD', '-e', 'PGCONNECT_TIMEOUT=2', '--entrypoint', 'psql',
                'timescale/timescaledb-ha:pg17', '-h', '127.0.0.1', '-p', str(stack.db_port),
                '-U', 'mrsm', '-d', 'meerschaum', '-X', '-v', 'ON_ERROR_STOP=1',
                '-c', 'UPDATE late_write SET v = 2', check=False, env=env, timeout=20,
            )
            assert attempted.returncode != 0, attempted.stdout + attempted.stderr
            process.wait(timeout=300)
        assert process.returncode == 0, (stack.scratch / 'late-write.log').read_text()[-6000:]
        assert stack.psql('meerschaum', 'TABLE late_write;') == '1'
        assert stack.psql('meerschaum', 'SHOW server_version_num;').startswith('18')
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=30)
        _docker('rm', '-f', '-v', writer, check=False)
        stack.cleanup()


@docker_test
@pytest.mark.skipif(sys.platform == 'win32', reason='Unix SIGKILL recovery')
def test_killed_rollback_after_config_edit_completes_without_deleting_either_volume():
    _require_images('timescale/timescaledb-ha:pg17', 'timescale/timescaledb-ha:pg18')
    stack = ScratchStack('timescale/timescaledb-ha:pg17', '/home/postgres/pgdata', False, False)
    process = None
    try:
        stack.check_isolation()
        stack.up('db')
        stack.check_sql_main()
        stack.psql('meerschaum', 'CREATE TABLE rollback_sentinel (v int); INSERT INTO rollback_sentinel VALUES (1);')
        original = (stack.root / 'config' / 'stack.yaml').read_bytes()
        result = stack.mrsm('upgrade', 'postgres', '18', '--yes')
        assert result.returncode == 0, result.stdout + result.stderr
        stack.psql('meerschaum', 'UPDATE rollback_sentinel SET v = 2;')
        manifests = sorted((stack.root / 'backups' / 'postgres').glob('*/manifest.json'))
        manifest_path = manifests[-1]
        before = json.loads(manifest_path.read_text())
        with (stack.scratch / 'rollback-kill.log').open('w') as log:
            process = subprocess.Popen(
                [stack.binary, 'upgrade', 'postgres', 'rollback', '--yes'],
                env=stack.env(MRSM_TEST_DOCKER='1', MRSM_TEST_UPGRADE_POSTGRES_FAIL='rollback_after_config'),
                cwd=str(stack.scratch), stdout=log, stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL, start_new_session=True,
            )
            deadline = time.time() + 120
            while time.time() < deadline:
                assert process.poll() is None, (stack.scratch / 'rollback-kill.log').read_text()[-6000:]
                if json.loads(manifest_path.read_text()).get('state') == 'rollback_config_restored':
                    break
                time.sleep(.2)
            else:
                raise AssertionError('Rollback did not reach its config edit')
            process.kill()
            process.wait(timeout=30)
        recovered = stack.mrsm('upgrade', 'postgres', 'rollback', '--yes')
        assert recovered.returncode == 0, recovered.stdout + recovered.stderr
        assert stack.psql('meerschaum', 'SHOW server_version_num;').startswith('17')
        assert stack.psql('meerschaum', 'TABLE rollback_sentinel;') == '1'
        assert (stack.root / 'config' / 'stack.yaml').read_bytes() == original
        assert json.loads(manifest_path.read_text())['state'] == 'rolled_back'
        for side in ('old', 'new'):
            assert _docker('volume', 'inspect', before[side]['volume'], check=False).returncode == 0
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=30)
        stack.cleanup()


@docker_test
def test_extension_tuning_requires_a_live_owned_stack_endpoint():
    """Exercise real PostgreSQL address formatting and the live-session ownership proof."""
    _require_images('timescale/timescaledb-ha:pg18')
    first = ScratchStack('timescale/timescaledb-ha:pg18', '/home/postgres/pgdata', False, False)
    second = ScratchStack('timescale/timescaledb-ha:pg18', '/home/postgres/pgdata', False, False)

    def telemetry(stack, expected):
        deadline = time.time() + 5
        while time.time() < deadline:
            if stack.psql('meerschaum', 'SHOW timescaledb.telemetry_level;') == expected:
                return
            time.sleep(.1)
        raise AssertionError(f'Telemetry did not become {expected}')

    def upgrade(stack, **env):
        result = stack.mrsm('upgrade', 'extensions', '-i', 'sql:main', '--yes', **env)
        assert result.returncode == 0, result.stdout + result.stderr
        return result

    try:
        defaults = {}
        for stack in (first, second):
            stack.check_isolation()
            stack.up('db')
            stack.check_sql_main()
            defaults[stack.project] = stack.psql(
                'meerschaum', "SELECT default_version FROM pg_available_extensions WHERE name = 'timescaledb';",
            )
            stack.psql('meerschaum', 'DROP EXTENSION timescaledb CASCADE;')
            stack.psql('meerschaum', "CREATE EXTENSION timescaledb VERSION '2.29.2';")
            stack.psql('meerschaum', "ALTER SYSTEM SET timescaledb.telemetry_level = 'basic'; SELECT pg_reload_conf();")
            telemetry(stack, 'basic')

        foreign_root = first.scratch / 'foreign-root'
        shutil.copytree(first.root / 'config', foreign_root / 'config')
        resolved = first.mrsm('stack', 'config', '--format', 'json', MRSM_ROOT_DIR=str(foreign_root))
        assert resolved.returncode == 0, resolved.stdout + resolved.stderr
        upgrade(first, MRSM_ROOT_DIR=str(foreign_root))
        telemetry(first, 'basic')
        assert first.psql('meerschaum', "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';") == defaults[first.project]

        first.psql('meerschaum', 'CREATE DATABASE other_db TEMPLATE template0;')
        first.psql('other_db', "CREATE EXTENSION timescaledb VERSION '2.29.2';")
        upgrade(first, MRSM_SQL_MAIN=f'postgresql://mrsm:{first.password}@127.0.0.1:{first.db_port}/other_db')
        telemetry(first, 'basic')
        assert first.psql('other_db', "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';") == defaults[first.project]

        upgrade(first, MRSM_SQL_MAIN=f'postgresql://mrsm:{second.password}@127.0.0.1:{second.db_port}/meerschaum')
        telemetry(second, 'basic')
        assert second.psql('meerschaum', "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';") == defaults[second.project]

        # A real local endpoint includes /32 or /128 under inet::text. The command must still
        # recognize its owned stack and prove the live nonce, then apply intended tuning.
        result = upgrade(first)
        assert 'telemetry' in _plain_output(result).lower()
        telemetry(first, 'off')
        telemetry(second, 'basic')
    finally:
        first.cleanup()
        second.cleanup()


def _db_status(stack: ScratchStack) -> str:
    done = _docker('inspect', '--format', '{{.State.Status}}', stack.db_container, check=False)
    return done.stdout.strip() or 'absent'


@docker_test
@pytest.mark.skipif(sys.platform == 'win32', reason='Unix process handling')
def test_stack_up_start_and_restart_wait_while_the_database_is_moved():
    """The guard covers every way to start the stopped old `db`, and only while a move runs."""
    _require_images('timescale/timescaledb-ha:pg17', 'timescale/timescaledb-ha:pg18')
    stack = ScratchStack('timescale/timescaledb-ha:pg17', '/home/postgres/pgdata', False, True)
    process = None
    try:
        stack.check_isolation()
        stack.up('db')
        stack.check_sql_main()
        stack.psql('meerschaum', 'CREATE TABLE guard_sentinel (v int); INSERT INTO guard_sentinel VALUES (7);')
        stack.up('holder')
        log_path = stack.scratch / 'guard.log'
        with log_path.open('w') as log:
            process = subprocess.Popen(
                [stack.binary, 'upgrade', 'postgres', '18', '--yes'],
                env=stack.env(MRSM_TEST_DOCKER='1', MRSM_TEST_UPGRADE_POSTGRES_FAIL='after_restore:meerschaum:SELECT pg_sleep(40)'),
                cwd=str(stack.scratch), stdout=log, stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL, start_new_session=True,
            )
            deadline = time.time() + 180
            while time.time() < deadline:
                assert process.poll() is None, log_path.read_text()[-6000:]
                manifests = sorted((stack.root / 'backups' / 'postgres').glob('*/manifest.json'))
                if manifests and json.loads(manifests[-1].read_text()).get('old_db_stopped'):
                    break
                time.sleep(0.2)
            else:
                raise AssertionError(log_path.read_text()[-6000:])
            # The old `db` is stopped for the restore: no way of starting it gets through.
            for command in (('up', '-d', 'db', 'holder'), ('start', 'db'), ('restart', 'db'), ('start', 'holder')):
                refused = stack.mrsm('stack', *command, timeout=300)
                assert refused.returncode != 0, command
                assert 'moving the database' in ' '.join(_plain_output(refused).split()), command
            assert _db_status(stack) == 'exited'
            process.wait(timeout=300)
        assert process.returncode == 0, log_path.read_text()[-6000:]
        # Nothing is held back once it ended.
        restarted = stack.mrsm('stack', 'restart', 'holder', timeout=300)
        assert restarted.returncode == 0, restarted.stdout + restarted.stderr
        assert stack.psql('meerschaum', 'TABLE guard_sentinel;') == '7'
    finally:
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=30)
        stack.cleanup()


@docker_test
@pytest.mark.skipif(sys.platform == 'win32', reason='Unix SIGKILL recovery')
def test_a_restore_that_began_with_db_stopped_leaves_it_stopped():
    """A killed restore, and one whose switch fails, never start a `db` that was down before."""
    _require_images('timescale/timescaledb-ha:pg17')
    stack = ScratchStack('timescale/timescaledb-ha:pg17', '/home/postgres/pgdata', False, False)
    process = None
    taken = None
    try:
        stack.check_isolation()
        stack.up('db')
        stack.check_sql_main()
        stack.psql('meerschaum', 'CREATE TABLE down_sentinel (v int); INSERT INTO down_sentinel VALUES (3);')
        assert stack.mrsm('backup', 'postgres').returncode == 0
        original = (stack.root / 'config' / 'stack.yaml').read_bytes()
        compose_file = stack.root / 'stack' / 'docker-compose.yaml'
        _docker(
            'compose', '--project-name', stack.project, '-f', str(compose_file), 'stop', 'db',
            cwd=str(stack.root / 'stack'), env={**os.environ, 'POSTGRES_PASSWORD': stack.password},
        )
        assert _db_status(stack) == 'exited'
        # 1. kill -9 while the copy is being restored, then recover.
        log_path = stack.scratch / 'down.log'
        with log_path.open('w') as log:
            process = subprocess.Popen(
                [stack.binary, 'restore', 'postgres', '--yes'],
                env=stack.env(MRSM_TEST_DOCKER='1', MRSM_TEST_UPGRADE_POSTGRES_FAIL='after_restore:meerschaum:SELECT pg_sleep(60)'),
                cwd=str(stack.scratch), stdout=log, stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL, start_new_session=True,
            )
            deadline = time.time() + 180
            while time.time() < deadline:
                assert process.poll() is None, log_path.read_text()[-6000:]
                temporary = _docker('ps', '--format', '{{.Names}}', '--filter', f'label={LABEL}', check=False).stdout.split()
                if any(name.startswith(f'{stack.project}-pgrestore-') for name in temporary):
                    time.sleep(8)
                    break
                time.sleep(0.2)
            else:
                raise AssertionError(log_path.read_text()[-6000:])
            process.kill()
            process.wait(timeout=60)
        recovery = stack.mrsm('restore', 'postgres', '--noask')
        assert 'stays stopped' in recovery.stdout, recovery.stdout + recovery.stderr
        assert _db_status(stack) == 'exited'
        assert (stack.root / 'config' / 'stack.yaml').read_bytes() == original
        assert len(stack.volumes()) == 1
        # 2. the switch fails (the published port is taken): the db stays down, its container is
        #    the stack's own again, and the new volume is gone.
        taken = socket.socket()
        taken.bind(('127.0.0.1', stack.db_port))
        taken.listen(1)
        failed = stack.mrsm('restore', 'postgres', '--yes')
        taken.close()
        taken = None
        assert failed.returncode != 0
        assert 'Putting the stack back' in failed.stdout + failed.stderr
        assert _db_status(stack) in ('created', 'exited', 'absent')
        assert (stack.root / 'config' / 'stack.yaml').read_bytes() == original
        assert len(stack.volumes()) == 1
        manifests = [json.loads(m.read_text()) for m in sorted((stack.root / 'backups' / 'postgres').glob('*/manifest.json'))]
        assert [m['state'] for m in manifests if m['kind'] == 'restore'] == ['failed', 'failed']
        # 3. the plain restore still works and leaves the restored database running.
        restored = stack.mrsm('restore', 'postgres', '--yes')
        assert restored.returncode == 0, restored.stdout + restored.stderr
        stack.wait_db()
        assert stack.psql('meerschaum', 'TABLE down_sentinel;') == '3'
    finally:
        if taken is not None:
            taken.close()
        if process is not None and process.poll() is None:
            process.kill()
            process.wait(timeout=30)
        stack.cleanup()

