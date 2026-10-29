#!/usr/bin/env python3
"""Exercise installed wheels or images without services, network access, or auto-installs.

Run in an isolated interpreter with an existing scratch MRSM_ROOT_DIR and plugins directory.
Exit nonzero for any failed public operation; image sizes and successful imports alone do not
establish the dependency-free façade contract. Output is JSON for preserving gate evidence.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import os
from pathlib import Path
import subprocess
import sys


def check_cli_data(path: Path) -> int:
    """Validate the tiny SQLite pipe printed by `mrsm show data --nopretty`."""
    records = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    assert len(records) == 2, records
    pipe, serialized_frame = records
    identity = ('connector_keys', 'metric_key', 'location_key', 'instance_keys')
    assert {key: pipe[key] for key in identity} == {
        'connector_keys': 'sql:smoke', 'metric_key': 'tiny',
        'location_key': None, 'instance_keys': 'sql:smoke',
    }, pipe
    # `--nopretty` prints the frame as a JSON string; native reads preserve integers.
    assert isinstance(serialized_frame, str), serialized_frame
    frame = json.loads(serialized_frame)
    assert frame == {'id': {'0': 1}, 'val': {'0': 2}}, frame
    assert all(type(value) is int for column in frame.values() for value in column.values())
    print('SMOKE_CLI_DATA_JSON:' + json.dumps(frame))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cli-data', type=Path, help='Validate a tiny pipe CLI output file.')
    args = parser.parse_args()
    if args.cli_data is not None:
        return check_cli_data(args.cli_data)

    root = Path(os.environ['MRSM_ROOT_DIR']).resolve()
    plugins = Path(os.environ['MRSM_PLUGINS_DIR']).resolve()
    if not root.is_dir() or not plugins.is_dir():
        raise RuntimeError('The smoke test requires existing scratch root and plugins directories.')
    os.environ['MRSM_NO_AUTO_INSTALL'] = '1'
    os.environ['MRSM_SQL_SMOKE'] = 'sqlite:///' + str(root / 'smoke.db')

    import meerschaum as mrsm
    from meerschaum.utils import packages

    report = {
        'package': mrsm.__file__,
        'version': mrsm.__version__,
        'dependencies': sorted(
            (d.metadata['Name'], d.version) for d in importlib.metadata.distributions()
            if d.metadata.get('Name')
        ),
        'install_requests': [],
        'checks': {},
    }

    def deny_install(*names, **kwargs):
        report['install_requests'].append(list(names))
        raise RuntimeError('Unexpected dependency installation request: ' + ', '.join(names))

    packages.pip_install = deny_install

    def check(name, function):
        try:
            result = function()
            report['checks'][name] = {'ok': True, 'result': result}
        except Exception as error:
            report['checks'][name] = {
                'ok': False, 'error': type(error).__name__, 'message': str(error),
            }

    def native_version():
        result = subprocess.run(['mrsm', '--version'], capture_output=True, text=True, timeout=30)
        if result.returncode:
            raise RuntimeError(result.stderr or result.stdout)
        return result.stdout.strip()

    def passwords():
        from meerschaum.core.User import hash_password, verify_password
        value = hash_password('smoke-password', salt=b'smoke-salt', rounds=1000)
        assert verify_password('smoke-password', value)
        assert not verify_password('wrong-password', value)
        return True

    def config_round_trip():
        from meerschaum.config import read_config, write_config
        data = {'value': 1, 'text': 'on', 'nested': {'enabled': True}}
        assert write_config({'smoke_probe': data})
        assert read_config(keys=['smoke_probe'], substitute=False)['smoke_probe'] == data
        return True

    def pipe_round_trip():
        heavy_names = ('sqlalchemy', 'pandas', 'polars', 'numpy')
        already = {m for m in heavy_names if m in sys.modules}
        pipe = mrsm.Pipe(
            'sql:smoke', 'probe', instance='sql:smoke',
            columns={'id': 'id'}, dtypes={'id': 'int', 'value': 'string'},
        )
        mrsm.initialize_engine_metrics(instance=pipe.instance_connector)
        result = pipe.sync([{'id': 1, 'value': 'ok'}])
        if not result[0]:
            raise RuntimeError(result[1])
        success, message = result
        assert result.success is success and result.message == message
        assert result.inserted == 1, result.inserted
        assert pipe.get_data(as_docs=True) == [{'id': 1, 'value': 'ok'}]
        metrics = mrsm.get_engine_metrics(instance=pipe.instance_connector, pipe=pipe)
        assert metrics['retention_days'] == 30
        assert isinstance(metrics['summary']['syncs'], int)
        assert all(item['metric_key'] == 'probe' for item in metrics['history'])
        ### Pipe data and performance reads must work without a dataframe or SQL package.
        heavy = [m for m in heavy_names if m in sys.modules and m not in already]
        assert not heavy, 'Unexpected imports: ' + ', '.join(heavy)
        return True

    check('native_version', native_version)
    check('get_config', lambda: mrsm.get_config('meerschaum', 'instance'))
    check('passwords', passwords)
    check('public_config_round_trip', config_round_trip)
    check('public_pipe_dict_round_trip', pipe_round_trip)
    print('SMOKE_JSON:' + json.dumps(report))
    return 0 if all(item['ok'] for item in report['checks'].values()) else 1


if __name__ == '__main__':
    sys.exit(main())
