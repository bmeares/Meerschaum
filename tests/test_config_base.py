"""Dependency-free native configuration reads and SDK package metadata lookup."""

import datetime
import os
from pathlib import Path
import subprocess
import sys
import textwrap

import pytest


ROOT = Path(__file__).resolve().parents[1]


def test_native_config_without_site_packages(tmp_path):
    """The native lookup and password façade need no Python framework or installer."""
    from meerschaum import _core
    if _core.load() is None:
        pytest.skip('requires the native extension')
    root = tmp_path / 'root'
    plugins = tmp_path / 'plugins'
    root.mkdir()
    plugins.mkdir()
    env = {k: v for k, v in os.environ.items() if not k.startswith('MRSM_')}
    env.update({
        'MRSM_ROOT_DIR': str(root),
        'MRSM_PLUGINS_DIR': str(plugins),
        'MRSM_CORE': 'rust',
        'MRSM_NO_AUTO_INSTALL': '1',
        'PYTHONPATH': str(ROOT),
    })
    result = subprocess.run(
        [sys.executable, '-S', '-c', textwrap.dedent('''
            import sys
            import meerschaum as mrsm
            from meerschaum.config import paths
            from meerschaum.core.User import hash_password, verify_password
            from meerschaum.utils import packages

            def forbidden(*args, **kw):
                raise AssertionError('native façade attempted to install a dependency')
            packages.pip_install = forbidden
            packages.attempt_import = forbidden
            assert mrsm.get_config('meerschaum', 'instance') == 'sql:main'
            assert verify_password('password', hash_password('password', rounds=1000))
            metadata_dir = paths.ROOT_DIR_PATH / 'config_probe-1.2.3.dist-info'
            metadata_dir.mkdir()
            (metadata_dir / 'METADATA').write_text(
                'Metadata-Version: 2.1\\nName: config-probe\\nVersion: 1.2.3\\n'
            )
            sys.path.insert(0, str(paths.ROOT_DIR_PATH))
            assert packages._get_package_metadata('config_probe', None)['version'] == '1.2.3'
            forbidden_modules = {'yaml', 'pip', 'uv', 'packaging', 'semver', 'rich', 'humanfriendly',
                                 'pandas', 'polars', 'pyarrow', 'sqlalchemy'}
            assert not (forbidden_modules & set(sys.modules)), forbidden_modules & set(sys.modules)
            assert 'meerschaum.utils.yaml' not in sys.modules
        ''')],
        env=env, capture_output=True, text=True, timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_public_config_reader_preserves_yaml_types(tmp_path):
    """Do not route the public reader through a JSON-only YAML value conversion."""
    from meerschaum.config import read_config
    (tmp_path / 'typed.yaml').write_text(
        'day: 2024-01-01\nstamp: 2024-01-01T12:30:00Z\n'
        'keys:\n  1: number\n  "1": text\nflag: yes\nquoted: "yes"\n'
    )
    value = read_config(directory=tmp_path, keys=['typed'], substitute=False)['typed']
    assert value['day'] == datetime.date(2024, 1, 1)
    assert value['stamp'] == datetime.datetime(2024, 1, 1, 12, 30, tzinfo=datetime.timezone.utc)
    assert value['keys'] == {1: 'number', '1': 'text'}
    assert value['flag'] is True
    assert value['quoted'] == 'yes'


def test_import_pandas_only_requests_data_extra(monkeypatch):
    from meerschaum.utils import packages
    requested = []
    monkeypatch.setattr(packages, 'pandas_name', lambda: 'pandas')

    def attempt(*names, **kw):
        requested.extend(names)
        return tuple(names) if len(names) > 1 else names[0]

    monkeypatch.setattr(packages, 'attempt_import', attempt)
    assert packages.import_pandas() == 'pandas'
    assert requested == ['pandas', 'pyarrow', 'pandas']


def test_distribution_metadata_does_not_require_installer(tmp_path, monkeypatch):
    from meerschaum.utils import packages
    target = tmp_path / 'site-packages'
    dist_info = target / 'config_probe-1.2.3.dist-info'
    dist_info.mkdir(parents=True)
    (dist_info / 'METADATA').write_text(
        'Metadata-Version: 2.1\nName: config-probe\nVersion: 1.2.3\nSummary: probe\n'
    )
    monkeypatch.setattr(packages, 'venv_target_path', lambda *a, **kw: target)

    def forbidden(*args, **kw):
        pytest.fail('metadata lookup invoked an installer or subprocess')

    monkeypatch.setattr(packages, 'run_python_package', forbidden)
    monkeypatch.setattr(packages, '_get_uv_bin', forbidden)
    assert packages._get_package_metadata('config_probe', 'probe')['version'] == '1.2.3'
    assert packages._get_package_metadata('missing_probe', 'probe') == {}
