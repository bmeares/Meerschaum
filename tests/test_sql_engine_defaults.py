"""Engine construction keeps shared flavor defaults isolated (approved V4-001 fix)."""

import copy
from types import SimpleNamespace

import pytest


@pytest.fixture
def engine_builder(monkeypatch):
    from meerschaum.connectors.sql._create_engine import create_engine
    from meerschaum.utils import packages
    calls = []

    def build(uri, **kwargs):
        calls.append((uri, copy.deepcopy(kwargs)))
        # SQLAlchemy and third-party dialects may mutate nested engine options.
        kwargs.setdefault('connect_args', {})['dialect_local_option'] = True
        return SimpleNamespace(uri=uri)

    sqlalchemy = SimpleNamespace(create_engine=build)
    pool = SimpleNamespace(QueuePool=object)

    def attempt(*names, **kwargs):
        modules = [sqlalchemy if name == 'sqlalchemy' else pool for name in names]
        return modules[0] if len(modules) == 1 else modules

    monkeypatch.setattr(packages, 'attempt_import', attempt)
    return create_engine, calls


def connector(flavor, system=None, overrides=None):
    return SimpleNamespace(
        flavor=flavor, host='localhost', database='example', username='example',
        _sys_config={
            'poolclass': 'sqlalchemy.pool.QueuePool',
            'create_engine': copy.deepcopy(system or {}),
        },
        create_engine=copy.deepcopy(overrides or {}),
    )


def test_engine_overrides_do_not_leak_across_flavors(engine_builder):
    from meerschaum._internal.static import STATIC_CONFIG
    create_engine, calls = engine_builder
    defaults = copy.deepcopy(STATIC_CONFIG)
    sqlite = connector(
        'sqlite', system={'pool_size': 2},
        overrides={'pool_size': 3, 'max_overflow': 4},
    )
    assert create_engine(sqlite, pool_size=5, pool_recycle=6) is not None
    assert calls[0][1]['pool_size'] == 5
    assert calls[0][1]['max_overflow'] == 4
    assert calls[0][1]['pool_recycle'] == 6
    assert STATIC_CONFIG == defaults

    for flavor in ('postgresql', 'mysql', 'geopackage', 'sqlite'):
        assert create_engine(connector(flavor)) is not None
        args = calls[-1][1]
        expected = defaults['sql']['create_engine_flavors'][flavor]['create_engine']
        assert args['pool_size'] == expected['pool_size']
        assert args['max_overflow'] == expected['max_overflow']
        assert args['pool_recycle'] == expected['pool_recycle']
        assert 'dialect_local_option' not in args['connect_args']
    assert STATIC_CONFIG == defaults


def test_engine_nested_default_options_are_copied(engine_builder):
    from meerschaum._internal.static import STATIC_CONFIG
    create_engine, calls = engine_builder
    defaults = copy.deepcopy(STATIC_CONFIG)
    assert create_engine(connector('postgresql')) is not None
    assert create_engine(connector('postgresql')) is not None
    assert 'dialect_local_option' not in calls[1][1]['connect_args']
    assert STATIC_CONFIG == defaults


@pytest.mark.parametrize('flavor', ['sqlite', 'geopackage'])
def test_sync_sqlite_engines_do_not_request_async_driver(flavor, monkeypatch):
    import importlib
    sqlalchemy = pytest.importorskip('sqlalchemy')
    from meerschaum.connectors.sql._create_engine import create_engine
    from meerschaum.utils import packages
    requested = []

    def attempt(name, **kwargs):
        requested.append(name)
        assert name in ('sqlalchemy', 'sqlalchemy.pool'), name
        return importlib.import_module(name)

    monkeypatch.setattr(packages, 'attempt_import', attempt)
    conn = connector(flavor)
    conn.database = ':memory:'
    engine = create_engine(conn)
    assert engine is not None
    try:
        with engine.connect() as connection:
            assert connection.execute(sqlalchemy.text('SELECT 1')).scalar() == 1
    finally:
        engine.dispose()
    assert requested == ['sqlalchemy', 'sqlalchemy.pool']
