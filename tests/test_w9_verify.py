"""
Wave 9 (task Q): `Pipe.verify()` runs in Rust (`_core.verify_pipe`).

Each scenario runs a sequence of verification calls on pipes and compares what it observes with
the results recorded from the parity-phase Python mode (`tests/oracle/test_w9_verify.json`, not
v4.1.4 output): the `SuccessTuple`s (messages
included: they carry the chunk bounds and the per-chunk statistics), the chunk bounds, and the
rows each verification leaves in the target table.

The connectors are built ad hoc from Python (`mrsm.get_connector(...)` with attributes that are
in no config file): the Rust session must use them instead of reading the config registry.
"""

from tests._ports import service_port

import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest

import meerschaum as mrsm
from meerschaum import _core
from tests import _oracle as oracle

SCHEMA = 'w9q'


def _flavors():
    selected = [
        f.strip()
        for f in os.environ.get('MRSM_TEST_FLAVORS', 'sqlite').split(',')
        if f.strip()
    ]
    flavors = ['sqlite']
    if 'postgis' in selected:
        flavors.append('postgis')
    return flavors


def _connector(flavor: str, label: str, tmp_path):
    if flavor == 'sqlite':
        return mrsm.get_connector(
            'sql', label,
            flavor='sqlite',
            database=str(tmp_path / f'{label}.db'),
        )
    if flavor == 'postgis':
        conn = mrsm.get_connector(
            'sql', label,
            flavor='postgis',
            username='test',
            password='test1234',
            database='testdb',
            port=service_port('postgis'),
            host='localhost',
            schema=SCHEMA,
        )
        conn.exec(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
        return conn
    raise ValueError(flavor)


def _source_rows():
    start = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return [
        {'dt': start + timedelta(days=day), 'id': id_, 'val': day * 10 + id_}
        for day in range(100)
        for id_ in (1, 2)
    ]


def _rows(pipe):
    df = pipe.get_data(debug=False)
    if df is None or len(df) == 0:
        return []
    records = [
        (row['dt'].isoformat(), int(row['id']), int(row['val']))
        for row in df.to_dict(orient='records')
    ]
    return sorted(records)


def _build_pipes(instance, source_conn, suffix, where=''):
    """A source pipe on `source_conn` and a SQL-defined target pipe on `instance`."""
    from meerschaum.utils.sql import sql_item_name
    source = mrsm.Pipe(
        'w9q', f'src_{suffix}',
        instance=source_conn,
        columns={'datetime': 'dt', 'id': 'id'},
    )
    source.delete()
    source = mrsm.Pipe(
        'w9q', f'src_{suffix}',
        instance=source_conn,
        columns={'datetime': 'dt', 'id': 'id'},
        dtypes={'val': 'int'},
    )
    success, msg = source.sync(_source_rows())
    assert success, msg
    source_table = sql_item_name(
        source.target, source_conn.flavor, source_conn.get_pipe_schema(source)
    )
    target = mrsm.Pipe(source_conn, f'tgt_{suffix}', instance=instance)
    target.delete()
    target = mrsm.Pipe(
        source_conn, f'tgt_{suffix}',
        instance=instance,
        columns={'datetime': 'dt', 'id': 'id'},
        dtypes={'val': 'int'},
        parameters={'fetch': {'definition': f'SELECT dt, id, val FROM {source_table}{where}'}},
    )
    target.register()
    return source, target


def _scenario(instance, source_conn, suffix):
    """Run verification calls on a partially synced target; return everything observed."""
    source, target = _build_pipes(instance, source_conn, suffix)
    observed = {}

    ### Only the newest ten days first: verification backfills the rest.
    newest = datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(days=90)
    success, msg = target.sync(begin=newest, debug=False)
    assert success, msg
    observed['rows_before'] = len(_rows(target))
    observed['chunk_bounds'] = [
        tuple(str(b) for b in bounds)
        for bounds in target.get_chunk_bounds(bounded=False, align=True, debug=False)
    ]

    calls = [
        ('check_rowcounts_only', {'check_rowcounts_only': True}),
        ('default', {}),
        ('again', {}),
        ('skip_greater', {'skip_chunks_with_greater_rowcounts': True}),
        ('bounded_interval', {'bounded': True, 'chunk_interval': timedelta(days=20)}),
        ('int_minutes', {'chunk_interval': 60 * 24 * 45, 'workers': 1, 'batchsize': 2}),
        ('window', {
            'begin': datetime(2024, 2, 1, tzinfo=timezone.utc),
            'end': datetime(2024, 3, 1, tzinfo=timezone.utc),
        }),
        ('params', {'params': {'id': 1}, 'chunksize': 7}),
        ('deduplicate', {'deduplicate': True}),
    ]
    for name, kwargs in calls:
        if name == 'window':
            target.clear(
                begin=datetime(2024, 2, 5, tzinfo=timezone.utc),
                end=datetime(2024, 2, 10, tzinfo=timezone.utc),
                debug=False,
            )
        result = target.verify(debug=False, **kwargs)
        assert isinstance(result, tuple) and len(result) == 2
        observed[name] = (result, len(_rows(target)))

    observed['rows'] = _rows(target)
    observed['source_rows'] = _rows(source)
    source.delete()
    target.delete()
    return observed


def _both(func, *args):
    expected = oracle.expected()
    actual = func(*args)
    return expected, actual


@pytest.mark.parametrize('flavor', _flavors())
def test_verify_inplace_matches_python(flavor, tmp_path, monkeypatch):
    """Same-instance SQL source (the in-place sync path)."""
    calls = []
    real = _core.verify_pipe

    def counting(*args, **kwargs):
        calls.append(args[0])
        return real(*args, **kwargs)

    monkeypatch.setattr(_core, 'verify_pipe', counting)
    suffix = uuid.uuid4().hex[:6]
    instance = _connector(flavor, f'w9q_inst_{suffix}', tmp_path)
    expected, actual = _both(_scenario, instance, instance, 'inplace')
    assert calls, "the Rust verification did not run"
    assert actual == expected
    assert expected['rows'] == expected['source_rows']


@pytest.mark.parametrize('flavor', _flavors())
def test_verify_cross_connector_matches_python(flavor, tmp_path, monkeypatch):
    """A SQL source on another connector (the fetch path)."""
    calls = []
    real = _core.verify_pipe

    def counting(*args, **kwargs):
        calls.append(args[0])
        return real(*args, **kwargs)

    monkeypatch.setattr(_core, 'verify_pipe', counting)
    suffix = uuid.uuid4().hex[:6]
    instance = _connector(flavor, f'w9q_inst_{suffix}', tmp_path)
    source_conn = _connector(flavor, f'w9q_src_{suffix}', tmp_path)
    expected, actual = _both(_scenario, instance, source_conn, 'cross')
    assert calls, "the Rust verification did not run"
    assert actual == expected
    assert expected['rows'] == expected['source_rows']


def test_empty_target_cannot_determine_bounds(tmp_path):
    """A bounded verification of a pipe without data warns and syncs instead."""
    suffix = uuid.uuid4().hex[:6]
    instance = _connector('sqlite', f'w9q_empty_{suffix}', tmp_path)

    def scenario():
        _, target = _build_pipes(instance, instance, 'empty', where=' WHERE 1 = 0')
        result = target.verify(bounded=True, debug=False)
        unbounded = target.verify(debug=False)
        rows = _rows(target)
        target.delete()
        return result, unbounded, rows

    expected, actual = _both(scenario)
    assert actual == expected


def test_every_call_gets_a_native_context(tmp_path):
    """Wave 15 (L): `debug`, forwarded keywords, plugin sources, references and templates all
    run natively; only an instance without a native driver would keep the Python orchestration."""
    from meerschaum.core.Pipe._verify import _rust_verify_context
    suffix = uuid.uuid4().hex[:6]
    instance = _connector('sqlite', f'w9q_fb_{suffix}', tmp_path)
    _, target = _build_pipes(instance, instance, 'fallback')

    context, _ = _rust_verify_context(target)
    assert context is not None
    assert set(context['connectors']) == {str(instance)}
    assert _rust_verify_context(target, debug=True)[0] is not None

    plugin_pipe = mrsm.Pipe('plugin:stress', 'w9q', instance=instance)
    assert _rust_verify_context(plugin_pipe)[0] is not None

    ref_pipe = mrsm.Pipe(
        str(instance), 'w9q_ref',
        instance=instance,
        parameters={'reference': {'connector': 'w9q', 'metric': 'src_fallback'}},
    )
    assert _rust_verify_context(ref_pipe)[0] is not None

    template_pipe = mrsm.Pipe(
        str(instance), 'w9q_link',
        instance=instance,
        parameters={'columns': {'datetime': 'dt'}, 'verify': {'chunk_minutes': '{{ self.x }}'}},
    )
    assert _rust_verify_context(template_pipe)[0] is not None

    target.delete()


def _variants_scenario(instance, source_conn, suffix):
    """Verifications that used to keep the Python orchestration."""
    source, target = _build_pipes(instance, source_conn, suffix)
    observed = {}
    newest = datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(days=90)
    success, msg = target.sync(begin=newest, debug=False)
    assert success, msg

    ### A pipe that inherits the definition (and columns) of the target through `reference`.
    reference = mrsm.Pipe(
        str(source_conn), f'ref_{suffix}',
        instance=instance,
        parameters={
            'reference': {
                'connector': str(source_conn),
                'metric': f'tgt_{suffix}',
                'instance': str(instance),
            },
        },
    )
    reference.delete()
    reference = mrsm.Pipe(
        str(source_conn), f'ref_{suffix}',
        instance=instance,
        parameters={
            'reference': {
                'connector': str(source_conn),
                'metric': f'tgt_{suffix}',
                'instance': str(instance),
            },
        },
    )
    reference.register()

    for name, pipe, kwargs in (
        ('debug', target, {'debug': True}),
        ('force', target, {'force': True, 'retries': 2, 'min_seconds': 0}),
        ('plugin_kwarg', target, {'some_plugin_flag': 1}),
        ('reference', reference, {}),
        ('reference_rowcounts', reference, {'check_rowcounts_only': True}),
    ):
        result = pipe.verify(**kwargs)
        assert isinstance(result, tuple) and len(result) == 2
        ### The debug run names tables and timings; only the outcome is compared for it.
        observed[name] = (
            (result[0], '' if name == 'debug' else result[1]),
            len(_rows(pipe)),
        )
    observed['rows'] = _rows(target)
    observed['source_rows'] = _rows(source)
    reference.delete()
    source.delete()
    target.delete()
    return observed


@pytest.mark.parametrize('flavor', _flavors())
def test_verify_variants_match_python(flavor, tmp_path, monkeypatch):
    """`debug`, forwarded keywords and references verify natively with the same outcomes."""
    calls = []
    real = _core.verify_pipe

    def counting(*args, **kwargs):
        calls.append(args[0])
        return real(*args, **kwargs)

    monkeypatch.setattr(_core, 'verify_pipe', counting)
    suffix = uuid.uuid4().hex[:6]
    instance = _connector(flavor, f'w9q_inst_{suffix}', tmp_path)
    source_conn = _connector(flavor, f'w9q_src_{suffix}', tmp_path)
    expected, actual = _both(_variants_scenario, instance, source_conn, 'variants')
    assert len(calls) == 5, "every variant must run natively"
    assert actual == expected
    assert expected['rows'] == expected['source_rows']


def test_register_connector_binding(tmp_path):
    """`_core.register_connector` puts a Python-built connector into the façade session."""
    suffix = uuid.uuid4().hex[:6]
    conn = _connector('sqlite', f'w9q_reg_{suffix}', tmp_path)
    rust = conn._rust_method('register_pipe').__self__
    keys = str(conn)
    assert _core.register_connector(keys, rust) is False
    assert _core.register_connector(keys, rust) is True
    assert _core.unregister_connector(keys) is True
    assert _core.unregister_connector(keys) is False
    with pytest.raises(ValueError):
        _core.register_connector(keys, object())
