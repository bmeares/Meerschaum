#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

import pytest
from datetime import datetime, timezone

import meerschaum as mrsm

from tests import debug
from tests.connectors import conns, get_flavors


@pytest.mark.parametrize("flavor", get_flavors())
def test_filter_existing_none_df(flavor: str):
    """filter_existing(None) returns three empty DataFrames with correct columns."""
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'filter_existing', 'none_df', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})
    pipe.delete()
    pipe = mrsm.Pipe('test', 'filter_existing', 'none_df', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})
    pipe.sync([{'dt': datetime(2021, 1, 1), 'id': 1, 'val': 10}], debug=debug)

    unseen, update, delta = pipe.filter_existing(None, debug=debug)

    assert unseen is not None
    assert update is not None
    assert delta is not None
    assert len(unseen) == 0
    assert len(update) == 0
    assert len(delta) == 0


@pytest.mark.parametrize("flavor", get_flavors())
def test_filter_existing_empty_df(flavor: str):
    """filter_existing of an empty DataFrame returns three references to the same empty df."""
    pd = mrsm.attempt_import('pandas')
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'filter_existing', 'empty_df', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})
    pipe.delete()
    pipe = mrsm.Pipe('test', 'filter_existing', 'empty_df', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})
    pipe.sync([{'dt': datetime(2021, 1, 1), 'id': 1, 'val': 10}], debug=debug)

    empty = pd.DataFrame([])
    unseen, update, delta = pipe.filter_existing(empty, debug=debug)

    assert len(unseen) == 0
    assert len(update) == 0
    assert len(delta) == 0


@pytest.mark.parametrize("flavor", get_flavors())
def test_filter_existing_all_new(flavor: str):
    """All rows are new → unseen contains all, update is empty, delta equals unseen."""
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'filter_existing', 'all_new', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})
    pipe.delete()
    pipe = mrsm.Pipe('test', 'filter_existing', 'all_new', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})

    existing = [
        {'dt': datetime(2021, 1, 1), 'id': 1, 'val': 10},
        {'dt': datetime(2021, 1, 2), 'id': 2, 'val': 20},
    ]
    pipe.sync(existing, debug=debug)

    new_rows = [
        {'dt': datetime(2021, 1, 3), 'id': 3, 'val': 30},
        {'dt': datetime(2021, 1, 4), 'id': 4, 'val': 40},
    ]
    pd = mrsm.attempt_import('pandas')
    new_df = pd.DataFrame(new_rows)

    unseen, update, delta = pipe.filter_existing(new_df, debug=debug)

    assert len(unseen) == 2
    assert len(update) == 0
    assert len(delta) == 2


@pytest.mark.parametrize("flavor", get_flavors())
def test_filter_existing_all_duplicate(flavor: str):
    """Syncing the same data again → all three DataFrames are empty."""
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'filter_existing', 'all_dup', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})
    pipe.delete()
    pipe = mrsm.Pipe('test', 'filter_existing', 'all_dup', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})

    rows = [
        {'dt': datetime(2021, 1, 1), 'id': 1, 'val': 10},
        {'dt': datetime(2021, 1, 2), 'id': 2, 'val': 20},
    ]
    pipe.sync(rows, debug=debug)

    pd = mrsm.attempt_import('pandas')
    dup_df = pd.DataFrame(rows)
    unseen, update, delta = pipe.filter_existing(dup_df, debug=debug)

    assert len(unseen) == 0
    assert len(update) == 0
    assert len(delta) == 0


@pytest.mark.parametrize("flavor", get_flavors())
def test_filter_existing_partial_update(flavor: str):
    """
    Some rows are new, some are updates.
    - id=1: value changed → update_df
    - id=2: unchanged → not in any df
    - id=3: new row → unseen_df
    - delta = unseen + update
    """
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'filter_existing', 'partial', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})
    pipe.delete()
    pipe = mrsm.Pipe('test', 'filter_existing', 'partial', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})

    existing = [
        {'dt': datetime(2021, 1, 1), 'id': 1, 'val': 10},
        {'dt': datetime(2021, 1, 2), 'id': 2, 'val': 20},
    ]
    pipe.sync(existing, debug=debug)

    incoming = [
        {'dt': datetime(2021, 1, 1), 'id': 1, 'val': 99},  # changed
        {'dt': datetime(2021, 1, 2), 'id': 2, 'val': 20},  # unchanged
        {'dt': datetime(2021, 1, 3), 'id': 3, 'val': 30},  # new
    ]
    pd = mrsm.attempt_import('pandas')
    incoming_df = pd.DataFrame(incoming)
    unseen, update, delta = pipe.filter_existing(incoming_df, debug=debug)

    assert len(unseen) == 1
    assert int(unseen['id'].iloc[0]) == 3

    assert len(update) == 1
    assert int(update['id'].iloc[0]) == 1

    assert len(delta) == 2
    delta_ids = sorted(int(v) for v in delta['id'])
    assert delta_ids == [1, 3]


@pytest.mark.parametrize("flavor", get_flavors())
def test_filter_existing_no_datetime_column(flavor: str):
    """Pipes without a datetime column still filter correctly by id."""
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'filter_existing', 'no_dt', instance=conn,
                     columns={'id': 'id'})
    pipe.delete()
    pipe = mrsm.Pipe('test', 'filter_existing', 'no_dt', instance=conn,
                     columns={'id': 'id'})

    pipe.sync([{'id': 1, 'val': 'a'}, {'id': 2, 'val': 'b'}], debug=debug)

    pd = mrsm.attempt_import('pandas')
    incoming_df = pd.DataFrame([
        {'id': 2, 'val': 'b'},   # unchanged
        {'id': 3, 'val': 'c'},   # new
    ])
    unseen, update, delta = pipe.filter_existing(incoming_df, debug=debug)

    assert len(unseen) == 1
    assert int(unseen['id'].iloc[0]) == 3


@pytest.mark.parametrize("flavor", get_flavors())
def test_filter_existing_no_pipe_data(flavor: str):
    """filter_existing on an unsynced pipe treats all rows as unseen."""
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'filter_existing', 'no_data', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})
    pipe.delete()
    pipe = mrsm.Pipe('test', 'filter_existing', 'no_data', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})
    pipe.register(debug=debug)

    pd = mrsm.attempt_import('pandas')
    df = pd.DataFrame([
        {'dt': datetime(2021, 1, 1), 'id': 1, 'val': 10},
        {'dt': datetime(2021, 1, 2), 'id': 2, 'val': 20},
    ])
    unseen, update, delta = pipe.filter_existing(df, debug=debug)

    assert len(unseen) == 2
    assert len(update) == 0
    assert len(delta) == 2


def test_filter_existing_polars_parity(monkeypatch):
    """The accelerated anti-join preserves unseen, update, and unchanged-row semantics."""
    pd = mrsm.attempt_import('pandas')
    import meerschaum.utils.dataframe as dataframe

    pipe = mrsm.Pipe(
        'test', 'filter_existing', 'polars_parity',
        instance=conns['sqlite'],
        columns={'datetime': 'dt', 'id': 'id'},
    )
    pipe.delete()
    success, msg = pipe.sync([
        {'dt': datetime(2021, 1, 1, tzinfo=timezone.utc), 'id': 1, 'val': 10},
        {'dt': datetime(2021, 1, 2, tzinfo=timezone.utc), 'id': 2, 'val': 20},
    ])
    assert success, msg
    incoming_df = pd.DataFrame([
        {'dt': datetime(2021, 1, 1, tzinfo=timezone.utc), 'id': 1, 'val': 99},
        {'dt': datetime(2021, 1, 2, tzinfo=timezone.utc), 'id': 2, 'val': 20},
        {'dt': datetime(2021, 1, 3, tzinfo=timezone.utc), 'id': 3, 'val': 30},
    ])

    monkeypatch.setattr(dataframe, '_POLARS_FILTER_MIN_ROWS', 10 ** 9)
    expected = pipe.filter_existing(incoming_df)
    monkeypatch.setattr(dataframe, '_POLARS_FILTER_MIN_ROWS', 0)
    actual = pipe.filter_existing(incoming_df)

    for actual_df, expected_df in zip(actual, expected):
        pd.testing.assert_frame_equal(actual_df, expected_df)


def test_filter_existing_polars_df():
    """`filter_existing()` accepts a Polars DataFrame and returns Pandas DataFrames."""
    pl = mrsm.attempt_import('polars')
    pipe = mrsm.Pipe(
        'test', 'filter_existing', 'polars_df',
        instance=conns['sqlite'],
        columns={'datetime': 'dt', 'id': 'id'},
    )
    pipe.delete()
    success, msg = pipe.sync([
        {'dt': datetime(2021, 1, 1, tzinfo=timezone.utc), 'id': 1, 'val': 10},
    ], debug=debug)
    assert success, msg

    incoming_df = pl.DataFrame([
        {'dt': datetime(2021, 1, 1, tzinfo=timezone.utc), 'id': 1, 'val': 99},
        {'dt': datetime(2021, 1, 2, tzinfo=timezone.utc), 'id': 2, 'val': 20},
    ])

    unseen, update, delta = pipe.filter_existing(incoming_df, debug=debug)

    for _df in (unseen, update, delta):
        assert isinstance(_df, pl.DataFrame), f"Expected Polars, got {type(_df)}."

    assert len(unseen) == 1
    assert len(update) == 1
    assert len(delta) == 2
    assert unseen['id'].to_list() == [2]
    assert update['val'].to_list() == [99]


def test_filter_existing_polars_empty_and_lazy():
    """Empty and lazy Polars frames also return Polars frames."""
    pl = mrsm.attempt_import('polars')
    pipe = mrsm.Pipe(
        'test', 'filter_existing', 'polars_empty',
        instance=conns['sqlite'],
        columns={'datetime': 'dt', 'id': 'id'},
    )
    pipe.delete()
    success, msg = pipe.sync([
        {'dt': datetime(2021, 1, 1, tzinfo=timezone.utc), 'id': 1, 'val': 10},
    ], debug=debug)
    assert success, msg

    empty = pl.DataFrame(schema={'dt': pl.Datetime(time_zone='UTC'), 'id': pl.Int64})
    for _df in pipe.filter_existing(empty, debug=debug):
        assert isinstance(_df, pl.DataFrame), f"Expected Polars, got {type(_df)}."

    lazy_df = pl.LazyFrame([
        {'dt': datetime(2021, 1, 2, tzinfo=timezone.utc), 'id': 2, 'val': 20},
    ])
    unseen, update, delta = pipe.filter_existing(lazy_df, debug=debug)
    for _df in (unseen, update, delta):
        assert isinstance(_df, pl.DataFrame), f"Expected Polars, got {type(_df)}."
    assert len(unseen) == 1
    assert len(update) == 0


def _normalize_records(df):
    """Return a DataFrame's rows as plain Python dicts for cross-library comparison."""
    import json
    from decimal import Decimal

    def _dump_json(value):
        return json.dumps(value, sort_keys=True, separators=(',', ':'), default=str)

    if df.__class__.__module__.split('.')[0] == 'polars':
        records = df.to_dicts()
    else:
        records = df.to_dict('records')

    def _normalize_value(value):
        if value is None or value != value:
            return None
        if isinstance(value, Decimal):
            return str(value)
        if hasattr(value, 'isoformat'):
            return value.isoformat()
        ### NOTE: Polars carries JSON columns as encoded strings,
        ### while Pandas carries them as `dict` / `list`.
        if isinstance(value, (dict, list)):
            return _dump_json(value)
        if isinstance(value, str) and value[:1] in ('{', '['):
            import json
            try:
                return _dump_json(json.loads(value))
            except json.JSONDecodeError:
                return value
        return value

    return [
        {col: _normalize_value(val) for col, val in record.items()}
        for record in records
    ]


def _assert_parity(pipe, docs, native_expected=True):
    """Assert the Polars path returns the same rows as the Pandas path."""
    pd = mrsm.attempt_import('pandas')
    pl = mrsm.attempt_import('polars')
    from meerschaum.utils.dataframe import to_polars

    pandas_df = pd.DataFrame(docs)
    ### NOTE: Build the Polars frame the same way `get_data()` does
    ### so that JSON columns stay JSON rather than being inferred as structs.
    polars_df = to_polars(
        pandas_df,
        json_cols=[
            col
            for col, typ in pipe.dtypes.items()
            if str(typ) == 'json' and col in pandas_df.columns
        ],
    )

    native = pipe._filter_existing_polars(polars_df)
    assert (native is not None) == native_expected, (
        f"Expected native Polars path to be {'taken' if native_expected else 'skipped'}."
    )

    pandas_dfs = pipe.filter_existing(pandas_df, debug=debug)
    polars_dfs = pipe.filter_existing(polars_df, debug=debug)

    for name, pandas_result, polars_result in zip(
        ('unseen', 'update', 'delta'), pandas_dfs, polars_dfs
    ):
        assert isinstance(polars_result, pl.DataFrame), f"{name} is not a Polars DataFrame."
        assert _normalize_records(polars_result) == _normalize_records(pandas_result), (
            f"Mismatched '{name}' rows."
        )
    return polars_dfs


@pytest.mark.parametrize("flavor", get_flavors())
def test_filter_existing_polars_parity_flavors(flavor: str):
    """The native Polars path matches the Pandas path across instance connectors."""
    conn = conns[flavor]
    pipe = mrsm.Pipe('test', 'filter_existing', 'pl_parity', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})
    pipe.delete()
    pipe = mrsm.Pipe('test', 'filter_existing', 'pl_parity', instance=conn,
                     columns={'datetime': 'dt', 'id': 'id'})
    success, msg = pipe.sync([
        {'dt': datetime(2021, 1, 1, tzinfo=timezone.utc), 'id': 1, 'val': 10},
        {'dt': datetime(2021, 1, 2, tzinfo=timezone.utc), 'id': 2, 'val': 20},
    ], debug=debug)
    assert success, msg

    ### One unchanged, one changed, one new.
    unseen, update, delta = _assert_parity(pipe, [
        {'dt': datetime(2021, 1, 1, tzinfo=timezone.utc), 'id': 1, 'val': 10},
        {'dt': datetime(2021, 1, 2, tzinfo=timezone.utc), 'id': 2, 'val': 99},
        {'dt': datetime(2021, 1, 3, tzinfo=timezone.utc), 'id': 3, 'val': 30},
    ])
    assert (len(unseen), len(update), len(delta)) == (1, 1, 2)

    ### All rows already exist.
    unseen, update, delta = _assert_parity(pipe, [
        {'dt': datetime(2021, 1, 1, tzinfo=timezone.utc), 'id': 1, 'val': 10},
        {'dt': datetime(2021, 1, 2, tzinfo=timezone.utc), 'id': 2, 'val': 20},
    ])
    assert (len(unseen), len(update), len(delta)) == (0, 0, 0)

    ### All rows are new.
    unseen, update, delta = _assert_parity(pipe, [
        {'dt': datetime(2022, 1, 1, tzinfo=timezone.utc), 'id': 4, 'val': 40},
        {'dt': datetime(2022, 1, 2, tzinfo=timezone.utc), 'id': 5, 'val': 50},
    ])
    assert (len(unseen), len(update), len(delta)) == (2, 0, 2)


def test_filter_existing_polars_parity_no_datetime():
    """The native Polars path matches Pandas for pipes without a datetime axis."""
    pipe = mrsm.Pipe('test', 'filter_existing', 'pl_no_dt', instance=conns['sqlite'],
                     columns={'id': 'id'})
    pipe.delete()
    pipe = mrsm.Pipe('test', 'filter_existing', 'pl_no_dt', instance=conns['sqlite'],
                     columns={'id': 'id'})
    success, msg = pipe.sync([{'id': 1, 'val': 10}, {'id': 2, 'val': 20}], debug=debug)
    assert success, msg

    unseen, update, delta = _assert_parity(pipe, [
        {'id': 1, 'val': 10},
        {'id': 2, 'val': 99},
        {'id': 3, 'val': 30},
    ])
    assert (len(unseen), len(update), len(delta)) == (1, 1, 2)


def test_filter_existing_polars_parity_int_axis():
    """The native Polars path matches Pandas for integer datetime axes."""
    pipe = mrsm.Pipe('test', 'filter_existing', 'pl_int_axis', instance=conns['sqlite'],
                     columns={'datetime': 'dt', 'id': 'id'}, dtypes={'dt': 'int'})
    pipe.delete()
    pipe = mrsm.Pipe('test', 'filter_existing', 'pl_int_axis', instance=conns['sqlite'],
                     columns={'datetime': 'dt', 'id': 'id'}, dtypes={'dt': 'int'})
    success, msg = pipe.sync([{'dt': 1, 'id': 1, 'val': 10}, {'dt': 2, 'id': 2, 'val': 20}],
                             debug=debug)
    assert success, msg

    unseen, update, delta = _assert_parity(pipe, [
        {'dt': 1, 'id': 1, 'val': 10},
        {'dt': 2, 'id': 2, 'val': 99},
        {'dt': 3, 'id': 3, 'val': 30},
    ])
    assert (len(unseen), len(update), len(delta)) == (1, 1, 2)


def test_filter_existing_polars_parity_no_indices():
    """Without index columns, every delta row is unseen."""
    pipe = mrsm.Pipe('test', 'filter_existing', 'pl_no_indices', instance=conns['sqlite'])
    pipe.delete()
    pipe = mrsm.Pipe('test', 'filter_existing', 'pl_no_indices', instance=conns['sqlite'])
    success, msg = pipe.sync([{'id': 1, 'val': 10}], debug=debug)
    assert success, msg

    unseen, update, delta = _assert_parity(pipe, [
        {'id': 1, 'val': 10},
        {'id': 2, 'val': 20},
    ])
    assert (len(unseen), len(update), len(delta)) == (1, 0, 1)


def test_filter_existing_polars_parity_json():
    """JSON columns round-trip through the native Polars path."""
    pipe = mrsm.Pipe('test', 'filter_existing', 'pl_json', instance=conns['sqlite'],
                     columns={'datetime': 'dt', 'id': 'id'}, dtypes={'meta': 'json'})
    pipe.delete()
    pipe = mrsm.Pipe('test', 'filter_existing', 'pl_json', instance=conns['sqlite'],
                     columns={'datetime': 'dt', 'id': 'id'}, dtypes={'meta': 'json'})
    success, msg = pipe.sync([
        {'dt': datetime(2021, 1, 1, tzinfo=timezone.utc), 'id': 1, 'meta': {'a': 1}},
    ], debug=debug)
    assert success, msg

    unseen, update, delta = _assert_parity(pipe, [
        {'dt': datetime(2021, 1, 1, tzinfo=timezone.utc), 'id': 1, 'meta': {'a': 2}},
        {'dt': datetime(2021, 1, 2, tzinfo=timezone.utc), 'id': 2, 'meta': {'b': 3}},
    ])
    assert (len(unseen), len(update), len(delta)) == (1, 1, 2)


def test_filter_existing_polars_include_unchanged_columns():
    """`include_unchanged_columns` joins the untouched columns onto the update frame."""
    pl = mrsm.attempt_import('polars')
    pipe = mrsm.Pipe('test', 'filter_existing', 'pl_unchanged', instance=conns['sqlite'],
                     columns={'datetime': 'dt', 'id': 'id'})
    pipe.delete()
    pipe = mrsm.Pipe('test', 'filter_existing', 'pl_unchanged', instance=conns['sqlite'],
                     columns={'datetime': 'dt', 'id': 'id'})
    success, msg = pipe.sync([
        {'dt': datetime(2021, 1, 1, tzinfo=timezone.utc), 'id': 1, 'a': 10, 'b': 'x'},
    ], debug=debug)
    assert success, msg

    incoming = pl.DataFrame([
        {'dt': datetime(2021, 1, 1, tzinfo=timezone.utc), 'id': 1, 'a': 99},
    ])
    _, update, _ = pipe.filter_existing(
        incoming,
        include_unchanged_columns=True,
        debug=debug,
    )
    assert isinstance(update, pl.DataFrame)
    assert update.to_dicts() == [{
        'dt': datetime(2021, 1, 1, tzinfo=timezone.utc),
        'id': 1,
        'b': 'x',
        'a': 99,
    }]


def test_filter_existing_polars_uuid_falls_back():
    """UUID columns cannot be joined natively, but Polars is still returned."""
    from uuid import UUID
    pl = mrsm.attempt_import('polars')
    pipe = mrsm.Pipe('test', 'filter_existing', 'pl_uuid', instance=conns['sqlite'],
                     columns={'datetime': 'dt', 'id': 'uid'}, dtypes={'uid': 'uuid'})
    pipe.delete()
    pipe = mrsm.Pipe('test', 'filter_existing', 'pl_uuid', instance=conns['sqlite'],
                     columns={'datetime': 'dt', 'id': 'uid'}, dtypes={'uid': 'uuid'})
    uid = UUID('00000000-0000-0000-0000-000000000001')
    success, msg = pipe.sync([
        {'dt': datetime(2021, 1, 1, tzinfo=timezone.utc), 'uid': uid, 'val': 10},
    ], debug=debug)
    assert success, msg

    incoming = pl.DataFrame([
        {'dt': datetime(2021, 1, 1, tzinfo=timezone.utc), 'uid': str(uid), 'val': 99},
    ])
    assert pipe._filter_existing_polars(incoming) is None, "Expected the Pandas fallback."

    unseen, update, delta = pipe.filter_existing(incoming, debug=debug)
    for _df in (unseen, update, delta):
        assert isinstance(_df, pl.DataFrame), f"Expected Polars, got {type(_df)}."
    assert (len(unseen), len(update), len(delta)) == (0, 1, 1)


def test_filter_existing_polars_no_pipe_data():
    """A Polars frame against an unsynced pipe treats every row as unseen."""
    pl = mrsm.attempt_import('polars')
    pipe = mrsm.Pipe('test', 'filter_existing', 'pl_no_data', instance=conns['sqlite'],
                     columns={'datetime': 'dt', 'id': 'id'})
    pipe.delete()
    pipe = mrsm.Pipe('test', 'filter_existing', 'pl_no_data', instance=conns['sqlite'],
                     columns={'datetime': 'dt', 'id': 'id'})

    incoming = pl.DataFrame([
        {'dt': datetime(2021, 1, 1, tzinfo=timezone.utc), 'id': 1, 'val': 10},
        {'dt': datetime(2021, 1, 2, tzinfo=timezone.utc), 'id': 2, 'val': 20},
    ])
    unseen, update, delta = pipe.filter_existing(incoming, debug=debug)
    for _df in (unseen, update, delta):
        assert isinstance(_df, pl.DataFrame), f"Expected Polars, got {type(_df)}."
    assert (len(unseen), len(update), len(delta)) == (2, 0, 2)


def test_polars_dtypes_reconcilable():
    """Only same-family dtype differences may be widened to a common dtype."""
    pl = mrsm.attempt_import('polars')
    from meerschaum.core.Pipe._sync import _polars_dtypes_reconcilable

    reconcilable = [
        (pl.Int64, pl.Int64),
        ### Pandas does not downcast, so Oracle's `int32` meets an incoming `int64`.
        (pl.Int64, pl.Int32),
        (pl.Int32, pl.Int64),
        (pl.Float32, pl.Float64),
        (pl.Datetime('us', 'UTC'), pl.Datetime('us', 'UTC')),
    ]
    irreconcilable = [
        ### Mixed numerics are coerced to `numeric` by the Pandas path.
        (pl.Int64, pl.Float64),
        (pl.String, pl.Int64),
        (pl.Int64, pl.String),
        (pl.Boolean, pl.Int64),
        ### Timezone-awareness and precision must match exactly.
        (pl.Datetime('us', 'UTC'), pl.Datetime('us', None)),
        (pl.Datetime('us', 'UTC'), pl.Datetime('ns', 'UTC')),
        (pl.Date, pl.Datetime('us', None)),
    ]
    for left, right in reconcilable:
        assert _polars_dtypes_reconcilable(left, right), f"{left} vs {right} should widen."
    for left, right in irreconcilable:
        assert not _polars_dtypes_reconcilable(left, right), f"{left} vs {right} should not widen."

    ### Widening must never narrow a value.
    for left, right in reconcilable:
        common = dict(pl.concat(
            [pl.DataFrame(schema={'c': left}), pl.DataFrame(schema={'c': right})],
            how='vertical_relaxed',
        ).schema)
        assert common['c'] in (left, right)

    big = pl.DataFrame({'c': [3_000_000_000]}, schema={'c': pl.Int64})
    small = pl.DataFrame({'c': [1]}, schema={'c': pl.Int32})
    common = dict(pl.concat([big.clear(), small.clear()], how='vertical_relaxed').schema)
    assert big.cast(common)['c'].to_list() == [3_000_000_000]
