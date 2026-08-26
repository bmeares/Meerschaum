#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Test functions from `meerschaum.utils.misc`.
"""

from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import uuid4
import pytest
from meerschaum.utils.packages import attempt_import
from meerschaum.utils.dtypes import MRSM_PD_DTYPES
DEBUG: bool = True
pd = attempt_import('pandas')


def test_polars_conversion():
    """Polars conversion is explicit and leaves Pandas inputs untouched."""
    pl = pytest.importorskip('polars')
    from meerschaum.utils.dataframe import to_pandas, to_polars

    pd_df = pd.DataFrame({'a': [1, None], 'b': ['x', 'y']})
    assert to_pandas(pd_df) is pd_df
    pl_df = to_polars(pd_df)
    assert isinstance(pl_df, pl.DataFrame)
    assert to_polars(pl_df) is pl_df
    for converted_df in (to_pandas(pl_df), to_pandas(pl_df.lazy())):
        assert converted_df['b'].tolist() == ['x', 'y']
        assert converted_df['a'].iloc[0] == 1
        assert pd.isna(converted_df['a'].iloc[1])


def test_polars_special_type_conversion():
    """Polars output preserves Meerschaum's Python-backed special values."""
    pl = pytest.importorskip('polars')
    from meerschaum.utils.dataframe import to_pandas, to_polars

    uuid_value = uuid4()
    pd_df = pd.DataFrame([{
        'dt': datetime(2025, 1, 1, tzinfo=timezone.utc),
        'date': date(2025, 1, 1),
        'decimal': Decimal('1.20'),
        'uuid': uuid_value,
        'bytes': b'x',
        'json': {'a': [1, None]},
    }])
    row = to_pandas(to_polars(pd_df)).iloc[0].to_dict()
    assert row == pd_df.iloc[0].to_dict()


def test_polars_enforce_all_string_dtypes():
    """Arrow-native dtype enforcement accepts string input and preserves Pandas compatibility."""
    pl = pytest.importorskip('polars')
    from meerschaum.utils.dataframe import enforce_dtypes

    raw_df = pd.DataFrame([{
        'int': '1',
        'float': '1.5',
        'bool': 'False',
        'str': 2,
        'numeric': '123.456',
        'bytes': 'Zm9vIGJhcg==',
        'date': '2025-01-01',
        'datetime': '2025-01-01T12:30:00Z',
    }])
    dtypes = {
        'int': 'int',
        'float': 'float',
        'bool': 'bool',
        'str': 'str',
        'numeric': 'numeric[5,2]',
        'bytes': 'bytes',
        'date': 'date',
        'datetime': 'datetime64[us, UTC]',
    }

    polars_df = enforce_dtypes(raw_df, dtypes, as_polars=True)
    pandas_df = enforce_dtypes(raw_df, dtypes)

    assert isinstance(polars_df, pl.DataFrame)
    assert polars_df.row(0, named=True) == {
        'int': 1,
        'float': 1.5,
        'bool': False,
        'str': '2',
        'numeric': Decimal('123.46'),
        'bytes': b'foo bar',
        'date': date(2025, 1, 1),
        'datetime': datetime(2025, 1, 1, 12, 30, tzinfo=timezone.utc),
    }
    assert pandas_df.iloc[0].to_dict() == polars_df.row(0, named=True)


def test_polars_enforce_mixed_special_dtypes():
    """Special dtypes retain Python values in a Polars result."""
    pl = pytest.importorskip('polars')
    from meerschaum.utils.dataframe import enforce_dtypes

    result = enforce_dtypes(
        pd.DataFrame([{
            'id': '1',
            'json': '{"foo": "bar"}',
            'uuid': '12345678-1234-5678-1234-567812345678',
        }]),
        {'id': 'int', 'json': 'json', 'uuid': 'uuid'},
        as_polars=True,
    )
    row = result.row(0, named=True)
    assert isinstance(result, pl.DataFrame)
    assert row['id'] == 1
    assert row['json'] == {'foo': 'bar'}
    assert str(row['uuid']) == '12345678-1234-5678-1234-567812345678'


def test_polars_enforcement_preserves_untyped_objects():
    """Accelerating declared columns must not change untyped object columns."""
    from meerschaum.utils.dataframe import enforce_dtypes

    result = enforce_dtypes(
        pd.DataFrame([{'id': '1', 'payload': {'foo': 'bar'}}]),
        {'id': 'int'},
    )
    assert result.iloc[0].to_dict() == {'id': 1, 'payload': {'foo': 'bar'}}
    assert str(result.dtypes['payload']) == 'object'


@pytest.mark.parametrize(
    "dtype,expected_dtype",
    [
        ('object', None),
        ('bool', 'bool[pyarrow]'),
        ('bytes', 'binary[pyarrow]'),
        ('bool[pyarrow]', None),
        ('float64', None),
        ('datetime64[ns]', None),
        ('datetime', MRSM_PD_DTYPES['datetime']),
        ('int', 'int64[pyarrow]'),
        ('int64', 'int64[pyarrow]'),
        ('int32', 'int32[pyarrow]'),
        ('int16', 'int16[pyarrow]'),
        ('int8', 'int8[pyarrow]'),
        ('int64[pyarrow]', None),
        ('datetime64[ns, UTC]', None),
    ]
)
def test_add_missing_cols_to_df(dtype: str, expected_dtype):
    """
    Test that new columns are successfully added to a dataframe.
    """
    from meerschaum.utils.dataframe import add_missing_cols_to_df
    expected_dtype = expected_dtype or dtype
    df = pd.DataFrame([{'foo': 'bar'}])
    new_df = add_missing_cols_to_df(df, {'baz': dtype})
    assert len(new_df.columns) == 2
    assert str(new_df.dtypes['baz']) == expected_dtype


@pytest.mark.parametrize(
    'old_docs,new_docs,expected_docs',
    [
        (
            [
                {'a': 1, 'b': 1},
            ], [
                {'a': 1},
            ], [
                {'a': 1},
            ]
        ),
        (
            [
                {'a': 1,},
            ], [
                {'a': 1},
            ], []
        ),
        (
            [
                {'a': datetime(2022, 1, 1), 'b': 100.0},
            ], [
                {'a': datetime(2022, 1, 1), 'b': 100.0},
            ], []
        ),
        (
            [
                {'a': 1, 'b': 1},
            ], [], []
        ),
        (
            [
                {'a': 'foo', 'b': 'bar'},
            ], [
                {'a': 'foo', 'b': 'bar'},
            ], []
        ),
        (
            [
                {'a': False, 'b': 'bar'},
            ], [
                {'a': False, 'b': 'bar'},
            ], []
        ),
        (
            [], [], []
        ),
        (
            [{'a': None}], [{'a': None}], []
        ),
        (
            pd.DataFrame([{'a': 1}], dtype='int64[pyarrow]'),
            pd.DataFrame([{'a': 1}], dtype='int32'),
            []
        )
    ]
)
def test_filter_unseen_df(old_docs, new_docs, expected_docs):
    """
    Test that duplicate rows are removed.
    """
    from meerschaum.utils.dataframe import filter_unseen_df
    old_df = pd.DataFrame(old_docs) if not isinstance(old_docs, pd.DataFrame) else old_docs
    new_df = pd.DataFrame(new_docs) if not isinstance(new_docs, pd.DataFrame) else new_docs
    delta_df = filter_unseen_df(old_df, new_df)
    assert delta_df.to_dict(orient='records') == expected_docs


@pytest.mark.parametrize(
    'old_df,new_df',
    [
        (
            pd.DataFrame({
                'id': pd.Series([1, None, 3], dtype='int64[pyarrow]'),
                'value': ['same', 'old', 'same'],
            }),
            pd.DataFrame({
                'id': pd.Series([1, None, 4], dtype='int64[pyarrow]'),
                'value': ['same', 'new', 'new'],
            }),
        ),
        (
            pd.DataFrame([{
                'dt': datetime(2025, 1, 1, tzinfo=timezone.utc),
                'uuid': uuid4(),
                'numeric': Decimal('1.20'),
                'bytes': b'old',
                'json': {'a': [1, None]},
            }]),
            pd.DataFrame([{
                'dt': datetime(2025, 1, 1, tzinfo=timezone.utc),
                'uuid': uuid4(),
                'numeric': Decimal('2.30'),
                'bytes': b'new',
                'json': {'a': [2, None]},
            }]),
        ),
        (
            pd.DataFrame({'id': [1, 1, 9], 'value': [None, None, 'same']}),
            pd.DataFrame({
                'id': [3, 1, 1, 2, 9],
                'value': ['new-3', None, None, 'new-2', 'same'],
            }),
        ),
    ],
)
def test_filter_unseen_df_polars_parity(monkeypatch, old_df, new_df):
    """The Polars anti-join matches the Pandas path, including its safe fallback."""
    pl = pytest.importorskip('polars')
    import meerschaum.utils.dataframe as dataframe

    monkeypatch.setattr(dataframe, '_POLARS_FILTER_MIN_ROWS', 10 ** 9)
    expected = dataframe.filter_unseen_df(old_df, new_df)
    monkeypatch.setattr(dataframe, '_POLARS_FILTER_MIN_ROWS', 0)
    actual = dataframe.filter_unseen_df(old_df, new_df)

    pd.testing.assert_frame_equal(actual, expected)


def test_filter_unseen_df_polars_preserves_source_order(monkeypatch):
    """The anti-join retains source order, including around duplicate null rows."""
    pytest.importorskip('polars')
    import meerschaum.utils.dataframe as dataframe

    monkeypatch.setattr(dataframe, '_POLARS_FILTER_MIN_ROWS', 0)
    old_df = pd.DataFrame({'id': [1, 1, 9], 'value': [None, None, 'same']})
    new_df = pd.DataFrame({
        'id': [3, 1, 1, 2, 9],
        'value': ['new-3', None, None, 'new-2', 'same'],
    })
    original_filter = dataframe._filter_unseen_df_with_polars
    native_results = []

    def capture_native_result(*args, **kwargs):
        native_result = original_filter(*args, **kwargs)
        native_results.append(native_result)
        return native_result

    monkeypatch.setattr(dataframe, '_filter_unseen_df_with_polars', capture_native_result)
    result = dataframe.filter_unseen_df(old_df, new_df)
    assert native_results[0] is not None
    assert result['id'].tolist() == [3, 2]


def test_filter_unseen_df_mixed_datetime_backends():
    """Native and Arrow-backed Pandas datetimes represent the same rows."""
    from meerschaum.utils.dataframe import filter_unseen_df

    native_dt = pd.Series(pd.to_datetime(['2025-01-01'], utc=True))
    arrow_dt = native_dt.astype('timestamp[us, tz=UTC][pyarrow]')
    assert filter_unseen_df(
        pd.DataFrame({'dt': arrow_dt}),
        pd.DataFrame({'dt': native_dt}),
        dtypes={'dt': 'datetime64[us, UTC]'},
    ).empty


def test_filter_unseen_df_polars_matches_boolean_dtype(monkeypatch):
    """The accelerated path preserves Pandas' mixed-backend dtype resolution."""
    pytest.importorskip('polars')
    import meerschaum.utils.dataframe as dataframe

    old_df = pd.DataFrame({'id': [1], 'value': ['NA'], 'flag': [False]})
    new_df = pd.DataFrame({'id': [1, 2], 'value': ['NA', 'new'], 'flag': [False, True]})

    monkeypatch.setattr(dataframe, '_POLARS_FILTER_MIN_ROWS', 10 ** 9)
    expected = dataframe.filter_unseen_df(old_df, new_df)
    monkeypatch.setattr(dataframe, '_POLARS_FILTER_MIN_ROWS', 0)
    actual = dataframe.filter_unseen_df(old_df, new_df)

    pd.testing.assert_frame_equal(actual, expected)


def test_filter_unseen_df_polars_falls_back(monkeypatch):
    """A Polars conversion error cleanly selects the established Pandas path."""
    pl = pytest.importorskip('polars')
    import meerschaum.utils.dataframe as dataframe

    def fail_conversion(*args, **kwargs):
        raise TypeError("unsupported value")

    monkeypatch.setattr(dataframe, '_POLARS_FILTER_MIN_ROWS', 0)
    monkeypatch.setattr(pl, 'from_pandas', fail_conversion)
    old_df = pd.DataFrame({'id': [1]})
    new_df = pd.DataFrame({'id': [1, 2]})
    assert dataframe._filter_unseen_df_with_polars(new_df, old_df) is None
    assert dataframe.filter_unseen_df(old_df, new_df)['id'].tolist() == [2]


def test_filter_unseen_df_numeric_precision_scale():
    """Configured numeric precision and scale are applied independently."""
    from meerschaum.utils.dataframe import filter_unseen_df

    result = filter_unseen_df(
        pd.DataFrame({'value': [Decimal('1.00')]}),
        pd.DataFrame({'value': ['2.345']}),
        dtypes={'value': 'numeric[5,2]'},
    )
    assert result.iloc[0]['value'] == Decimal('2.35')


@pytest.mark.parametrize(
    'df,expected_types,expected_tuples',
    [
        (
            pd.DataFrame({
                'dt_tz_aware': [datetime(2025, 1, 1, tzinfo=timezone.utc)],
                'distant_dt': [datetime(1, 1, 1)],
                'dt_second': pd.to_datetime([datetime(2025, 1, 1)]).astype('datetime64[s]'),
                'other': [1],
            }),
            {
                'dt_tz_aware': 'datetime64[us, UTC]',
                'distant_dt': 'datetime64[us]',
                'dt_second': 'datetime64[s]',
            },
            {
                'dt_tz_aware': ('UTC', 'microsecond'),
                'distant_dt': (None, 'microsecond'),
                'dt_second': (None, 'second'),
            },
        ),
        (
            pd.DataFrame({
                'dt': pd.Series(
                    [datetime(2025, 1, 1, tzinfo=timezone.utc)],
                    dtype='timestamp[us, tz=UTC][pyarrow]',
                ),
            }),
            {'dt': 'datetime64[us, UTC]'},
            {'dt': ('UTC', 'microsecond')},
        ),
    ]
)
def test_get_datetime_cols_types(df, expected_types, expected_tuples):
    """
    Test that datetime columns are correctly identified along with their types.
    """
    from meerschaum.utils.dataframe import get_datetime_cols, get_datetime_cols_types
    dt_cols_types = get_datetime_cols_types(df)
    assert dt_cols_types == expected_types

    dt_cols_tuples = get_datetime_cols(df, with_tz_precision=True)
    assert dt_cols_tuples == expected_tuples
