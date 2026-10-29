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
from tests._binary import skip_or_fail
DEBUG: bool = True
pd = attempt_import('pandas')


def _polars():
    """polars, which CI installs: missing, it fails the test in CI and skips it locally."""
    try:
        import polars
    except ImportError:
        skip_or_fail('polars is not installed')
    return polars


def test_polars_conversion():
    """Polars conversion is explicit and leaves Pandas inputs untouched."""
    pl = _polars()
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
    pl = _polars()
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


def test_polars_geometry_uses_geoarrow_wkb():
    """Polars uses GeoArrow WKB while Pandas retains GeoPandas geometry."""
    pytest.importorskip('geoarrow.types')
    import json
    pl = _polars()
    shapely = attempt_import('shapely')
    geopandas = attempt_import('geopandas')
    from meerschaum.utils.dataframe import enforce_dtypes

    raw_df = pd.DataFrame({'id': ['1', '2'], 'geom': ['POINT (1 2)', None]})
    dtypes = {'id': 'int', 'geom': 'geometry[Point, 4326]'}
    polars_df = enforce_dtypes(raw_df, dtypes, as_polars=True)
    geometry_dtype = polars_df.schema['geom']

    assert geometry_dtype.ext_name() == 'geoarrow.wkb'
    assert geometry_dtype.ext_storage() == pl.Binary
    assert json.loads(geometry_dtype.ext_metadata()) == {
        'crs': 'EPSG:4326',
        'crs_type': 'authority_code',
    }
    assert shapely.from_wkb(polars_df['geom'][0]).equals(shapely.Point(1, 2))
    assert polars_df['geom'][1] is None
    assert 'extension<geoarrow.wkb' in str(polars_df.to_arrow().schema.field('geom').type)

    pandas_df = enforce_dtypes(raw_df, dtypes)
    assert isinstance(pandas_df, geopandas.GeoDataFrame)
    assert pandas_df['geom'].iloc[0].equals(shapely.Point(1, 2))


def test_polars_enforce_all_string_dtypes():
    """Arrow-native dtype enforcement accepts string input and preserves Pandas compatibility."""
    pl = _polars()
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
    """JSON uses canonical Arrow storage while Pandas retains Python values."""
    pl = _polars()
    from meerschaum.utils.dataframe import enforce_dtypes, to_pandas

    result = enforce_dtypes(
        pd.DataFrame([
            {
                'id': '1',
                'json': '{"foo": "bar"}',
                'uuid': '12345678-1234-5678-1234-567812345678',
            },
            {'id': '2', 'json': [1, None], 'uuid': None},
            {'id': '3', 'json': None, 'uuid': None},
        ]),
        {'id': 'int', 'json': 'json', 'uuid': 'uuid'},
        as_polars=True,
    )
    assert isinstance(result, pl.DataFrame)
    assert result.schema['json'].ext_name() == 'arrow.json'
    assert result.schema['json'].ext_storage() == pl.String
    assert result['json'].to_list() == ['{"foo":"bar"}', '[1,null]', None]
    assert 'extension<arrow.json' in str(result.to_arrow().schema.field('json').type)
    for transformed in (result.filter(pl.col('id') == 1), pl.concat([result, result])):
        assert transformed.schema['json'].ext_name() == 'arrow.json'

    pandas_result = to_pandas(result)
    assert pandas_result['json'].tolist() == [{'foo': 'bar'}, [1, None], None]
    assert str(pandas_result['uuid'][0]) == '12345678-1234-5678-1234-567812345678'


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
    'dtype,values,pandas_reprs,polars_dtype,polars_values',
    [
        (
            'numeric[0,0]', [Decimal('1.5'), Decimal('0')],
            ["Decimal('1.5')", "Decimal('0')"], 'Decimal(2, 1)', [Decimal('1.5'), Decimal('0.0')],
        ),
        (
            'numeric[5,10]', [Decimal('1.5'), Decimal('0')],
            ["Decimal('1.5')", "Decimal('0')"], 'Decimal(2, 1)', [Decimal('1.5'), Decimal('0.0')],
        ),
        ('numeric[0,0]', [None, None], ["Decimal('NaN')", "Decimal('NaN')"], 'Null', [None, None]),
        ('numeric[5,10]', [None, None], ["Decimal('NaN')", "Decimal('NaN')"], 'Null', [None, None]),
        (
            'numeric[0,0]', [Decimal('0.000012345'), None],
            ["Decimal('0.000012345')", "Decimal('NaN')"], 'Decimal(9, 9)', [Decimal('0.000012345'), None],
        ),
        (
            'numeric[5,10]', [Decimal('0.000012345'), None],
            ["Decimal('0.000012345')", 'None'], 'Decimal(9, 9)', [Decimal('0.000012345'), None],
        ),
    ],
)
def test_enforce_numeric_outside_polars_decimals_is_unbounded(
    dtype, values, pandas_reprs, polars_dtype, polars_values,
):
    """
    `numeric[p,s]` that is not a Polars decimal (precision 0, or a scale above the precision) is an
    unbounded numeric, as in v4.1.4: v4's native `polars.Decimal(p, s)` cast raises, so the values
    stay as they are (pandas, with v4's null quirks) or are typed from the values (`as_polars`).
    Expected values recorded from `meerschaum==4.1.4`.
    """
    pl = _polars()
    from meerschaum.utils.dataframe import enforce_dtypes

    df = pd.DataFrame({'a': values})
    result = enforce_dtypes(df, {'a': dtype})
    assert str(result.dtypes['a']) == 'object'
    assert [repr(value) for value in result['a']] == pandas_reprs

    polars_result = enforce_dtypes(df, {'a': dtype}, as_polars=True)
    expected_dtype = (
        pl.Null if polars_dtype == 'Null'
        else pl.Decimal(*map(int, polars_dtype[len('Decimal('):-1].split(',')))
    )
    assert polars_result.schema['a'] == expected_dtype
    assert polars_result['a'].to_list() == polars_values


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
def test_filter_unseen_df_matches_a_row_set_reference(old_df, new_df):
    """The unseen rows are exactly the new rows no old row equals, in source order."""
    import json
    from meerschaum.utils.dataframe import filter_unseen_df

    def _norm(value):
        if isinstance(value, (dict, list)):
            return json.dumps(value, sort_keys=True, default=str)
        if value is None or (not isinstance(value, (str, bytes)) and pd.isna(value)):
            return None
        return value if not hasattr(value, 'isoformat') else value.isoformat()

    def _rows(df):
        return [tuple(_norm(v) for v in row) for row in df.itertuples(index=False)]

    old_rows = set(_rows(old_df))
    expected = [row for row in _rows(new_df) if row not in old_rows]
    assert _rows(filter_unseen_df(old_df, new_df)) == expected


def test_filter_unseen_df_preserves_source_order_around_duplicate_null_rows():
    """The anti-join retains source order, including around duplicate null rows."""
    from meerschaum.utils.dataframe import filter_unseen_df

    old_df = pd.DataFrame({'id': [1, 1, 9], 'value': [None, None, 'same']})
    new_df = pd.DataFrame({
        'id': [3, 1, 1, 2, 9],
        'value': ['new-3', None, None, 'new-2', 'same'],
    })
    assert filter_unseen_df(old_df, new_df)['id'].tolist() == [3, 2]


def test_filter_unseen_df_keeps_boolean_columns():
    """Boolean columns come back as booleans."""
    from meerschaum.utils.dataframe import filter_unseen_df

    old_df = pd.DataFrame({'id': [1], 'value': ['NA'], 'flag': [False]})
    new_df = pd.DataFrame({'id': [1, 2], 'value': ['NA', 'new'], 'flag': [False, True]})
    result = filter_unseen_df(old_df, new_df)
    assert result['id'].tolist() == [2]
    assert result['flag'].tolist() == [True]
    assert str(result['flag'].dtype).startswith('bool')


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
    if int(pd.__version__.split('.')[0]) < 3:
        pytest.skip(f"datetime units are those of pandas 3 (pandas {pd.__version__} is installed)")
    dt_cols_types = get_datetime_cols_types(df)
    assert dt_cols_types == expected_types

    dt_cols_tuples = get_datetime_cols(df, with_tz_precision=True)
    assert dt_cols_tuples == expected_tuples
