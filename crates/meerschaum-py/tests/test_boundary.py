"""Strict dataframe boundary regression tests; all data is synthetic."""
import decimal
import uuid

import pytest

import meerschaum._core as core
from meerschaum._core import _gate

pytestmark = pytest.mark.skipif(not core.AVAILABLE, reason='Rust extension is required')


@pytest.fixture(autouse=True)
def restore_gate():
    yield
    _gate.reload()


@pytest.mark.parametrize('infer_string', [False, True])
@pytest.mark.parametrize('partial_dtypes', [False, True])
@pytest.mark.parametrize('old_values,new_values', [
    (['a'], ['b', None]),
    (['a'], ['b']),
    (['a'], [None]),
    (['a'], ['a']),
    (['a'], []),
    ([None], ['b']),
    ([None], [None]),
    (['a', None], ['b', None]),
    (['a', 1], ['b', None, 2]),
])
def test_explicit_object_string_inference(infer_string, partial_dtypes, old_values, new_values):
    """Infer using both complete merge inputs, including null-only and empty deltas."""
    pd = pytest.importorskip('pandas')
    from meerschaum.utils.dataframe import filter_unseen_df
    with pd.option_context('future.infer_string', infer_string):
        old = pd.DataFrame({'s': pd.Series(old_values, dtype=object)})
        new = pd.DataFrame({'s': pd.Series(new_values, dtype=object)})
        old_before, new_before = old.copy(deep=True), new.copy(deep=True)
        options = {'dtypes': {'unrelated': 'object'}} if partial_dtypes else {}
        _gate.set_mode('python')
        expected = filter_unseen_df(old, new, **options)
        _gate.set_mode('rust')
        actual = filter_unseen_df(old, new, **options)
    pd.testing.assert_frame_equal(actual, expected)
    for got, want in zip(actual.s, expected.s):
        if want is pd.NA:
            assert got is pd.NA
    pd.testing.assert_frame_equal(old, old_before)
    pd.testing.assert_frame_equal(new, new_before)


@pytest.mark.parametrize('infer_string', [False, True])
@pytest.mark.parametrize('declared', [False, True, 'partial'])
@pytest.mark.parametrize('kind', ['uuid', 'bytes'])
@pytest.mark.parametrize('new_has_value', [False, True])
def test_special_object_null_identity(infer_string, declared, kind, new_has_value):
    """UUID/bytes nulls use pd.NA; serialized JSON and Decimal keep their sentinels."""
    pd = pytest.importorskip('pandas')
    from meerschaum.utils.dataframe import filter_unseen_df
    old_value = uuid.UUID(int=1) if kind == 'uuid' else b'a'
    new_value = uuid.UUID(int=2) if kind == 'uuid' else b'b'
    with pd.option_context('future.infer_string', infer_string):
        old = pd.DataFrame({
            's': pd.Series([old_value], dtype=object),
            'j': pd.Series([{'x': 1}], dtype=object),
            'n': pd.Series([decimal.Decimal('1.1')], dtype=object),
        })
        new = pd.DataFrame({
            's': pd.Series([None, new_value] if new_has_value else [None], dtype=object),
            'j': pd.Series([None, {'x': 2}] if new_has_value else [None], dtype=object),
            'n': pd.Series([None, decimal.Decimal('2.2')] if new_has_value else [None], dtype=object),
        })
        types = {'s': kind, 'j': 'json', 'n': 'numeric'}
        options = (
            {'dtypes': types} if declared is True
            else {'dtypes': {'n': 'numeric'}} if declared == 'partial'
            else {}
        )
        _gate.set_mode('python')
        expected = filter_unseen_df(old, new, **options)
        _gate.set_mode('rust')
        actual = filter_unseen_df(old, new, **options)
    pd.testing.assert_frame_equal(actual, expected)
    sentinel = pd.NA if new_has_value else None
    assert expected.s.iloc[0] is sentinel
    assert actual.s.iloc[0] is sentinel
    json_sentinel = None if new_has_value else pd.NA
    assert actual.j.iloc[0] is expected.j.iloc[0] is json_sentinel
    assert actual.n.iloc[0].is_nan()


@pytest.mark.parametrize('kind', ['uuid', 'bytes'])
@pytest.mark.parametrize('infer_string', [False, True])
@pytest.mark.parametrize('declared', [False, True])
def test_special_object_detector_uses_first_value_in_mixed_delta(kind, infer_string, declared):
    pd = pytest.importorskip('pandas')
    from meerschaum.utils.dataframe import filter_unseen_df
    old_value, new_value = (uuid.UUID(int=1), uuid.UUID(int=2)) if kind == 'uuid' else (b'a', b'b')
    with pd.option_context('future.infer_string', infer_string):
        old = pd.DataFrame({'s': pd.Series([old_value], dtype=object)})
        new = pd.DataFrame({'s': pd.Series([new_value, None, 'text'], dtype=object)})
        options = {'dtypes': {'s': 'object'}} if declared else {}
        _gate.set_mode('python')
        expected = filter_unseen_df(old, new, **options)
        _gate.set_mode('rust')
        actual = filter_unseen_df(old, new, **options)
    pd.testing.assert_frame_equal(actual, expected)
    assert actual.s.iloc[1] is expected.s.iloc[1] is pd.NA
    assert actual.s.iloc[0] == new_value
    assert type(actual.s.iloc[0]) is type(new_value)
    assert actual.s.iloc[2] == 'text'


@pytest.mark.parametrize('kind', ['uuid', 'bytes', 'json'])
def test_special_serialization_null_only_delta_from_nonnull_input(kind):
    """JSON serialization depends on the full new input; UUID/bytes parsing uses the delta."""
    pd = pytest.importorskip('pandas')
    from meerschaum.utils.dataframe import filter_unseen_df
    value = {'uuid': uuid.UUID(int=1), 'bytes': b'a', 'json': {'x': 1}}[kind]
    old = pd.DataFrame({'s': pd.Series([value], dtype=object)})
    new = pd.DataFrame({'s': pd.Series([None, value], dtype=object)})
    _gate.set_mode('python')
    expected = filter_unseen_df(old, new)
    _gate.set_mode('rust')
    actual = filter_unseen_df(old, new)
    pd.testing.assert_frame_equal(actual, expected)
    assert actual.s.tolist() == [None]
    assert actual.s.iloc[0] is expected.s.iloc[0] is None


@pytest.mark.parametrize('kind', ['uuid', 'bytes'])
@pytest.mark.parametrize('declared', [False, True])
def test_special_object_empty_delta_dtype(kind, declared):
    pd = pytest.importorskip('pandas')
    from meerschaum.utils.dataframe import filter_unseen_df
    value = uuid.UUID(int=1) if kind == 'uuid' else b'a'
    old = pd.DataFrame({'s': pd.Series([value], dtype=object)})
    new = pd.DataFrame({'s': pd.Series([], dtype=object)})
    options = {'dtypes': {'s': kind}} if declared else {}
    _gate.set_mode('python')
    expected = filter_unseen_df(old, new, **options)
    _gate.set_mode('rust')
    actual = filter_unseen_df(old, new, **options)
    pd.testing.assert_frame_equal(actual, expected)


@pytest.mark.parametrize('lazy', [False, True])
@pytest.mark.parametrize('kind', ['string', 'uuid', 'bytes', 'decimal'])
def test_polars_object_values_never_become_pointer_bytes(lazy, kind):
    pl = pytest.importorskip('polars')
    old_value, new_value = {
        'string': ('a', 'b'),
        'uuid': (uuid.UUID(int=1), uuid.UUID(int=2)),
        'bytes': (b'a', b'b'),
        'decimal': (decimal.Decimal('1.1'), decimal.Decimal('2.2')),
    }[kind]
    old = pl.DataFrame({'s': pl.Series([old_value], dtype=pl.Object), 'id': [1]})
    new = pl.DataFrame({'s': pl.Series([new_value, None], dtype=pl.Object), 'id': [2, 3]})
    actual = core.filter_unseen_df(old.lazy() if lazy else old, new.lazy() if lazy else new)
    assert isinstance(actual, pl.DataFrame)
    assert actual.columns == ['s', 'id']
    assert actual['id'].to_list() == [2, 3]
    assert actual['s'].dtype == pl.Object
    assert actual['s'][0] == new_value
    assert type(actual['s'][0]) is type(new_value)
    assert actual['s'][1] is None


def test_generated_polars_uuid_output_can_be_used_again():
    pd = pytest.importorskip('pandas')
    pl = pytest.importorskip('polars')
    value = uuid.UUID(int=1)
    first = core.enforce_dtypes(pd.DataFrame({'u': [str(value), None]}), {'u': 'uuid'}, as_polars=True)
    assert first['u'].dtype == pl.Object
    second = core.enforce_dtypes(first, {'u': 'uuid'}, as_polars=True)
    assert second['u'].to_list() == [value, None]


@pytest.mark.parametrize('lazy', [False, True])
@pytest.mark.parametrize('kind', [
    'opaque', 'tuple', 'bytearray', 'bigint', 'numpy_int', 'numpy_float', 'numpy_str', 'int', 'dict',
])
def test_polars_arbitrary_objects_raise_before_export(lazy, kind):
    pl = pytest.importorskip('polars')
    np = pytest.importorskip('numpy')
    value = {
        'opaque': object(), 'tuple': (1, 2), 'bytearray': bytearray(b'a'),
        'bigint': 2 ** 80, 'numpy_int': np.int64(1), 'numpy_float': np.float32(1.5),
        'numpy_str': np.str_('a'), 'int': 1, 'dict': {'x': 1},
    }[kind]
    frame = pl.DataFrame({'opaque': pl.Series([value], dtype=pl.Object)})
    with pytest.raises(TypeError, match="Polars Object column 'opaque'.*unsupported"):
        core.query_df(frame.lazy() if lazy else frame)


@pytest.mark.parametrize('kind', ['Null', 'String', 'Binary', 'Object'])
def test_polars_object_tracking_preserves_native_dtypes(kind):
    pl = pytest.importorskip('polars')
    old_value, new_value = {'Null': (None, None), 'Object': (None, None),
                            'String': ('a', 'b'), 'Binary': (b'a', b'b')}[kind]
    dtype = getattr(pl, kind)
    old = pl.DataFrame({'s': pl.Series([old_value], dtype=dtype), 'id': [1]})
    new = pl.DataFrame({'s': pl.Series([new_value, None], dtype=dtype), 'id': [2, 3]})
    actual = core.filter_unseen_df(old, new)
    assert actual['s'].dtype == dtype
    assert actual['s'].to_list() == [new_value, None]
    assert actual['id'].to_list() == [2, 3]
