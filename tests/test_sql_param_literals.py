"""SQL filter values remain literal across reads, row counts, and deletes."""

import uuid
import os

import pytest

import meerschaum as mrsm


@pytest.fixture
def pipe(tmp_path):
    conn = mrsm.get_connector(
        'sql', f'param_literals_{uuid.uuid4().hex}',
        flavor='sqlite', database=str(tmp_path / 'params.db'),
    )
    return mrsm.Pipe(
        'test', 'param_literals', instance=conn,
        columns={'datetime': 'id'},
        dtypes={'id': 'int', 'payload': 'json', 'someWHERE': 'str'},
    )


@pytest.mark.parametrize('bounds', [{}, {'begin': 0, 'end': 3}])
def test_where_keyword_in_filter_value_and_column(pipe, bounds):
    rows = [
        {'id': 1, 'someWHERE': 'someWHERE', 'payload': {'value': 1}},
        {'id': 2, 'someWHERE': 'other', 'payload': {'value': 2}},
    ]
    assert pipe.sync(rows)[0]
    params = {'someWHERE': 'someWHERE'}
    assert pipe.get_data(['id'], params=params, as_docs=True, **bounds) == [{'id': 1}]
    assert pipe.get_rowcount(params=params, **bounds) == 1


@pytest.mark.parametrize('value', ["O'Brien", "a' OR 1 = 1 OR 'b"])
def test_json_filter_quotes_do_not_change_the_predicate(pipe, value):
    rows = [
        {'id': 1, 'someWHERE': 'first', 'payload': {'value': value}},
        {'id': 2, 'someWHERE': 'other', 'payload': {'value': 'other'}},
    ]
    assert pipe.sync(rows)[0]
    params = {'payload': {'value': value}}
    assert pipe.get_data(['id'], params=params, as_docs=True) == [{'id': 1}]
    assert pipe.get_rowcount(params=params) == 1
    assert pipe.get_sync_time(params=params) == 1
    success, message = pipe.clear(params=params)
    assert success, message
    assert pipe.get_data(['id'], as_docs=True) == [{'id': 2}]


def test_null_marker_identifiers_are_text(pipe):
    """
    rc15 Q18 (v4.1.4 added a SQL NULL row on the second sync): a declared text ID 'None' is the
    text 'None', so re-syncing it changes nothing, and `params={'id': 'None'}` matches it.
    """
    marker = mrsm.Pipe(
        'test', 'null_marker', instance=pipe.instance_connector,
        columns={'id': 'id'}, dtypes={'id': 'str', 'value': 'int'},
    )
    rows = [{'id': 'None', 'value': 1}]
    assert marker.sync(rows)[0]
    assert marker.get_data(as_docs=True) == rows
    for _ in range(2):
        assert marker.sync(rows)[0]
        assert marker.get_data(as_docs=True) == rows
    assert marker.get_data(params={'id': 'None'}, as_docs=True) == rows
    assert marker.get_rowcount(params={'id': None}) == 0


@pytest.mark.parametrize('value', ['a;b', 'a -- comment', 'DROP table'])
@pytest.mark.parametrize('bounds', [{}, {'begin': 0, 'end': 3}])
def test_rejected_filter_never_broadens_reads_or_deletes(pipe, value, bounds):
    rows = [
        {'id': 1, 'someWHERE': value, 'payload': {'value': 1}},
        {'id': 2, 'someWHERE': 'other', 'payload': {'value': 2}},
    ]
    assert pipe.sync(rows)[0]
    params = {'someWHERE': value}
    assert pipe.get_data(['id'], params=params, as_docs=True, **bounds) == []
    assert pipe.get_rowcount(params=params, **bounds) == 0
    assert pipe.get_sync_time(params=params) is None
    assert pipe.clear(params=params, **bounds)[0]
    assert pipe.get_rowcount() == 2


@pytest.mark.parametrize('flavor', ['mysql', 'mariadb'])
def test_mysql_backslash_filters_do_not_broaden_reads_or_deletes(flavor):
    if flavor not in os.environ.get('MRSM_TEST_FLAVORS', 'sqlite').split(','):
        pytest.skip(f'{flavor} test service not selected')
    from tests.connectors import conns

    pipe = mrsm.Pipe(
        'test', f'param_backslash_{uuid.uuid4().hex[:8]}', instance=conns[flavor],
        columns={'id': 'id'}, dtypes={'id': 'int', 'value': 'str'},
    )
    value = r"\' OR 1 = 1 #"
    try:
        assert pipe.sync([{'id': 1, 'value': value}, {'id': 2, 'value': 'other'}])[0]
        for param in [value, [value]]:
            params = {'value': param}
            assert pipe.get_data(['id'], params=params, as_docs=True) == [{'id': 1}]
            assert pipe.get_rowcount(params=params) == 1
        assert pipe.clear(params={'value': value})[0]
        assert pipe.get_data(['id'], as_docs=True) == [{'id': 2}]
    finally:
        pipe.delete()
