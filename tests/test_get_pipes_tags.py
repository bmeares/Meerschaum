"""
The tag filter of `get_pipes()` / `fetch_pipes_keys()` on every SQL flavor.

The filter reads the stored `parameters` as text, so it has to see past the first characters: MSSQL's
bare `VARCHAR` is `VARCHAR(30)`, which once hid every pipe whose `tags` follow a `columns` block
(compose projects select pipes by `-t <project>`).
"""

import pytest

import meerschaum as mrsm

from tests.connectors import conns, get_flavors

TAG = 'w17bo_kitchen'


@pytest.mark.parametrize("flavor", get_flavors())
def test_tags_are_found_after_long_parameters(flavor: str):
    conn = conns[flavor]
    if conn.type != 'sql':
        pytest.skip("SQL instances only")

    columns = {'datetime': 'a_long_datetime_column', 'id': 'a_long_identifier_column'}
    dtypes = {'a_long_value_column': 'float'}
    pipes = [
        mrsm.Pipe(
            'w17bo_tags', metric, instance=conn,
            columns=columns, dtypes=dtypes, tags=tags,
        )
        for metric, tags in (
            ('kitchen_a', [TAG, 'w17bo_extra']),
            ('kitchen_b', [TAG]),
            ('garage', ['w17bo_garage']),
        )
    ]
    for pipe in pipes:
        pipe.delete()
    try:
        for pipe in pipes:
            success, msg = pipe.register()
            assert success, msg

        def metrics(**kwargs):
            found = mrsm.get_pipes(
                connector_keys=['w17bo_tags'], instance=conn, as_list=True, **kwargs,
            )
            return sorted(pipe.metric_key for pipe in found)

        assert metrics(tags=[TAG]) == ['kitchen_a', 'kitchen_b']
        assert metrics(tags=[f'{TAG},w17bo_extra']) == ['kitchen_a']
        assert metrics(tags=[TAG, 'w17bo_garage']) == ['garage', 'kitchen_a', 'kitchen_b']
        assert metrics(tags=[f'_{TAG}']) == ['garage']
        assert metrics(tags=['w17bo_nothing']) == []
    finally:
        for pipe in pipes:
            pipe.delete()
