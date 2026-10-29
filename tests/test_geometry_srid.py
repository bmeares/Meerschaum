"""Stored geometry SRIDs and GeoDataFrame CRS across the Python boundary (v4.1.4 parity)."""

from uuid import uuid4

import pytest

import meerschaum as mrsm
from meerschaum.utils.sql import sql_item_name
from tests.connectors import conns, get_flavors


_FLAVORS = {'sqlite', 'postgresql', 'timescaledb', 'timescaledb_ha', 'postgis',
            'mysql', 'mariadb', 'mssql', 'duckdb'}


def _frames():
    gpd = pytest.importorskip('geopandas')
    shapely = pytest.importorskip('shapely')
    return [
        gpd.GeoDataFrame(
            {'id': ids, 'geom': [shapely.Point(x, y) for x, y in points]},
            geometry='geom', crs='EPSG:4326',
        )
        for ids, points in [
            ([1, 2], [(1, 2), (3, 4)]),
            ([1, 3], [(11, 2), (5, 6)]),
        ]
    ]


@pytest.mark.parametrize('flavor', [f for f in get_flavors() if f in _FLAVORS])
@pytest.mark.parametrize('dtype', ['geometry', 'geometry[Point, 4326]', None])
def test_geometry_insert_update_srid(flavor, dtype):
    """An update and an append preserve v4's storage and return the updated geometry."""
    frames = _frames()
    conn = conns[flavor]
    pipe = mrsm.Pipe(
        'w17cgeo', 'srid', uuid4().hex[:10], instance=conn,
        columns={'id': 'id'},
        dtypes={'geom': dtype} if dtype else {},
    )
    try:
        for frame, expected in zip(frames, ['Inserted 2, updated 0', 'Inserted 1, updated 1']):
            result = pipe.sync(frame)
            assert result[0], result[1]
            assert expected in result[1]

        table = sql_item_name(pipe.target, conn.flavor, schema=conn.get_pipe_schema(pipe))
        if conn.flavor in ('postgis', 'timescaledb-ha'):
            stored = conn.read(f'SELECT id, ST_SRID(geom) AS srid FROM {table} ORDER BY id')
            expected_srid = 0 if dtype == 'geometry' else 4326
            assert stored['id'].tolist() == [1, 2, 3]
            assert stored['srid'].tolist() == [expected_srid] * 3
        else:
            stored = conn.read(f'SELECT id, geom FROM {table} ORDER BY id')
            assert stored['id'].tolist() == [1, 2, 3]
            for value in stored['geom']:
                if conn.flavor == 'mssql':
                    assert value.startswith('POINT (')
                else:
                    # Plain WKB point: no EWKB flag carrying an SRID.
                    assert value.startswith('0101000000') and len(value) == 42

        data = pipe.get_data().sort_values('id')
        assert data['id'].tolist() == [1, 2, 3]
        assert [(point.x, point.y) for point in data['geom']] == [(11, 2), (3, 4), (5, 6)]
        if dtype != 'geometry':
            assert data.crs.to_epsg() == 4326
    finally:
        pipe.delete()


@pytest.mark.parametrize('flavor', [f for f in get_flavors() if f in ('postgis', 'timescaledb_ha')])
def test_geography_second_sync_matches_v4(flavor):
    """Keep v4's geography-to-TEXT conversion and the per-row SRIDs it leaves behind."""
    frames = _frames()
    conn = conns[flavor]
    pipe = mrsm.Pipe(
        'w17cgeo', 'geography', uuid4().hex[:10], instance=conn,
        columns={'id': 'id'}, dtypes={'geom': 'geography[Point, 4326]'},
    )
    try:
        for frame in frames:
            result = pipe.sync(frame)
            assert result[0], result[1]
        table = sql_item_name(pipe.target, conn.flavor, schema=conn.get_pipe_schema(pipe))
        stored = conn.read(f'SELECT id, ST_SRID(geom) AS srid FROM {table} ORDER BY id')
        assert stored['id'].tolist() == [1, 2, 3]
        # The untouched geography value retains EWKB; the update and append use plain WKB.
        assert stored['srid'].tolist() == [0, 4326, 0]
    finally:
        pipe.delete()


def test_geopackage_registers_frame_crs(tmp_path):
    """A bare geometry dtype keeps the frame CRS in GeoPackage's metadata, as GDAL does."""
    frames = _frames()
    conn = mrsm.get_connector(
        'sql', 'w17cgeo_' + uuid4().hex[:10], flavor='geopackage',
        database=str(tmp_path / 'geometry.gpkg'),
    )
    pipe = mrsm.Pipe(
        'w17cgeo', 'metadata', instance=conn,
        columns={'id': 'id'}, dtypes={'geom': 'geometry'},
    )
    try:
        for frame in frames:
            result = pipe.sync(frame)
            assert result[0], result[1]
        assert conn.value('SELECT srs_id FROM gpkg_geometry_columns') == 4326
        table = sql_item_name(pipe.target, conn.flavor)
        headers = conn.read(f'SELECT hex(substr(geom, 1, 8)) AS header FROM {table}')
        # PARITY: v4 serialize_geometry(..., 'gpkg_wkb') uses `srid or -1` for the blob header.
        assert headers['header'].tolist() == ['47500001FFFFFFFF'] * 3
    finally:
        pipe.delete()
