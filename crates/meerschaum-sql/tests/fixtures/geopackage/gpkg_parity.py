#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
The Python half of the GeoPackage parity test (`tests/instance_geopackage.rs`).

Run with `PYTHONPATH=<repo root>` and empty temporary `MRSM_ROOT_DIR` / `MRSM_PLUGINS_DIR`:

    python gpkg_parity.py sync <db> <scenario.json>   # v4: sync every pipe into a GeoPackage
    python gpkg_parity.py read <db> <scenario.json>   # v4: pipe.get_data() of every pipe
    python gpkg_parity.py ogr <db> <scenario.json>    # GDAL (geopandas/pyogrio): every layer

`ogr` reads a copy of the file with `gpkg_ogr_contents.feature_count` reset to NULL: v4 leaves
the count of the empty frame it registers (0) in place, so GDAL would report no features for
v4's tables (the Rust connector writes NULL; see `instance/geopackage.rs`).

All output is JSON on the last line of stdout.
"""

import json
import shutil
import sqlite3
import sys
import tempfile
import warnings
from datetime import datetime


def decode(value):
    if isinstance(value, dict):
        if '$dt' in value:
            return datetime.fromisoformat(value['$dt'])
        if '$wkt' in value:
            import shapely
            return shapely.from_wkt(value['$wkt'])
    return value


def load(scenario_path):
    with open(scenario_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def get_pipe(conn, spec):
    import meerschaum as mrsm
    return mrsm.Pipe(
        spec['connector_keys'], spec['metric_key'], spec['location_key'],
        instance=conn,
        parameters=json.loads(json.dumps(spec['parameters'])),
    )


def get_conn(db_path):
    import meerschaum as mrsm
    return mrsm.get_connector('sql', 'gpkg_parity', flavor='geopackage', database=db_path)


def do_sync(db_path, scenario_path):
    conn = get_conn(db_path)
    out = []
    for spec in load(scenario_path)['pipes']:
        pipe = get_pipe(conn, spec)
        docs = [{k: decode(v) for k, v in doc.items()} for doc in spec['docs']]
        out.append(list(pipe.sync(docs)))
    return out


def geom_str(value):
    if value is None:
        return None
    try:
        import pandas as pd
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value.wkt if hasattr(value, 'wkt') else str(value)


def do_read(db_path, scenario_path):
    conn = get_conn(db_path)
    out = {}
    for spec in load(scenario_path)['pipes']:
        pipe = get_pipe(conn, spec)
        df = pipe.get_data()
        geom_cols = [c for c, t in pipe.dtypes.items() if str(t).startswith('geometry')]
        out[pipe.target] = {
            'rows': [
                {
                    col: (geom_str(val) if col in geom_cols else (None if val is None else str(val)))
                    for col, val in doc.items()
                }
                for doc in df.to_dict(orient='records')
            ],
            'crs': str(getattr(df, 'crs', None)),
        }
    return out


def do_ogr(db_path, scenario_path):
    import geopandas as gpd
    import meerschaum as mrsm
    tmp_dir = tempfile.mkdtemp()
    try:
        copy = f"{tmp_dir}/copy.gpkg"
        shutil.copy(db_path, copy)
        con = sqlite3.connect(copy)
        con.execute("UPDATE gpkg_ogr_contents SET feature_count = NULL")
        con.commit()
        con.close()
        out = {}
        for spec in load(scenario_path)['pipes']:
            target = mrsm.Pipe(spec['connector_keys'], spec['metric_key'], spec['location_key']).target
            with warnings.catch_warnings():
                warnings.simplefilter('ignore')
                gdf = gpd.read_file(copy, layer=target)
            out[target] = {
                'crs': gdf.crs.to_string() if gdf.crs is not None else None,
                'dtypes': {col: str(typ) for col, typ in gdf.dtypes.items()},
                'geometry_name': gdf.geometry.name,
                'rows': [
                    {
                        col: (geom_str(val) if col == gdf.geometry.name else str(val))
                        for col, val in doc.items()
                    }
                    for doc in gdf.to_dict(orient='records')
                ],
            }
        return out
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == '__main__':
    mode = sys.argv[1]
    if mode == 'sync':
        result = do_sync(sys.argv[2], sys.argv[3])
    elif mode == 'read':
        result = do_read(sys.argv[2], sys.argv[3])
    elif mode == 'ogr':
        result = do_ogr(sys.argv[2], sys.argv[3])
    else:
        raise ValueError(f"Unknown mode '{mode}'.")
    print(json.dumps(result, default=str))
