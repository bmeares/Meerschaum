#! /usr/bin/env python3
# vim:fenc=utf-8

"""
Generate `dtypes.json`: `are_dtypes_equal` over a matrix of dtype strings, `is_dtype_special`,
`is_dtype_numeric`, `get_geometry_type_srid` (with the canonical string built the way
`get_pd_type_from_db_type` builds it), numeric precision/scale parsing, and precision units.

Run from the repository root:

    PYTHONPATH=$(pwd) python crates/meerschaum-core/tests/fixtures/gen_dtypes.py
"""

import json
import pathlib
import sys

HERE = pathlib.Path(__file__).parent
sys.path.insert(0, str(HERE))
import _stubs  # noqa: E402

_stubs.isolate()

from meerschaum.utils.dtypes import (  # noqa: E402
    are_dtypes_equal, is_dtype_special, is_dtype_numeric, get_geometry_type_srid,
    get_next_precision_unit, MRSM_PRECISION_UNITS_ALIASES, MRSM_PRECISION_UNITS_SCALARS,
)
from meerschaum.utils.dtypes.sql import get_numeric_precision_scale  # noqa: E402

DTYPES = [
    'datetime', 'datetime64[ns]', 'datetime64[ms, UTC]', 'datetime64[us, UTC]', 'timestamp',
    'int', 'Int64', 'int32', 'int64[pyarrow]', 'uint8', 'Int32', 'bigint',
    'float', 'float64', 'double', 'float32', 'Float64',
    'numeric', 'numeric[10,2]', 'decimal', 'Decimal', 'decimal128(10, 2)[pyarrow]', 'number',
    'str', 'string', 'large_string[pyarrow]', 'string[python]', 'object',
    'json', 'JSON', 'jsonl',
    'uuid', 'UUID', 'guid',
    'bytes', 'binary', 'blob', 'binary[pyarrow]', 'large_binary',
    'bool', 'boolean', 'bool[pyarrow]',
    'date', 'date32[day][pyarrow]', 'date64', 'day',
    'geometry', 'geometry[Point, 4326]', 'geography', 'geom', 'geog',
    'category', '',
]

GEOMETRY = [
    'geometry', 'geometry[]', 'geometry[MULTIPOLYGON, 6570]', 'geometry[3361]',
    'geometry[0, Point]', 'geometry[multipolygon, 4326]', 'geometry[LINESTRINGZ, 2264]',
    'geometry[MULTIPOLYGONZ]', 'geometry[EPSG:4326]', 'geometry[ESRI:102003]',
    'geometry[POINT, ESRI:102003]', 'geometry[srid=4326, type=Point]', 'geography',
    'geography[POINT]', 'geography[Point, 4326]', 'GEOMETRY(POINT, 4326)',
    'GEOMETRY(MULTILINESTRING Z, 3857)', 'geometry[, 4326]', 'geometry[4326.0]',
    'geometry[1e3]', 'geom[Polygon, 26917]',
]

NUMERIC = ['numeric', 'numeric[10,2]', 'numeric[ 38 , 9 ]', 'NUMERIC(10, 2)', 'numeric[10]',
           'numeric[a,b]', 'numeric[1,2,3]']


def main():
    pairs = []
    for l in DTYPES:
        for r in DTYPES:
            pairs.append([l, r, are_dtypes_equal(l, r)])
    special = {d: is_dtype_special(d) for d in DTYPES}
    numeric = {d: is_dtype_numeric(d) for d in DTYPES}
    geometry = []
    for d in GEOMETRY:
        try:
            typ, srid = get_geometry_type_srid(d)
            modifiers = [str(m) for m in (typ, srid) if m]
            kind = 'geography' if 'geography' in d.lower() or d.lower().startswith('geog') else 'geometry'
            canonical = kind if not modifiers else f"{kind}[{', '.join(modifiers)}]"
            geometry.append({'dtype': d, 'type': typ, 'srid': srid, 'canonical': canonical})
        except Exception as e:
            geometry.append({'dtype': d, 'error': type(e).__name__})
    numeric_ps = {d: list(get_numeric_precision_scale(None, d)) for d in NUMERIC}
    units = {}
    for u in list(MRSM_PRECISION_UNITS_SCALARS) + list(MRSM_PRECISION_UNITS_ALIASES):
        entry = {}
        for decrease in (True, False):
            try:
                entry[str(decrease)] = get_next_precision_unit(u, decrease=decrease)
            except ValueError:
                entry[str(decrease)] = None
        units[u] = entry
    with open(HERE / 'dtypes.json', 'w', encoding='utf-8') as f:
        json.dump({
            'are_dtypes_equal': pairs,
            'is_dtype_special': special,
            'is_dtype_numeric': numeric,
            'geometry': geometry,
            'numeric_precision_scale': numeric_ps,
            'next_precision_unit': units,
        }, f, indent=1)
        f.write('\n')


if __name__ == '__main__':
    main()
