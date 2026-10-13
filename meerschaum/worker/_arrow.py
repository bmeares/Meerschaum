#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
DataFrames across the worker boundary.

- `to_payload(obj)` normalizes one fetch result (or one chunk of a generator) into a
  `(kind, value, frames)` triple: DataFrames, GeoDataFrames and Polars frames become a single
  Arrow IPC stream (`kind='arrow'`); lists of dicts travel as typed MessagePack documents
  (`'docs'`), dicts of lists as columns (`'columns'`) and strings as text (`'text'`), so the Rust
  kernel can apply v4's own `parse_df_datetimes` / `enforce_dtypes` rules to them.
- `ipc_to_df(data)` turns an Arrow IPC stream from the host back into a pandas DataFrame for
  custom instance connectors (`sync_pipe(pipe, df)`), restoring the special columns from their
  field tags.

Special pandas columns are tagged with the field metadata the kernel reads
(`meerschaum:dtype`, `ARROW:extension:name`, `ARROW:extension:metadata`):
JSON (`arrow.json`), UUID (`arrow.uuid`), numeric (`meerschaum:dtype = numeric`), bytes and
geometry (`geoarrow.wkb` with the CRS).
"""

from __future__ import annotations

import json
from decimal import Decimal
from uuid import UUID
from typing import Any, Dict, List, Optional, Tuple

DTYPE_KEY = b'meerschaum:dtype'
EXT_NAME_KEY = b'ARROW:extension:name'
EXT_META_KEY = b'ARROW:extension:metadata'


def _pa():
    """`pyarrow`, imported the way v4 imports it (`attempt_import`, `venv='mrsm'`), so the worker
    and plugin code share one module."""
    from meerschaum.utils.packages import attempt_import
    return attempt_import('pyarrow', lazy=False)


def _pd():
    """`pandas` via v4's `import_pandas()`."""
    from meerschaum.utils.packages import import_pandas
    return import_pandas()


def _is_na(value: Any) -> bool:
    """`pd.isna()` for scalars (containers are never NA)."""
    if value is None:
        return True
    if isinstance(value, float):
        return value != value
    return type(value).__name__ in ('NAType', 'NaTType')


def _first_valid(values) -> Any:
    for value in values:
        if not _is_na(value):
            return value
    return None


def _json_default(value: Any) -> Any:
    try:
        from meerschaum.utils.dtypes import json_serialize_value
        return json_serialize_value(value)
    except Exception:
        return str(value)


def _crs_metadata(crs: Any) -> str:
    if crs is None:
        return '{}'
    try:
        authority = crs.to_authority()
    except Exception:
        authority = None
    if authority:
        return json.dumps({'crs': f"{authority[0]}:{authority[1]}", 'crs_type': 'authority_code'})
    return '{}'


def _object_column(name: str, series) -> Tuple[Any, Dict[bytes, bytes]]:
    """Convert a pandas `object` column, tagging special kinds."""
    pa = _pa()
    values = series.tolist()
    first = _first_valid(values)
    meta: Dict[bytes, bytes] = {}

    if isinstance(first, (dict, list)):
        arr = pa.array(
            [
                (None if _is_na(v) else json.dumps(v, default=_json_default, separators=(',', ':')))
                for v in values
            ],
            pa.string(),
        )
        meta = {DTYPE_KEY: b'json', EXT_NAME_KEY: b'arrow.json', EXT_META_KEY: b''}
        return arr, meta

    if isinstance(first, Decimal):
        arr = pa.array([(None if _is_na(v) else str(v)) for v in values], pa.string())
        return arr, {DTYPE_KEY: b'numeric'}

    if isinstance(first, UUID):
        arr = pa.array([(None if _is_na(v) else str(v)) for v in values], pa.string())
        return arr, {DTYPE_KEY: b'uuid'}

    if isinstance(first, (bytes, bytearray, memoryview)):
        arr = pa.array([(None if _is_na(v) else bytes(v)) for v in values], pa.binary())
        return arr, {DTYPE_KEY: b'bytes'}

    if first is not None and (type(first).__module__ or '').startswith('shapely'):
        arr = pa.array(
            [(None if _is_na(v) or v is None else v.wkb) for v in values],
            pa.binary(),
        )
        return arr, {
            DTYPE_KEY: b'geometry',
            EXT_NAME_KEY: b'geoarrow.wkb',
            EXT_META_KEY: b'{}',
        }

    try:
        return pa.array(series, from_pandas=True), meta
    except Exception:
        ### Mixed object columns degrade to their `str()` values.
        return pa.array([(None if _is_na(v) else str(v)) for v in values], pa.string()), meta


def pandas_to_table(df) -> Any:
    """Convert a pandas (or GeoPandas) DataFrame into a `pyarrow.Table`."""
    pa = _pa()
    arrays, fields = [], []
    for col in df.columns:
        name = str(col)
        series = df[col]
        if isinstance(series, type(df)):
            ### Duplicate column names: keep the first.
            series = series.iloc[:, 0]
        dtype_name = str(getattr(series, 'dtype', ''))
        meta: Dict[bytes, bytes] = {}
        if dtype_name == 'geometry':
            arr = pa.array(
                [(None if v is None or _is_na(v) else v.wkb) for v in series.tolist()],
                pa.binary(),
            )
            meta = {
                DTYPE_KEY: b'geometry',
                EXT_NAME_KEY: b'geoarrow.wkb',
                EXT_META_KEY: _crs_metadata(getattr(series, 'crs', None)).encode('utf-8'),
            }
        elif dtype_name == 'object':
            arr, meta = _object_column(name, series)
        else:
            try:
                arr = pa.Array.from_pandas(series)
            except Exception:
                arr, meta = _object_column(name, series.astype('object'))
        arrays.append(arr)
        fields.append(pa.field(name, arr.type, nullable=True, metadata=(meta or None)))
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))


def to_table(df) -> Any:
    """Convert any supported DataFrame type into a `pyarrow.Table`."""
    pa = _pa()
    module = type(df).__module__ or ''
    if isinstance(df, pa.Table):
        return df
    if isinstance(df, pa.RecordBatch):
        return pa.Table.from_batches([df])
    if module.startswith('polars'):
        if hasattr(df, 'collect') and not hasattr(df, 'to_arrow'):
            df = df.collect()
        try:
            import polars as pl
            return df.to_arrow(compat_level=pl.CompatLevel.oldest())
        except Exception:
            return df.to_arrow()
    if module.startswith('dask'):
        df = df.compute()
    if hasattr(df, '__arrow_c_stream__') and not hasattr(df, 'iloc'):
        return pa.table(df)
    return pandas_to_table(df)


def table_to_ipc(table) -> bytes:
    """Serialize a table as one Arrow IPC stream."""
    pa = _pa()
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def df_to_ipc(df) -> bytes:
    """Serialize any supported DataFrame as one Arrow IPC stream."""
    return table_to_ipc(to_table(df))


def is_dataframe(obj: Any) -> bool:
    """Whether `obj` is a DataFrame of a supported library."""
    type_str = str(type(obj))
    return 'DataFrame' in type_str or 'LazyFrame' in type_str or 'pyarrow.lib.Table' in type_str


def to_payload(obj: Any) -> Tuple[str, Any, List[bytes]]:
    """
    Normalize one fetch result into `(kind, value, frames)`.

    `kind` is one of `'arrow'`, `'docs'`, `'columns'`, `'text'`, or `'none'`.
    """
    if obj is None:
        return 'none', None, []
    if isinstance(obj, str):
        return 'text', obj, []
    if isinstance(obj, dict):
        return 'columns', {
            str(k): (list(v) if isinstance(v, (list, tuple)) else (
                v.tolist() if hasattr(v, 'tolist') else [v]
            ))
            for k, v in obj.items()
        }, []
    if isinstance(obj, (list, tuple)):
        return 'docs', [
            ({str(k): v for k, v in doc.items()} if isinstance(doc, dict) else doc)
            for doc in obj
        ], []
    if is_dataframe(obj) or hasattr(obj, '__arrow_c_stream__'):
        return 'arrow', None, [df_to_ipc(obj)]
    raise TypeError(f"Unsupported fetch result of type {type(obj)}.")


### -------------------------------------------------------------------------------------------
### Host -> Python
### -------------------------------------------------------------------------------------------

def _int_mapper():
    pd = _pd()
    pa = _pa()
    return {
        pa.int8(): pd.Int8Dtype(),
        pa.int16(): pd.Int16Dtype(),
        pa.int32(): pd.Int32Dtype(),
        pa.int64(): pd.Int64Dtype(),
        pa.uint8(): pd.UInt8Dtype(),
        pa.uint16(): pd.UInt16Dtype(),
        pa.uint32(): pd.UInt32Dtype(),
        pa.uint64(): pd.UInt64Dtype(),
        pa.bool_(): pd.BooleanDtype(),
    }


def ipc_to_table(data: bytes) -> Any:
    """Read one Arrow IPC stream into a `pyarrow.Table`."""
    pa = _pa()
    reader = pa.ipc.open_stream(pa.py_buffer(data))
    return reader.read_all()


def table_to_df(table) -> Any:
    """
    Convert a table from the host into a pandas DataFrame, restoring JSON, UUID, numeric and
    geometry columns as Python objects (GeoDataFrame when GeoPandas is importable).
    """
    pd = _pd()
    pa = _pa()
    special: Dict[str, Tuple[str, Dict[bytes, bytes]]] = {}
    plain_fields = []
    plain_arrays = []
    for i, field in enumerate(table.schema):
        meta = field.metadata or {}
        ext = meta.get(EXT_NAME_KEY, b'').decode('utf-8')
        tag = meta.get(DTYPE_KEY, b'').decode('utf-8').split('[')[0]
        kind = {
            'arrow.json': 'json',
            'arrow.uuid': 'uuid',
            'geoarrow.wkb': 'geometry',
        }.get(ext) or (tag if tag in ('json', 'uuid', 'numeric', 'geometry', 'geography') else None)
        column = table.column(i)
        if kind:
            special[field.name] = (kind, meta)
        plain_fields.append(pa.field(field.name, column.type))
        plain_arrays.append(column)

    mapper = _int_mapper()
    df = pa.Table.from_arrays(plain_arrays, schema=pa.schema(plain_fields)).to_pandas(
        types_mapper=mapper.get,
    )
    geometry_cols = []
    crs = None
    for name, (kind, meta) in special.items():
        values = table.column(name).to_pylist()
        if kind == 'json':
            parsed = [(json.loads(v) if isinstance(v, str) else v) for v in values]
        elif kind == 'uuid':
            parsed = [
                (UUID(bytes=v) if isinstance(v, bytes) else (UUID(v) if isinstance(v, str) else v))
                for v in values
            ]
        elif kind == 'numeric':
            parsed = [(Decimal(str(v)) if v is not None else None) for v in values]
        else:
            try:
                from meerschaum.utils.packages import attempt_import
                shapely = attempt_import('shapely', lazy=False, install=False)
                parsed = [
                    (shapely.from_wkb(v) if isinstance(v, bytes) else (
                        shapely.from_wkt(v) if isinstance(v, str) else v
                    ))
                    for v in values
                ]
                geometry_cols.append(name)
                ext_meta = meta.get(EXT_META_KEY, b'{}').decode('utf-8') or '{}'
                crs = json.loads(ext_meta).get('crs', None) or crs
            except Exception:
                parsed = values
        df[name] = pd.Series(parsed, dtype='object', index=df.index)

    if geometry_cols:
        try:
            from meerschaum.utils.packages import attempt_import
            geopandas = attempt_import('geopandas', lazy=False, install=False)
            return geopandas.GeoDataFrame(df, geometry=geometry_cols[0], crs=crs)
        except Exception:
            pass
    return df


def ipc_to_df(data: bytes) -> Any:
    """Read one Arrow IPC stream into a pandas DataFrame."""
    return table_to_df(ipc_to_table(data))
