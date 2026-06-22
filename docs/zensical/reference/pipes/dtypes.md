# 🔢 Data Types

This page explains what Meerschaum data types (dtypes) are, when to use each one, and how dtype coercion behaves during syncs. For the dtype table in context with the rest of the metadata, see [`parameters.dtypes`](/reference/pipes/parameters/#dtypes).

## What dtypes are

A pipe's dtypes map each column to a storage type. They live under the `dtypes` key of the [`parameters`](/reference/pipes/parameters/) dictionary and are surfaced through the convenience attribute `Pipe.dtypes`.

```python
import meerschaum as mrsm

pipe = mrsm.Pipe(
    'demo', 'dtypes',
    instance='sql:memory',
    columns={'datetime': 'ts', 'id': 'station'},
    dtypes={
        'ts': 'datetime',
        'station': 'int',
        'reading': 'numeric',
    },
)

print(pipe.dtypes)
# {'ts': 'datetime', 'station': 'int', 'reading': 'numeric'}
```

If you don't set dtypes explicitly, they are **inferred from the first sync** and persisted into `parameters['dtypes']`. Explicitly setting them up front avoids surprises (e.g. an integer column that later receives a float).

!!! note "Generic or specific"
    A dtype value may be a base Meerschaum dtype (e.g. `int`, `numeric`) **or** any Pandas dtype string (e.g. `Int64`, `int32[pyarrow]`, `float64`, `bool`, `object`, `datetime64[ms, UTC]`). The base dtypes are portable across database flavors; specific Pandas dtypes give you finer control over storage width.

## Supported dtypes

The base Meerschaum dtypes and the SQL types they map to are below. The authoritative mapping lives in [`meerschaum/utils/dtypes/sql.py`](https://github.com/bmeares/Meerschaum/blob/main/meerschaum/utils/dtypes/sql.py) (`get_db_type_from_pd_type` and `get_pd_type_from_db_type`).

| Meerschaum dtype | Maps to (SQL) | When to use |
|---|---|---|
| `datetime` | `TIMESTAMPTZ` / `DATETIMEOFFSET` (tz-aware, coerced to UTC) | Timezone-aware timestamps. The typical `datetime` axis for incremental syncing. |
| `datetime64[<precision>]` | `TIMESTAMP` / `TIMESTAMP WITHOUT TIME ZONE` | Timezone-naive timestamps (e.g. `datetime64[us]`). |
| `date` | `DATE` | Calendar dates with no time component. |
| `int` | `BIGINT` / `INT` | Whole numbers. Also used for an **integer** `datetime` axis (epoch units). |
| `float` | `DOUBLE PRECISION` / `FLOAT` | Approximate decimals where rounding error is acceptable (measurements, ratios). |
| `numeric`, `numeric[precision,scale]` | `NUMERIC` / `DECIMAL` | Exact decimals (money, identifiers). `numeric[10,2]` fixes precision and scale. |
| `bool` | `BOOL` / `BIT` (`INT` on Oracle/MSSQL/MySQL, `FLOAT` on SQLite) | True/false flags. |
| `string` | `TEXT` (`NVARCHAR(MAX)` MSSQL, `NVARCHAR2(2000)` Oracle) | Text. |
| `uuid` | `UUID` where supported (`UNIQUEIDENTIFIER` MSSQL, otherwise `TEXT`) | UUID identifiers. |
| `json` | `JSONB` on PostgreSQL-like flavors, otherwise `TEXT` | Nested `dict` / `list` documents. |
| `bytes` | `BYTEA` / `BLOB` / `VARBINARY` (otherwise base64-encoded `TEXT`) | Binary blobs. |
| `geometry`, `geometry[type,srid]`, `geography` | PostGIS `GEOMETRY` / `GEOGRAPHY` (otherwise base64-encoded WKB) | Spatial data. See [Geometry](#geometry-postgis) below. |

!!! note "Aliases"
    Several friendly aliases resolve to the base dtypes: `decimal` / `number` → `numeric`, `binary` / `blob` / `bytea` → `bytes`, `guid` → `uuid`, `geom` → `geometry`, `geog` → `geography`, `boolean` → `bool`.

## `numeric` vs `float` vs `int`

- **`int`** — exact whole numbers. Use for counts and integer keys.
- **`float`** — IEEE-754 double. Fast and compact, but subject to binary rounding error. Use for measurements where exactness isn't required.
- **`numeric`** — arbitrary-precision `Decimal`. Use when exactness matters (currency, precise identifiers) or when a column mixes integers and decimals. Optionally pin precision and scale with `numeric[precision,scale]`.

By default, a column that starts as `int` and later receives float values is promoted to `numeric` (so no data is lost). This is governed by the [`mixed_numerics`](/reference/pipes/parameters/#mixed_numerics) parameter:

```python
pipe = mrsm.Pipe(
    'demo', 'numerics',
    instance='sql:memory',
    columns={'datetime': 'ts'},
    dtypes={'val': 'int'},
    parameters={'mixed_numerics': False},  # keep 'val' as int; do not promote to numeric
)
```

With `mixed_numerics=False`, the int→float promotion is suppressed, preventing a schema change on the target table (similar to [`static`](/reference/pipes/parameters/#static)).

## Precision (datetime units)

The `precision` parameter sets the granularity of the `datetime` axis and the value captured by [`autotime`](/reference/pipes/parameters/#autotime). Supported units (and aliases):

| Unit | Aliases |
|---|---|
| `nanosecond` | `ns` |
| `microsecond` | `us` |
| `millisecond` | `ms` |
| `second` | `s`, `sec` |
| `minute` | `m`, `min` |
| `hour` | `h`, `hr` |
| `day` | `d`, `D` |

By default the dtype determines the precision — e.g. the default `datetime64[us, UTC]` is microsecond precision. Set it explicitly under `parameters['precision']`:

```python
pipe = mrsm.Pipe(
    'demo', 'precision',
    instance='sql:memory',
    columns={'datetime': 'ts'},
    parameters={'precision': 'second'},
)
```

See [`parameters.precision`](/reference/pipes/parameters/#precision) for rounding intervals and `round_to`.

## Geometry (PostGIS)

Spatial columns use the `geometry` dtype, optionally qualified with a geometry type and/or SRID: `geometry[srid]`, `geometry[type,srid]`, or `geography`. When no SRID is given, the default is `EPSG:4326` (WGS 84).

```python
import meerschaum as mrsm

pipe = mrsm.Pipe(
    'demo', 'spatial',
    instance='sql:main',  # a PostGIS-enabled PostgreSQL connector
    columns={'datetime': 'ts', 'id': 'station'},
    dtypes={'location': 'geometry[POINT, 4326]'},
)
pipe.sync([{'ts': '2025-01-01', 'station': 1, 'location': 'POINT (-82.4 34.85)'}])
```

Values may be `shapely` objects, WKT strings, or WKB (hex / GPKG) bytes — they are coerced automatically.

!!! warning "Native geometry requires PostGIS"
    Only **PostGIS**-enabled PostgreSQL instances store these as true `GEOMETRY` / `GEOGRAPHY` columns. On other flavors the geometry is serialized to base64-encoded WKB and stored as text — spatial queries on the database side are not available there.

## Coercion and enforcement

During [syncing](/reference/pipes/syncing/), incoming data is cast to the pipe's registered dtypes by `Pipe.enforce_dtypes()` before being written. This is controlled by the [`enforce`](/reference/pipes/parameters/#enforce) parameter (default `True`):

- **`enforce=True`** (default) — every incoming DataFrame is coerced to match `parameters['dtypes']`.
- **`enforce=False`** — coercion is skipped for performance. Only use this when your source already produces clean, correctly-typed data.

When a sync introduces a **new column** or a value that conflicts with the inferred dtype, Meerschaum auto-alters the target table — adding the column or widening the type (e.g. `int` → `numeric`, or a numeric column → `string` if text arrives). This automatic schema evolution is disabled by [`static=True`](/reference/pipes/parameters/#static), in which case new columns and type changes are rejected instead.

## Inspecting and changing dtypes

Read the current dtypes with `Pipe.dtypes` (or `pipe.parameters['dtypes']`). To change a dtype on an existing pipe, edit the parameter and persist:

```python
import meerschaum as mrsm

pipe = mrsm.Pipe('demo', 'dtypes', instance='sql:main')
pipe.dtypes['reading'] = 'numeric'
pipe.edit()           # persist the change
# or: pipe.update_parameters({'dtypes': {'reading': 'numeric'}}, persist=True)
```

!!! warning "Changing a dtype is not a backfill"
    Editing a dtype updates the metadata and affects how *future* rows are written; it does not retroactively rewrite existing rows. To rebuild a column's stored values under the new type, re-sync the historical range (e.g. `Pipe.verify()` or `sync pipes --begin ...`).

## Common dtype issues

- **A column became `numeric` (or `string`) unexpectedly.** A later sync sent a float into an `int` column (promoted to `numeric`), or text into a numeric column (promoted to `string`). Set the dtype explicitly up front, or use [`mixed_numerics=False`](/reference/pipes/parameters/#mixed_numerics) / [`static=True`](/reference/pipes/parameters/#static) to lock the schema.
- **`Unknown Pandas data type '...'. Falling back to 'TEXT'.`** The dtype string isn't recognized. Use a base Meerschaum dtype or a valid Pandas dtype string.
- **Timezone offsets disappeared / shifted.** The `datetime` dtype is tz-aware and coerces everything to UTC. For tz-naive storage, use `datetime64[us]` (no timezone).
- **Geometry stored as a base64 text blob.** The instance isn't PostGIS-enabled — see [Geometry](#geometry-postgis).
- **Schema won't change when I expect it to.** `static=True` blocks new columns and type changes; remove it (or set `enforce=True`) to allow schema evolution.

---------------

See the [full API reference](https://docs.meerschaum.io/meerschaum/utils/dtypes.html) for the underlying dtype utilities.
