"""Synthetic frames shared by the v4 builder and the v5 continuation (imported by both interpreters)."""
import datetime
import decimal
import uuid

from meerschaum.utils.packages import attempt_import
pd = attempt_import('pandas', lazy=False)

BASE = datetime.datetime(2024, 1, 1)

KITCHEN_DTYPES = {
    'dt': 'datetime',
    'dtz': 'datetime64[ns, UTC]',
    'd': 'date',
    'j': 'json',
    'u': 'uuid',
    'n': 'numeric',
    'by': 'bytes',
}


def kitchen_df(ids, shift=0, geometry=False):
    rows = []
    for i in ids:
        row = {
            'id': i,
            'dt': BASE + datetime.timedelta(hours=i),
            'i': i * 10 + shift,
            'f': i * 1.5 + shift,
            'b': i % 2 == 0,
            's': f'str-{i}-{shift}',
            'dtz': datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc) + datetime.timedelta(hours=i),
            'd': (BASE + datetime.timedelta(days=i)).date(),
            'j': {'k': i, 'v': [1, 2, shift]},
            'u': uuid.UUID(int=i + 1),
            'n': decimal.Decimal(f'{i}.123456') + shift,
            'by': bytes([i % 256, 1, 2]),
        }
        rows.append(row)
    df = pd.DataFrame(rows)
    if geometry:
        shapely_geometry = attempt_import('shapely.geometry', lazy=False)
        Point = shapely_geometry.Point
        df['geom'] = [Point(i, i + 0.5) for i in ids]
    return df


def auto_docs(n, tag):
    return [{'label': f'{tag}-{i}', 'amount': i * 2.5} for i in range(n)]


def nullidx_df(start, n):
    rows = []
    for i in range(start, start + n):
        rows.append({
            'dt': BASE + datetime.timedelta(hours=i),
            'id': None if i % 3 == 0 else f'id{i % 3}',
            'v': i,
        })
    return pd.DataFrame(rows)


def static_df(n):
    return pd.DataFrame([
        {'dt': BASE + datetime.timedelta(hours=i), 'v': i * 1.0, 'name': f'n{i}'} for i in range(n)
    ])
