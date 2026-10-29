#!/usr/bin/env python3
"""
Render the Meerschaum v3 / v4 / v5 comparison figure (PNG + SVG) from a `run.py` results JSON.

    python scripts/bench/plot_v345.py scripts/bench/results/2026-10-20-v3-v4-v5.json \
        --out docs/zensical/assets/bench/meerschaum-v3-v4-v5

Reads only the JSON (medians, versions, hardware summary, date); every number on the image is
computed from it, nothing is typed in. Requires matplotlib (not part of the benchmark runner).

Design: one image, 1600x900 px, five panels: headline speedups, sync throughput by size on
SQLite and TimescaleDB (rows/s, log axis), incremental / re-sync / get_data times (seconds,
linear, value-labelled) and startup latency. Colors are slots 1-3 of the dataviz reference
palette (blue, orange, aqua; validated all-pairs for three series); every mark carries a direct
value label, so identity and value never rest on color alone (the aqua slot is below 3:1
contrast on the light surface, which the labels and the markdown table relieve).
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import matplotlib

matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib import font_manager  # noqa: E402

W, H, DPI = 1600, 900, 100
SURFACE = '#fcfcfb'
INK = '#0b0b0b'
INK2 = '#52514e'
MUTED = '#8a8984'
GRID = '#e8e7e3'
BLUE, ORANGE, AQUA = '#2a78d6', '#eb6834', '#1baf7a'
BLUE_LIGHT = '#86b6ef'  # sequential blue, step 250: the standalone v5 binary
COLORS = {'v3': AQUA, 'v4': ORANGE, 'v5': BLUE}
SIZES = {'10k': 10_000, '100k': 100_000, '1M': 1_000_000}
BACKEND_TITLES = {'sqlite': 'SQLite', 'timescaledb': 'TimescaleDB', 'postgis': 'PostGIS'}


class Data:
    """Median lookups over a results document."""

    def __init__(self, doc: Dict[str, Any]):
        self.meta = doc['meta']
        self.rows = {(r['backend'], r['case'], r['impl']): r for r in doc['results']}

    def med(self, backend: str, case: str, impl: str) -> Optional[float]:
        row = self.rows.get((backend, case, impl))
        if not row or row.get('median') is None:
            return None
        return float(row['median'])

    def version(self, major: str) -> str:
        return {'v3': self.meta.get('v3_version'), 'v4': self.meta.get('v4_version'),
                'v5': self.meta.get('v5_version')}[major] or '?'


def variance_note(data: Data) -> str:
    """Name any 1M sync cell whose three runs spread more than 25% (honesty about noise)."""
    notes = []
    for (backend, case, impl), row in data.rows.items():
        samples = row.get('seconds') or []
        if case == 'sync_1M' and impl.endswith('py') and len(samples) > 1 \
                and max(samples) > 1.25 * min(samples):
            notes.append(f'{BACKEND_TITLES.get(backend, backend)} 1M {impl[:2]}: '
                         f'{min(samples):.0f} to {max(samples):.0f} s')
    return ('  Run-to-run spread: ' + '; '.join(sorted(notes)) + '.') if notes else ''


def pick_font() -> str:
    available = {f.name for f in font_manager.fontManager.ttflist}
    for name in ('Inter', 'Liberation Sans', 'DejaVu Sans'):
        if name in available:
            return name
    return 'sans-serif'


def fmt_seconds(value: float) -> str:
    if value >= 100:
        return f'{value:.0f} s'
    if value >= 10:
        return f'{value:.1f} s'
    if value >= 1:
        return f'{value:.2f} s'.rstrip('0').rstrip('.') if value >= 1 else f'{value:.2f} s'
    return f'{value * 1000:.0f} ms'


def fmt_rate(value: float) -> str:
    if value >= 1e6:
        return f'{value / 1e6:.1f}M'
    if value >= 1e4:
        return f'{value / 1e3:.0f}k'
    if value >= 1e3:
        return f'{value / 1e3:.1f}k'
    return f'{value:.0f}'


def geomean(values: List[float]) -> Optional[float]:
    values = [v for v in values if v and v > 0]
    return math.exp(sum(math.log(v) for v in values) / len(values)) if values else None


def speedups(data: Data, against: str, backends: List[str]) -> List[float]:
    """`against` time / v5 time for every fresh-sync cell with both medians (Python API)."""
    out = []
    for backend in backends:
        for size in SIZES:
            old = data.med(backend, f'sync_{size}', f'{against}py')
            new = data.med(backend, f'sync_{size}', 'v5py')
            if old and new:
                out.append(old / new)
    return out


def px(fig, x, y, w, h):
    """Axes rectangle from pixel coordinates (origin top-left)."""
    return fig.add_axes([x / W, 1 - (y + h) / H, w / W, h / H])


def style_axes(ax):
    ax.set_facecolor(SURFACE)
    for side in ('top', 'right', 'left'):
        ax.spines[side].set_visible(False)
    ax.spines['bottom'].set_color(MUTED)
    ax.spines['bottom'].set_linewidth(1)
    ax.tick_params(axis='both', colors=INK2, labelsize=10, length=0)
    ax.grid(axis='y', color=GRID, linewidth=1)
    ax.set_axisbelow(True)


def panel_title(fig, x, y, title, subtitle):
    fig.text(x / W, 1 - y / H, title, fontsize=13.5, fontweight='bold', color=INK, va='top')
    fig.text(x / W, 1 - (y + 20) / H, subtitle, fontsize=10, color=INK2, va='top')


def tile(fig, x, y, w, h, big, line1, line2):
    ax = px(fig, x, y, w, h)
    ax.set_axis_off()
    ax.add_patch(matplotlib.patches.FancyBboxPatch(
        (0, 0), 1, 1, boxstyle='round,pad=0,rounding_size=0.06', transform=ax.transAxes,
        facecolor='#f1f0ec', edgecolor='none'))
    ax.text(0.07, 0.62, big, fontsize=40, fontweight='bold', color=BLUE, va='center',
            transform=ax.transAxes)
    ax.text(0.07, 0.27, line1, fontsize=11.5, fontweight='bold', color=INK, va='center',
            transform=ax.transAxes)
    ax.text(0.07, 0.10, line2, fontsize=9.5, color=INK2, va='center', transform=ax.transAxes)


def sync_panel(fig, data: Data, backend: str, x, y, w, h):
    ax = px(fig, x, y + 46, w, h - 46)
    style_axes(ax)
    panel_title(fig, x, y, f'Fresh sync throughput, {BACKEND_TITLES[backend]}',
                'rows per second (log scale), higher is better')
    ax.set_yscale('log')
    xs = list(range(len(SIZES)))
    top = 1.0
    bottom = float('inf')
    series = {}
    for major in ('v3', 'v4', 'v5'):
        pts = []
        for i, (label, rows) in enumerate(SIZES.items()):
            t = data.med(backend, f'sync_{label}', f'{major}py')
            if t:
                pts.append((i, rows / t))
        series[major] = pts
        for _, v in pts:
            top, bottom = max(top, v), min(bottom, v)
    if bottom == float('inf'):
        ax.text(0.5, 0.5, 'n/a', transform=ax.transAxes, ha='center', color=MUTED)
        return
    ax.set_ylim(10 ** math.floor(math.log10(bottom) - 0.15), 10 ** math.ceil(math.log10(top) + 0.45))
    for major, pts in series.items():
        if not pts:
            continue
        ax.plot([p[0] for p in pts], [p[1] for p in pts], color=COLORS[major], linewidth=2.4,
                marker='o', markersize=8, markeredgecolor=SURFACE, markeredgewidth=2,
                solid_capstyle='round', zorder=3)
    # Direct labels to the right of the last point, nudged apart (identity + value in ink).
    ends = sorted(((pts[-1][1], major) for major, pts in series.items()
                   if pts and pts[-1][0] == len(SIZES) - 1))
    last_px = None
    for value, major in ends:
        y_px = ax.transData.transform((0, value))[1]
        if last_px is not None and y_px - last_px < 15:
            y_px = last_px + 15
        last_px = y_px
        y_data = ax.transData.inverted().transform((0, y_px))[1]
        ax.annotate(f'{major} {fmt_rate(value)}', (len(SIZES) - 1, value),
                    xytext=(len(SIZES) - 1 + 0.12, y_data), textcoords='data', ha='left',
                    va='center', fontsize=10, fontweight='bold', color=INK, zorder=4,
                    annotation_clip=False)
    for major, pts in series.items():
        if major == 'v5' and pts and pts[0][0] == 0:
            ax.annotate(fmt_rate(pts[0][1]), pts[0], textcoords='offset points', xytext=(0, 9),
                        ha='center', fontsize=10, fontweight='bold', color=INK, zorder=4)
    ax.set_xticks(xs, [f'{k} rows' for k in SIZES])
    ax.set_xlim(-0.35, len(SIZES) - 0.1)
    ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _p: fmt_rate(v)))
    ax.yaxis.set_minor_locator(matplotlib.ticker.NullLocator())
    # Legend: color is never the only identity channel.
    handles = [matplotlib.lines.Line2D([0], [0], color=COLORS[m], linewidth=2.4, marker='o',
                                       markersize=7, markeredgecolor=SURFACE,
                                       label=f'v{data.version(m)}') for m in ('v3', 'v4', 'v5')]
    ax.legend(handles=handles, loc='upper left', ncol=3, frameon=False, fontsize=10,
              labelcolor=INK2, handlelength=1.4, columnspacing=1.2, borderaxespad=0.1)


def grouped_bars(fig, data: Data, x, y, w, h, title, subtitle, case, backends):
    ax = px(fig, x, y + 46, w, h - 46)
    style_axes(ax)
    panel_title(fig, x, y, title, subtitle)
    values = {}
    for b_i, backend in enumerate(backends):
        for m_i, major in enumerate(('v3', 'v4', 'v5')):
            v = data.med(backend, case, f'{major}py')
            if v is not None:
                values[(b_i, m_i)] = v
    if not values:
        ax.text(0.5, 0.5, 'n/a', transform=ax.transAxes, ha='center', color=MUTED)
        return
    width = 0.26
    peak = max(values.values())
    for (b_i, m_i), v in values.items():
        left = b_i + (m_i - 1) * (width + 0.03)
        ax.bar(left, v, width=width, color=COLORS[('v3', 'v4', 'v5')[m_i]], zorder=3,
               linewidth=0)
        ax.annotate(fmt_seconds(v), (left, v), textcoords='offset points', xytext=(0, 4),
                    ha='center', fontsize=9, fontweight='bold', color=INK, zorder=4)
    ax.set_ylim(0, peak * 1.16)
    ax.set_yticks([])
    ax.grid(False)
    ax.set_xticks(range(len(backends)), [BACKEND_TITLES[b] for b in backends])
    ax.set_xlim(-0.6, len(backends) - 0.4)


def startup_panel(fig, data: Data, x, y, w, h):
    ax = px(fig, x, y + 46, w, h - 46)
    style_axes(ax)
    panel_title(fig, x, y, 'Startup latency', 'milliseconds, lower is better')
    groups = [
        ('import\nmeerschaum', 'startup', 'startup_import',
         [('v3', 'v3py'), ('v4', 'v4py'), ('v5', 'v5py')]),
        ('mrsm\n--version', 'startup', 'startup_version',
         [('v3', 'v3cli'), ('v4', 'v4cli'), ('v5', 'v5cli'), ('v5b', 'v5bin')]),
    ]
    width = 0.19
    peak = 0.0
    ticks, labels = [], []
    for g_i, (label, backend, case, members) in enumerate(groups):
        n = len(members)
        center = g_i * 1.5
        ticks.append(center)
        labels.append(label)
        for m_i, (key, impl) in enumerate(members):
            v = data.med(backend, case, impl)
            if v is None:
                continue
            left = center + (m_i - (n - 1) / 2) * (width + 0.03)
            color = BLUE_LIGHT if key == 'v5b' else COLORS[key]
            ax.bar(left, v * 1000, width=width, color=color, zorder=3, linewidth=0)
            peak = max(peak, v * 1000)
            ax.annotate(f'{v * 1000:.0f}', (left, v * 1000), textcoords='offset points',
                        xytext=(0, 4), ha='center', fontsize=9, fontweight='bold', color=INK,
                        zorder=4)
    ax.set_ylim(0, max(peak, 1) * 1.42)
    ax.set_yticks([])
    ax.grid(False)
    ax.set_xticks(ticks, labels)
    ax.set_xlim(-0.55, 1.5 + 0.55)
    handles = [matplotlib.patches.Patch(color=c, label=l) for c, l in (
        (AQUA, 'v3'), (ORANGE, 'v4'), (BLUE, 'v5 wheel'), (BLUE_LIGHT, 'v5 binary'))]
    ax.legend(handles=handles, loc='upper center', ncol=2, frameon=False, fontsize=9,
              labelcolor=INK2, handlelength=1, columnspacing=1.0, borderaxespad=0.0,
              bbox_to_anchor=(0.5, 1.04))


def render(doc: Dict[str, Any], out: Path, backends: Optional[List[str]] = None) -> None:
    data = Data(doc)
    meta = data.meta
    plt.rcParams['font.family'] = pick_font()
    plt.rcParams['svg.fonttype'] = 'path'
    fig = plt.figure(figsize=(W / DPI, H / DPI), dpi=DPI, facecolor=SURFACE)
    wanted = backends or ('sqlite', 'timescaledb')
    backends = [b for b in wanted if b in meta['backends']]
    sync_backends = backends[:2]

    v3, v4, v5 = (data.version(m) for m in ('v3', 'v4', 'v5'))
    fig.text(48 / W, 1 - 34 / H, 'Meerschaum v5 is a Rust rewrite, and it shows',
             fontsize=27, fontweight='bold', color=INK, va='top')
    fig.text(48 / W, 1 - 76 / H,
             f'Same pipes, same data, same host: v{v3} vs v{v4} vs v{v5}. Python API timings, '
             'median of 3 runs.', fontsize=13, color=INK2, va='top')

    # Headline tiles
    gm4 = geomean(speedups(data, 'v4', sync_backends))
    gm3 = geomean(speedups(data, 'v3', sync_backends))
    cli4, cli5 = data.med('startup', 'startup_version', 'v4cli'), data.med(
        'startup', 'startup_version', 'v5bin')
    def cell_text(values):
        if not values:
            return ''
        return (f'geometric mean of {len(values)} cells '
                f'({min(values):.1f}× to {max(values):.0f}×)')

    tx, ty, tw, th = 48, 118, 300, 124
    if gm4:
        tile(fig, tx, ty, tw, th, f'{gm4:.0f}×', 'faster fresh sync than v4',
             cell_text(speedups(data, 'v4', sync_backends)))
    if gm3:
        tile(fig, tx, ty + th + 12, tw, th, f'{gm3:.0f}×', 'faster fresh sync than v3',
             cell_text(speedups(data, 'v3', sync_backends)))
    if cli4 and cli5:
        tile(fig, tx, ty + 2 * (th + 12), tw, th, f'{cli4 / cli5:.0f}×',
             'faster CLI start than v4', 'mrsm --version, standalone binary')

    # Sync throughput, two backends
    top_y = 118
    top_h = 3 * th + 24
    sx = 392
    gap = 56
    pw = (W - 48 - sx - gap) / 2
    for i, backend in enumerate(sync_backends):
        sync_panel(fig, data, backend, sx + i * (pw + gap), top_y, pw, top_h)

    # Bottom row
    by, bh = 118 + top_h + 26, H - (118 + top_h + 26) - 100
    cw = (W - 96 - 3 * 30) / 4
    # The tiles column and the sync panels span the top; bottom row is four equal panels.
    grouped_bars(fig, data, 48, by, cw, bh, 'Incremental sync',
                 '100k table + 1% new rows, seconds', 'inc_100k', backends)
    grouped_bars(fig, data, 48 + (cw + 30), by, cw, bh, 'Re-sync, nothing new',
                 '100k rows, all duplicates, seconds', 'resync_100k', backends)
    grouped_bars(fig, data, 48 + 2 * (cw + 30), by, cw, bh, 'Read 100k rows',
                 'pipe.get_data(), seconds', 'get_data_100k', backends)
    startup_panel(fig, data, 48 + 3 * (cw + 30), by, cw, bh)

    hw = meta.get('hardware') or {}
    hw_text = (f'{hw["cpu_model"].replace("(R)", "").replace("(TM)", "").replace("CPU ", "")}, '
               f'{hw.get("logical_cpus")} threads, {hw.get("ram_gib")} GiB RAM'
               if hw.get('cpu_model') else f'{meta.get("cpus")} CPUs')
    servers = ', '.join(BACKEND_TITLES[b] for b in backends)
    lines = [
        (f'meerschaum {v3} / {v4} / {v5} (release candidate), Python {meta["python"]}, '
         f'{meta["os"]}. Measured {meta["date"]}.', INK2),
        (f'Hardware: {hw_text}. Backends: {servers} (local containers). Labels show medians '
         '(lower is better except throughput).', INK2),
        ('Each version ran in its own environment with the same pinned pandas, numpy, pyarrow '
         'and SQLAlchemy. Harness: scripts/bench in the Meerschaum repository.'
         + variance_note(data), MUTED),
    ]
    for k, (text, color) in enumerate(lines):
        fig.text(48 / W, 1 - (H - 56 + 17 * k) / H, text, fontsize=9.5, color=color, va='top')

    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out.with_suffix('.png'), dpi=DPI, facecolor=SURFACE,
                metadata={'Software': None})
    fig.savefig(out.with_suffix('.svg'), facecolor=SURFACE, metadata={'Date': None})
    plt.close(fig)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument('results', type=Path, help='results JSON written by scripts/bench/run.py')
    p.add_argument('--backends', default='sqlite,timescaledb',
                   help='backends shown (default sqlite,timescaledb; the first two get the '
                        'throughput panels)')
    p.add_argument('--out', type=Path, required=True,
                   help='output path without extension (.png and .svg are written)')
    args = p.parse_args()
    render(json.loads(args.results.read_text()), args.out,
           [b for b in args.backends.split(',') if b])
    print(f'wrote {args.out.with_suffix(".png")} and {args.out.with_suffix(".svg")}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
