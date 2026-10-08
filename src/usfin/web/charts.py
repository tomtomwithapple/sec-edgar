"""Hand-built inline SVG line chart for the series pages (no JS, no chart library).

`line_chart(points, title=, y_label=)` takes `(date, value | None)` pairs in date order and
returns one `<svg>` string: `role="img"` with `<title>`/`<desc>` (the desc states the range, the
extremes and how many values are missing), recessive grid and axis labels, one 2px line in the
page's `--accent` token (colors come from `style.css`, so light and dark both work). A None value
breaks the line — a gap, never a line drawn through it; a value with no neighbour on either side
is drawn as a dot so it stays visible. The chart scales with its `viewBox`; at phone width the
stylesheet enlarges the axis text. Up to `HOVER_MAX` points get invisible hit circles with a
native `<title>` tooltip (date and value), so hover works without JavaScript.
"""

import math
from collections.abc import Sequence
from datetime import date
from decimal import Decimal
from html import escape

WIDTH, HEIGHT = 640, 280
LEFT, RIGHT, TOP, BOTTOM = 64, 16, 16, 44
HOVER_MAX = 400
_ids = iter(range(1, 1 << 62))


def _nice_step(span: float, target: int = 5) -> float:
    raw = span / max(target, 1)
    mag = 10 ** math.floor(math.log10(raw))
    for m in (1, 2, 2.5, 5, 10):
        if raw <= m * mag:
            return m * mag
    return 10 * mag


def compact(value: float, step: float | None = None) -> str:
    """Short axis label: 37.1T, 812K, 4.25 (decimals follow the tick step)."""
    if value == 0:
        return "0"
    for threshold, divisor, suffix in (
        (1e12, 1e12, "T"),
        (1e9, 1e9, "B"),
        (1e6, 1e6, "M"),
        (1e4, 1e3, "K"),
    ):
        if abs(value) >= threshold or (step or 0) >= threshold:
            scaled_step = (step or divisor) / divisor
            places = 0 if scaled_step >= 1 else min(3, -math.floor(math.log10(scaled_step)) + 1)
            text = f"{value / divisor:,.{places}f}"
            if places:
                text = text.rstrip("0").rstrip(".")
            return text + suffix
    places = 0
    if step and step < 1:
        places = min(6, -math.floor(math.log10(step)) + 1)
    text = f"{value:,.{places}f}"
    return text.rstrip("0").rstrip(".") if places else text


def _fmt(value: float) -> str:
    """A value in full for tooltips and the description: shortest round-trip, no exponent."""
    value = float(value)
    if value.is_integer():
        return f"{value:,.0f}"
    return f"{Decimal(repr(value)):,f}"


def _x_ticks(first: date, last: date) -> list[date]:
    span_days = (last - first).days
    if span_days > 3 * 365:
        years = last.year - first.year
        step = max(1, math.ceil(years / 7))
        start = first.year + (1 if (first.month, first.day) != (1, 1) else 0)
        return [date(y, 1, 1) for y in range(start, last.year + 1, step)]
    months = (last.year - first.year) * 12 + last.month - first.month
    step = max(1, math.ceil(months / 6))
    out = []
    y, m = first.year, first.month
    if first.day != 1:
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    while (y, m) <= (last.year, last.month):
        out.append(date(y, m, 1))
        m += step
        while m > 12:
            y, m = y + 1, m - 12
    return out


def _x_label(d: date, first: date, last: date) -> str:
    return str(d.year) if (last - first).days > 3 * 365 else f"{d:%Y-%m}"


def line_chart(
    points: Sequence[tuple[date, float | None]],
    *,
    title: str,
    y_label: str,
    x_label: str = "Date",
) -> str:
    """One `<svg>` line chart, or "" when there is no value to draw."""
    vals = [(d, float(v)) for d, v in points if v is not None]
    if not vals:
        return ""
    first, last = points[0][0], points[-1][0]
    lo, hi = min(v for _, v in vals), max(v for _, v in vals)
    if lo == hi:
        pad = abs(lo) * 0.05 or 1.0
        lo, hi = lo - pad, hi + pad
    step = _nice_step(hi - lo)
    y0, y1 = math.floor(lo / step) * step, math.ceil(hi / step) * step
    x_span = max((last - first).days, 1)
    pw, ph = WIDTH - LEFT - RIGHT, HEIGHT - TOP - BOTTOM

    def sx(d: date) -> float:
        return LEFT + pw * ((d - first).days / x_span if last != first else 0.5)

    def sy(v: float) -> float:
        return TOP + ph * (1 - (v - y0) / (y1 - y0))

    uid = next(_ids)
    parts = [
        (
            f'<svg class="chart" role="img" viewBox="0 0 {WIDTH} {HEIGHT}"'
            f' aria-labelledby="chart{uid}-t chart{uid}-d" xmlns="http://www.w3.org/2000/svg">'
        ),
        f'<title id="chart{uid}-t">{escape(title)}</title>',
    ]
    missing = sum(1 for _, v in points if v is None)
    lo_v, hi_v = min(v for _, v in vals), max(v for _, v in vals)
    desc = (
        f"Line chart of {escape(y_label)} from {first.isoformat()} to {last.isoformat()}:"
        f" {len(vals)} values, low {_fmt(lo_v)}, high {_fmt(hi_v)}"
        + (f"; {missing} missing, shown as gaps" if missing else "")
        + "."
    )
    parts.append(f'<desc id="chart{uid}-d">{desc}</desc>')

    # grid + y axis labels
    n = round((y1 - y0) / step)
    for i in range(n + 1):
        v = y0 + i * step
        y = sy(v)
        parts.append(
            f'<line class="grid" x1="{LEFT}" x2="{WIDTH - RIGHT}" y1="{y:.1f}" y2="{y:.1f}"/>'
        )
        parts.append(
            f'<text class="tick" x="{LEFT - 6}" y="{y:.1f}" text-anchor="end"'
            f' dominant-baseline="middle">{escape(compact(v, step))}</text>'
        )
    # x axis ticks
    base = TOP + ph
    parts.append(f'<line class="axis" x1="{LEFT}" x2="{WIDTH - RIGHT}" y1="{base}" y2="{base}"/>')
    for d in _x_ticks(first, last):
        x = sx(d)
        parts.append(f'<line class="axis" x1="{x:.1f}" x2="{x:.1f}" y1="{base}" y2="{base + 4}"/>')
        parts.append(
            f'<text class="tick" x="{x:.1f}" y="{base + 18}" text-anchor="middle">'
            f"{_x_label(d, first, last)}</text>"
        )
    # axis titles
    parts.append(
        f'<text class="label" x="{LEFT + pw / 2:.0f}" y="{HEIGHT - 4}" text-anchor="middle">'
        f"{escape(x_label)}</text>"
    )
    parts.append(
        f'<text class="label y" transform="translate(12 {TOP + ph / 2:.0f}) rotate(-90)"'
        f' text-anchor="middle">{escape(y_label)}</text>'
    )

    # the line: one sub-path per run of values; a lone value is a dot
    path: list[str] = []
    dots: list[str] = []
    run: list[tuple[float, float]] = []

    def flush() -> None:
        if len(run) == 1:
            x, y = run[0]
            dots.append(f'<circle class="dot" cx="{x:.1f}" cy="{y:.1f}" r="3"/>')
        elif run:
            path.append("M" + "L".join(f"{x:.1f},{y:.1f}" for x, y in run))
        run.clear()

    for d, v in points:
        if v is None:
            flush()
        else:
            run.append((sx(d), sy(float(v))))
    flush()
    if path:
        parts.append(f'<path class="line" d="{"".join(path)}"/>')
    parts.extend(dots)
    if len(vals) <= HOVER_MAX:
        for d, v in vals:
            parts.append(
                f'<circle class="hit" cx="{sx(d):.1f}" cy="{sy(v):.1f}" r="6">'
                f"<title>{d.isoformat()}: {_fmt(v)}</title></circle>"
            )
    parts.append("</svg>")
    return "".join(parts)
