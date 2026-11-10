"""The snapshot's report: what the PNG shows, where its candles came from, what it could not show."""

from __future__ import annotations

import pandas as pd

from ._bars_source import ChartBars
from ._renderer import Rendered, SnapshotJob
from ._util import fmt_time

_LIST_MAX = 8

_FRAMED = {
    "layer": "framed on the layer, as the user's chart frames it when the layer lands",
    "user": "the user's window",
    "range": "as you asked",
    "default": "the chart's opening view, latest bar centered",
}


def report(job: SnapshotJob, rendered: Rendered) -> str:
    chart = job.bars.times()
    w, h = rendered.size
    lines = [f"Snapshot of {job.symbol} {job.timeframe} ({job.theme}, {w}x{h} px): {job.path}", job.look_line]
    lines.append(_view_line(job, rendered, chart))
    lines.extend(_bars_lines(job, job.bars, chart))
    lines.extend(job.notes)
    if job.layer is not None:
        lines.append(f"Layer \"{job.layer['title']}\": {job.counts or 'empty'}.")
    rep = rendered.report
    if rep:
        many = len(rep.get("layers") or []) > 1
        for key, title, advice, read in _FINDINGS:
            found = [read(item, many) for item in rep.get(key) or []]
            if found:
                lines.append(f"{title} ({len(found)}){advice}: " + _join(found))
        for note in rep.get("notes") or []:
            lines.append(f"Note: {note}")
    if rendered.page_errors:
        lines.append("Page errors: " + _join(rendered.page_errors))
    if rendered.unanswered:
        lines.append("Unanswered requests (drawn without them): " + _join(sorted(set(rendered.unanswered))))
    lines.append("Read the PNG to look at it.")
    return "\n".join(lines)


def _view_line(job: SnapshotJob, rendered: Rendered, chart: pd.Series) -> str:
    """The window shown, how it was chosen, and how many of the chart's bars are in it."""
    frame = job.frame
    why = f"view {frame.preset}" if frame.kind == "preset" else _FRAMED[frame.kind]
    fallback = ""
    if rendered.framed == "pending":
        what = {"layer": "frame the layer", "user": f"show the user's window {frame.start} to {frame.end}",
                "range": f"show {frame.start} to {frame.end}", "preset": f"apply view {frame.preset}"}[frame.kind]
        fallback, why = f" Could not {what}: its times are not on the chart.", _FRAMED["default"]
    view = rendered.view
    if not (view and view.get("from") and view.get("to")):
        return f"Shows the chart's {len(chart):,} bars ({why}).{fallback}"
    lo, hi = str(view["from"]).replace("T", " "), str(view["to"]).replace("T", " ")
    seg = f"Shows {lo} to {hi} ({why})"
    try:
        shown = int(((chart.index >= pd.Timestamp(lo)) & (chart.index <= pd.Timestamp(hi))).sum())
        seg += f": {shown:,} of the chart's {len(chart):,} bars"
    except (ValueError, TypeError):
        pass
    rep = rendered.report or {}
    rv = rep.get("view") if isinstance(rep.get("view"), dict) else {}
    future = int(rv.get("futureSlots") or 0)
    seg += f", plus room for {future} future bars." if future else "."
    panes = rv.get("panes")
    if isinstance(panes, list) and panes:
        seg += " Panes: " + ", ".join(str(p) for p in panes) + "."
    user = job.user
    if frame.kind == "layer" and user and user.window:
        ulo, uhi = str(user.start).replace("T", " "), str(user.end).replace("T", " ")
        if (ulo, uhi) != (lo, hi):
            seg += (f" The user's chart is on {ulo} to {uhi} now; publishing frames it as shown here. "
                    f'c.snapshot(view="user") shows their current window.')
    return seg + fallback


def _us_intraday(job: SnapshotJob) -> bool:
    return job.timeframe != "1day" and job.tz == "America/New_York"


def _bars_lines(job: SnapshotJob, bars: ChartBars, chart: pd.Series) -> list[str]:
    """Where the candles came from, and how the agent's bars differ from the chart's."""
    daily = job.timeframe == "1day"
    span = (f"{len(chart):,} bars, {fmt_time(chart.index[0], daily, 'text')} to "
            f"{fmt_time(chart.index[-1], daily, 'text')}" if len(chart) else "no bars")
    if bars.mode == "tool":
        lines = [f"Candles: the chart's own bars from the market-data tool ({bars.source or 'unknown source'}), {span}."]
        if bars.misses:
            lines.append("Older bars the chart asked for did not load: " + _join(bars.misses) + ".")
        if job.df is not None and job.idx is not None:
            lines.extend(compare(job.timeframe, job.df, job.idx, chart))
        return lines
    hours = ""
    if _us_intraday(job) and len(chart):
        mins = chart.index.hour * 60 + chart.index.minute
        if mins.min() >= 9 * 60 + 30 and mins.max() < 16 * 60:
            hours = "regular hours only, "
    theirs = ("The user's chart shows its own bars, including pre/after hours." if _us_intraday(job)
              else "The user's chart shows its own bars, which can differ.")
    return [f"Candles from your DataFrame ({hours}{len(chart):,} bars): the market-data tool failed: "
            f"{bars.failure}. {theirs}"]


def compare(timeframe: str, df, idx: pd.DatetimeIndex, chart: pd.Series) -> list[str]:
    """Material differences between the bars the agent computed on and the chart's."""
    if not len(chart) or not len(idx):
        return []
    daily = timeframe == "1day"
    mine = pd.DatetimeIndex(idx)
    lines = []
    if not daily:
        tod = mine.hour * 60 + mine.minute
        lo, hi = int(tod.min()), int(tod.max())
        same_days = chart[chart.index.normalize().isin(mine.normalize().unique())]
        ctod = same_days.index.hour * 60 + same_days.index.minute
        pre, post = same_days[ctod < lo], same_days[ctod > hi]
        outside = len(pre) + len(post)
        if outside >= max(3, 0.02 * len(same_days)):
            where = []
            if len(pre):
                where.append(f"{_hhmm(ctod[ctod < lo])} before")
            if len(post):
                where.append(f"{_hhmm(ctod[ctod > hi])} after")
            lines.append(f"Your DataFrame has no bars outside {_hhmm([lo, hi])}; the chart has {outside:,} "
                         f"there on the same days ({', '.join(where)}).")
        inside = same_days[(ctod >= lo) & (ctod <= hi)]
    else:
        inside = chart[(chart.index >= mine[0]) & (chart.index <= mine[-1])]
    lacking = inside.index.difference(mine)
    if len(lacking) >= max(3, 0.02 * max(len(inside), 1)):
        lines.append(f"Your DataFrame lacks {len(lacking):,} of the chart's bars in its own span "
                     f"(first {fmt_time(lacking[0], daily, 'text')}).")
    if mine[-1] < chart.index[-1]:
        later = int((chart.index > mine[-1]).sum())
        lines.append(f"Your DataFrame ends {fmt_time(mine[-1], daily, 'text')}; the chart's last bar is "
                     f"{fmt_time(chart.index[-1], daily, 'text')}, {later:,} bars later.")
    cols = {str(c).lower(): c for c in df.columns}
    if "close" in cols:
        mine_close = pd.Series(pd.to_numeric(df[cols["close"]], errors="coerce").to_numpy(), index=mine)
        mine_close = mine_close[~mine_close.index.duplicated(keep="last")]
        shared = mine_close.index.intersection(chart.index)
        if len(shared):
            rel = ((mine_close.loc[shared] - chart.loc[shared]).abs() / chart.loc[shared].abs()).dropna()
            if len(rel) and rel.max() > 0.01:
                at = rel.idxmax()
                lines.append(f"Your closes differ from the chart's by up to {rel.max():.1%} "
                             f"({fmt_time(at, daily, 'text')}) on the bars both have.")
    if lines:
        lines.append("Compute on chartkit.bars(...) to work from the chart's own bars.")
    return lines


def _hhmm(minutes) -> str:
    """The span of these minutes of the day, as ``HH:MM-HH:MM``."""
    lo, hi = int(min(minutes)), int(max(minutes))
    return f"{lo // 60:02d}:{lo % 60:02d}-{hi // 60:02d}:{hi % 60:02d}"


def _who(d: dict, many: bool) -> str:
    el = str(d.get("element", "?"))
    return f"{d.get('layer')}/{el}" if many and d.get("layer") else el


def _cut(c: dict) -> str:
    where = "layer title" if c.get("where") == "title" else "price-axis tag"
    return f"{where} \"{c.get('text', '')}\" shows \"{c.get('shown', '')}\""


def _cover(c: dict) -> str:
    if c.get("by") == "chip":
        return "the layer title chip"
    return f"the price tag \"{c.get('byText', '')}\""


def _forecast(f: dict, many: bool) -> str:
    who = f"\"{f['label']}\"" if f.get("label") else _who(f, many)
    at = str(f.get("at") or "?").replace("T", " ")
    state = f.get("state")
    if state == "hit":
        return f"{who} hit on {at}"
    if state == "missed":
        return f"{who} missed (target {at} passed)"
    if state == "unknown":
        return f"{who} not judged: the chart's bars start after its origin"
    return f"{who} pending until {at}"


def _join(items: list[str]) -> str:
    more = len(items) - _LIST_MAX
    return "; ".join(items[:_LIST_MAX]) + (f"; +{more} more" if more > 0 else "")


#: The page's findings, in the order the report lists them: (report key, title, advice, how one reads).
_FINDINGS = (
    ("droppedLabels", "Labels hidden by overlap", "",
     lambda d, many: f"{_who(d, many)} \"{d.get('text', '')}\""),
    ("offscreen", "Out of view", "", lambda o, many: f"{_who(o, many)} ({o.get('kind', '?')})"),
    ("cutText", "Text cut short", ", shorten it", lambda c, many: _cut(c)),
    ("coveredLabels", "Labels under the chart's own text", ", move them",
     lambda c, many: f"{_who(c, many)} ({c.get('kind', '?')}) under {_cover(c)}"),
    ("markersOverCandles", "Marker text over candles", ", shorten it or use c.label",
     lambda m, many: f"\"{m.get('text', '')}\""),
    ("forecasts", "Forecasts", "", _forecast),
)
