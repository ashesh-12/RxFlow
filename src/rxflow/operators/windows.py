"""Event-time windows and watermarks.

Windows are keyed and driven by event time. A watermark is the engine's belief
that event time has moved past T; events behind it are late.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Hashable, Iterable, Iterator

from rxflow.clock import parse_duration
from rxflow.compose import operator
from rxflow.context import emit_side, get_runtime
from rxflow.envelope import Envelope, as_envelope

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True, slots=True)
class Window:
    key: Hashable
    start: datetime
    end: datetime
    items: tuple[Envelope, ...]

    def __len__(self) -> int:
        return len(self.items)


def assign_watermarks(bounded_out_of_orderness: str | timedelta | float = "0s"):
    """Set runtime.watermark = max(event_time) - bound. Windows read this to close."""
    bound = parse_duration(bounded_out_of_orderness)

    @operator
    def assign_watermarks(stream):
        max_ts = None
        rt = get_runtime()
        for item in stream:
            env = as_envelope(item)
            et = env.event_time
            if et is not None and (max_ts is None or et > max_ts):
                max_ts = et
                if rt is not None:
                    rt.watermark = max_ts - bound
            yield env

    return assign_watermarks()


def tumbling_window(size, *, allowed_lateness="0s"):
    size_td = parse_duration(size)
    lateness = parse_duration(allowed_lateness)

    @operator
    def tumbling_window(stream):
        yield from _run_windows(stream, size_td, size_td, lateness, kind="tumbling")

    return tumbling_window()


def sliding_window(size, slide, *, allowed_lateness="0s"):
    size_td = parse_duration(size)
    slide_td = parse_duration(slide)
    lateness = parse_duration(allowed_lateness)

    @operator
    def sliding_window(stream):
        yield from _run_windows(stream, size_td, slide_td, lateness, kind="sliding")

    return sliding_window()


def session_window(gap, *, allowed_lateness="0s"):
    gap_td = parse_duration(gap)
    lateness = parse_duration(allowed_lateness)

    @operator
    def session_window(stream):
        yield from _run_sessions(stream, gap_td, lateness)

    return session_window()


def _event_time(env: Envelope) -> datetime | None:
    return env.event_time


def _align(ts: datetime, size: timedelta) -> datetime:
    size_ms = max(int(size.total_seconds() * 1000), 1)
    epoch_ms = int((ts - _EPOCH).total_seconds() * 1000)
    aligned = (epoch_ms // size_ms) * size_ms
    return _EPOCH + timedelta(milliseconds=aligned)


def _window_starts(ts: datetime, size: timedelta, slide: timedelta, kind: str) -> list[datetime]:
    if kind == "tumbling":
        return [_align(ts, size)]
    latest = _align(ts, slide)
    n = max(int(size / slide), 1)
    starts = []
    for i in range(n):
        start = latest - i * slide
        if start <= ts < start + size:
            starts.append(start)
    return starts


def _to_env(key: Hashable, start: datetime, end: datetime, items: list[Envelope]) -> Envelope:
    last = items[-1]
    return Envelope(
        payload=Window(key=key, start=start, end=end, items=tuple(items)),
        key=key,
        event_time=end,
        offset=last.offset,
        partition=last.partition,
        headers=last.headers,
    )


def _run_windows(
    stream: Iterable,
    size: timedelta,
    slide: timedelta,
    lateness: timedelta,
    kind: str,
) -> Iterator[Envelope]:
    # key -> start -> items
    buckets: dict[Hashable, dict[datetime, list[Envelope]]] = defaultdict(
        lambda: defaultdict(list)
    )
    rt = get_runtime()

    def close_ready(watermark: datetime | None) -> Iterator[Envelope]:
        if watermark is None:
            return
            yield  # pragma: no cover — makes this a generator
        for key, keyed in list(buckets.items()):
            for start, items in list(keyed.items()):
                end = start + size
                if watermark >= end + lateness and items:
                    yield _to_env(key, start, end, items)
                    del keyed[start]
            if not keyed:
                del buckets[key]

    for item in stream:
        env = as_envelope(item)
        et = _event_time(env)
        if et is None:
            et = current_now(rt)
            env = env.with_event_time(et)
        wm = rt.watermark if rt is not None else None
        if wm is not None and et < wm:
            emit_side("late", env)
            continue
        for start in _window_starts(et, size, slide, kind):
            buckets[env.key][start].append(env)
        yield from close_ready(wm)

    # end of stream: flush remaining
    for key, keyed in list(buckets.items()):
        for start, items in list(keyed.items()):
            if items:
                yield _to_env(key, start, start + size, items)
        keyed.clear()
    buckets.clear()


def _run_sessions(
    stream: Iterable, gap: timedelta, lateness: timedelta
) -> Iterator[Envelope]:
    # key -> (start, last_time, items)
    open_s: dict[Hashable, tuple[datetime, datetime, list[Envelope]]] = {}
    rt = get_runtime()

    def close_if_ready(watermark: datetime | None) -> Iterator[Envelope]:
        if watermark is None:
            return
            yield
        for key, (start, last, items) in list(open_s.items()):
            end = last + gap
            if watermark >= end + lateness and items:
                yield _to_env(key, start, end, items)
                del open_s[key]

    for item in stream:
        env = as_envelope(item)
        et = _event_time(env)
        if et is None:
            et = current_now(rt)
            env = env.with_event_time(et)
        wm = rt.watermark if rt is not None else None
        if wm is not None and et < wm:
            emit_side("late", env)
            continue
        cur = open_s.get(env.key)
        if cur is None:
            open_s[env.key] = (et, et, [env])
        else:
            start, last, items = cur
            if et - last > gap:
                yield _to_env(env.key, start, last + gap, items)
                open_s[env.key] = (et, et, [env])
            else:
                items.append(env)
                new_start = start if et >= start else et
                new_last = last if et <= last else et
                open_s[env.key] = (new_start, new_last, items)
        yield from close_if_ready(wm)

    for key, (start, last, items) in list(open_s.items()):
        if items:
            yield _to_env(key, start, last + gap, items)
    open_s.clear()


def current_now(rt: Any) -> datetime:
    if rt is not None:
        return rt.clock.now()
    return datetime.now(timezone.utc)
