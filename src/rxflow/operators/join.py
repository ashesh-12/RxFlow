"""Stream–table and stream–stream joins. Match key is the envelope key."""

from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Mapping

from rxflow.clock import parse_duration
from rxflow.compose import operator
from rxflow.context import emit_side, get_runtime
from rxflow.envelope import Envelope, as_envelope


def _ts(env: Envelope) -> datetime:
    if env.event_time is not None:
        return env.event_time
    if env.ingest_time is not None:
        return env.ingest_time
    return datetime.min.replace(tzinfo=timezone.utc)


def join_table(table: Mapping[Any, Any] | None = None, *, state_id: str | None = None, how: str = "inner"):
    """Enrich each event with a slowly-changing table row. ``how``: inner | left."""

    @operator
    def join_table(stream):
        rt = get_runtime()
        store = None
        if table is None and state_id is not None and rt is not None:
            store = rt.state.namespace(state_id)
        for item in stream:
            env = as_envelope(item)
            k = env.key
            if table is not None:
                row = table.get(k) if hasattr(table, "get") else None
            elif store is not None:
                row = store.get(k)
            else:
                row = None
            if row is None:
                if how == "left":
                    yield env.with_payload({"left": env.payload, "right": None})
                else:
                    emit_side("unmatched", env)
                continue
            yield env.with_payload({"left": env.payload, "right": row})

    return join_table()


def as_table(key_fn=None, *, state_id: str = "table"):
    """Upsert envelopes into the state store (changelog → table). Yields the event through."""

    @operator
    def as_table(stream):
        rt = get_runtime()
        for item in stream:
            env = as_envelope(item)
            k = key_fn(env) if key_fn is not None else env.key
            if rt is not None:
                rt.state.put(state_id, k, env.payload)
            yield env.with_key(k)

    return as_table()


def join_stream(other, window, *, how: str = "inner"):
    """Match two streams on key within ±window of event time."""
    win = parse_duration(window)

    @operator
    def join_stream(stream):
        right_src = other._source_fn() if hasattr(other, "_source_fn") else iter(other)
        yield from _stream_stream(stream, right_src, win, how)

    return join_stream()


class _Peek:
    __slots__ = ("_it", "done", "cur")

    def __init__(self, it):
        self._it = iter(it)
        self.done = False
        self.cur = None
        self.advance()

    def advance(self) -> None:
        try:
            self.cur = as_envelope(next(self._it))
        except StopIteration:
            self.done = True
            self.cur = None


def _stream_stream(left_src, right_src, window, how: str):
    left_buf: dict[Any, deque] = defaultdict(deque)
    right_buf: dict[Any, deque] = defaultdict(deque)
    left, right = _Peek(left_src), _Peek(right_src)

    def evict(buf: deque, now) -> None:
        while buf and now - _ts(buf[0]) > window:
            buf.popleft()

    def probe(env: Envelope, other_buf: deque, side: str):
        t = _ts(env)
        evict(other_buf, t)
        matched = False
        for other in other_buf:
            if abs(_ts(other) - t) <= window:
                matched = True
                payload = (
                    {"left": env.payload, "right": other.payload}
                    if side == "left"
                    else {"left": other.payload, "right": env.payload}
                )
                yield env.with_payload(payload)
        if not matched and how == "left" and side == "left":
            yield env.with_payload({"left": env.payload, "right": None})

    def ingest(env: Envelope, side: str):
        buf = left_buf if side == "left" else right_buf
        other = right_buf if side == "left" else left_buf
        t = _ts(env)
        evict(buf[env.key], t)
        buf[env.key].append(env)
        yield from probe(env, other[env.key], side)

    while not left.done or not right.done:
        if left.done:
            yield from ingest(right.cur, "right")
            right.advance()
        elif right.done:
            yield from ingest(left.cur, "left")
            left.advance()
        elif _ts(left.cur) <= _ts(right.cur):
            yield from ingest(left.cur, "left")
            left.advance()
        else:
            yield from ingest(right.cur, "right")
            right.advance()
