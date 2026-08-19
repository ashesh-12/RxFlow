"""Stateful operators. Closures (or the runtime store) hold per-key state.

After ``key_by``, distinct and scan run per key. ``key is None`` is one partition.
"""

from __future__ import annotations

from rxflow.compose import SKIP, apply_policy, operator
from rxflow.context import get_runtime
from rxflow.envelope import as_envelope


@operator
def distinct_until_changed(stream, key_fn=None):
    """Drop consecutive duplicates. Partitioned by envelope key."""
    last: dict = {}
    for item in stream:
        env = as_envelope(item)
        ident = key_fn(env) if key_fn is not None else env.payload
        part = env.key
        if part not in last or last[part] != ident:
            last[part] = ident
            yield env


@operator
def scan_op(
    stream,
    accumulator_fn,
    seed,
    *,
    state_id: str = "scan",
    on_error="fail_job",
    retries=0,
):
    """Running accumulator per key. Seed is not emitted on an empty stream."""
    rt = get_runtime()
    states: dict = dict(rt.state.namespace(state_id)) if rt is not None else {}
    for item in stream:
        env = as_envelope(item)
        k = env.key
        prev = states[k] if k in states else seed
        result = apply_policy(
            "scan_op",
            lambda e, p=prev: accumulator_fn(p, e),
            env,
            on_error=on_error,
            retries=retries,
        )
        if result is SKIP:
            continue
        states[k] = result
        if rt is not None:
            rt.state.put(state_id, k, result)
        yield env.with_payload(result)


@operator
def buffer_count(stream, size: int):
    """Count-based batch. Leftover items are flushed when the stream ends."""
    buf = []
    for item in stream:
        buf.append(as_envelope(item))
        if len(buf) == size:
            batch = tuple(buf)
            buf = []
            last = batch[-1]
            yield last.with_payload(batch)
    if buf:
        last = buf[-1]
        yield last.with_payload(tuple(buf))
