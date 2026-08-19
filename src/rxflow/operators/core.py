"""Stateless / light operators. Domain callables receive the Envelope."""

from __future__ import annotations

from typing import Any, Callable, Iterable

from rxflow.compose import SKIP, apply_policy, operator
from rxflow.context import get_runtime, send_dlq
from rxflow.envelope import Envelope, as_envelope


@operator
def map_op(stream, transform_fn, *, on_error="fail_job", retries=0):
    """Transform. If ``fn`` returns an Envelope it is used as-is; otherwise payload is replaced."""
    for item in stream:
        env = as_envelope(item)
        result = apply_policy(
            "map_op", transform_fn, env, on_error=on_error, retries=retries
        )
        if result is SKIP:
            continue
        yield result if isinstance(result, Envelope) else env.with_payload(result)


def map_payload(transform_fn, **kwargs):
    """Like map_op, but the domain function sees payload only."""
    return map_op(lambda env: transform_fn(env.payload), **kwargs)


@operator
def filter_op(stream, predicate_fn, *, on_error="fail_job", retries=0):
    rt = get_runtime()
    for item in stream:
        env = as_envelope(item)
        keep = apply_policy(
            "filter_op", predicate_fn, env, on_error=on_error, retries=retries
        )
        if keep is SKIP:
            continue
        if keep:
            yield env
        elif rt is not None:
            rt.metrics.record_dropped("filter_op")


@operator
def flat_map(stream, transform_fn, *, on_error="fail_job", retries=0):
    """One event in, zero or more out."""
    for item in stream:
        env = as_envelope(item)
        result = apply_policy(
            "flat_map", transform_fn, env, on_error=on_error, retries=retries
        )
        if result is SKIP or result is None:
            continue
        for out in result:
            yield out if isinstance(out, Envelope) else env.with_payload(out)


@operator
def tap(stream, effect_fn, *, on_error="fail_job", retries=0):
    """Side effect without changing the item. I/O belongs here, not in predicates."""
    for item in stream:
        env = as_envelope(item)
        result = apply_policy("tap", effect_fn, env, on_error=on_error, retries=retries)
        if result is SKIP:
            continue
        yield env


@operator
def take(stream, n: int):
    if n <= 0:
        return
    i = 0
    for item in stream:
        yield as_envelope(item)
        i += 1
        if i >= n:
            return


@operator
def skip(stream, n: int):
    i = 0
    for item in stream:
        if i >= n:
            yield as_envelope(item)
        else:
            i += 1


@operator
def validate(stream, schema_fn, *, on_invalid="dlq"):
    """Named schema check. Invalid events go to DLQ, are skipped, or fail the job."""
    for item in stream:
        env = as_envelope(item)
        try:
            ok = bool(schema_fn(env.payload))
        except Exception as exc:
            ok = False
            if on_invalid == "fail_job":
                raise
            send_dlq(env, "validate", exc)
            continue
        if ok:
            yield env
            continue
        if on_invalid == "fail_job":
            raise ValueError("validation failed")
        send_dlq(env, "validate", "validation failed")
        rt = get_runtime()
        if rt is not None:
            rt.metrics.record_dropped("validate")


@operator
def assign_trace_id(stream, header: str = "trace_id"):
    from uuid import uuid4

    for item in stream:
        env = as_envelope(item)
        if header in env.headers:
            yield env
        else:
            yield env.with_header(header, uuid4().hex)


def identity() -> Callable[[Iterable[Any]], Iterable[Any]]:
    return lambda stream: stream
