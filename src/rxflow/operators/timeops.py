"""Processing/event-time rate operators: debounce, throttle, timeout, sample."""

from __future__ import annotations

from datetime import datetime, timezone

from rxflow.clock import parse_duration
from rxflow.compose import operator
from rxflow.context import get_runtime
from rxflow.envelope import Envelope, as_envelope


def _now(env, rt) -> datetime:
    if env.event_time is not None:
        return env.event_time
    if rt is not None:
        return rt.clock.now()
    return datetime.now(timezone.utc)


def throttle(interval):
    """Emit at most once per interval per key."""
    gap = parse_duration(interval)

    @operator
    def throttle(stream):
        last: dict = {}
        rt = get_runtime()
        for item in stream:
            env = as_envelope(item)
            t = _now(env, rt)
            k = env.key
            prev = last.get(k)
            if prev is None or t - prev >= gap:
                last[k] = t
                yield env

    return throttle()


def debounce(interval):
    """Emit the last value for a key after a quiet period (event-time)."""
    gap = parse_duration(interval)

    @operator
    def debounce(stream):
        held: dict = {}  # key -> (env, last_time)
        rt = get_runtime()
        for item in stream:
            env = as_envelope(item)
            t = _now(env, rt)
            # flush keys whose quiet period has passed relative to this event
            for k, (held_env, held_t) in list(held.items()):
                if t - held_t >= gap:
                    yield held_env
                    del held[k]
            held[env.key] = (env, t)
        for held_env, _ in held.values():
            yield held_env

    return debounce()


def timeout(interval):
    """If the gap between events for a key exceeds interval, emit a timeout marker first."""
    gap = parse_duration(interval)

    @operator
    def timeout(stream):
        last: dict = {}
        rt = get_runtime()
        for item in stream:
            env = as_envelope(item)
            t = _now(env, rt)
            k = env.key
            prev = last.get(k)
            if prev is not None and t - prev >= gap:
                yield Envelope(
                    payload={"timeout": True, "since": prev, "at": t},
                    key=k,
                    event_time=t,
                    headers=env.headers,
                )
            last[k] = t
            yield env

    return timeout()


def sample(interval):
    """Periodic latest value per key, aligned to event time. Flush leftover on end."""
    gap = parse_duration(interval)

    @operator
    def sample(stream):
        latest: dict = {}
        next_tick: dict = {}
        last_out: dict = {}
        rt = get_runtime()
        for item in stream:
            env = as_envelope(item)
            t = _now(env, rt)
            k = env.key
            latest[k] = env
            if k not in next_tick:
                next_tick[k] = t + gap
                continue
            if t >= next_tick[k]:
                yield env
                last_out[k] = env
                next_tick[k] = t + gap
        for k, env in latest.items():
            if last_out.get(k) is not env:
                yield env

    return sample()
