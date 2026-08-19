"""Phase 1: envelope, compose, operator factory, core operators, subscribe."""

from datetime import timezone

import pytest

from rxflow import (
    Envelope,
    Observable,
    StreamProcessingError,
    compose,
    filter_op,
    map_op,
    scan_op,
    buffer_count,
    take,
    skip,
    tap,
    flat_map,
)
from tests.conftest import env


def test_envelope_is_frozen_and_updates_immutably():
    e = env({"n": 1}, key="u")
    e2 = e.with_payload({"n": 2}).with_key("v")
    assert e.payload == {"n": 1} and e.key == "u"
    assert e2.payload == {"n": 2} and e2.key == "v"
    with pytest.raises(Exception):
        e.payload = {}  # type: ignore[misc]


def test_headers_cannot_be_mutated_in_place():
    e = env(1, trace_id="abc")
    with pytest.raises(TypeError):
        e.headers["trace_id"] = "hacked"  # type: ignore[index]


def test_map_filter_identity_and_compose_associativity():
    add1 = map_op(lambda e: e.payload + 1)
    even = filter_op(lambda e: e.payload % 2 == 0)
    source = [env(1), env(2), env(3), env(4)]

    left = compose(compose(add1, add1), even)
    right = compose(add1, compose(add1, even))
    flat = compose(add1, add1, even)

    def payloads(op):
        return [as_p.payload for as_p in op(iter(source))]

    assert payloads(left) == payloads(right) == payloads(flat) == [4, 6]


def test_error_tagged_with_failing_stage_and_not_rewrapped():
    boom = map_op(lambda e: e.payload / 0)
    downstream = filter_op(lambda e: True)
    pipeline = compose(boom, downstream)
    with pytest.raises(StreamProcessingError) as caught:
        list(pipeline(iter([env(1)])))
    err = caught.value
    assert err.stage == "map_op"
    assert isinstance(err.original, ZeroDivisionError)
    assert caught.value is err  # single wrap


def test_scan_empty_stream_does_not_emit_seed():
    obs = Observable.from_iterable([]).pipe(scan_op(lambda s, e: s + e.payload, 0))
    assert obs.collect() == []


def test_scan_running_sum():
    obs = Observable.from_iterable([env(1), env(2), env(3)]).pipe(
        scan_op(lambda s, e: s + e.payload, 0)
    )
    assert [e.payload for e in obs.collect()] == [1, 3, 6]


def test_buffer_count_flushes_leftover():
    obs = Observable.from_iterable([env(i) for i in range(5)]).pipe(buffer_count(3))
    batches = [e.payload for e in obs.collect()]
    assert [tuple(x.payload for x in batch) for batch in batches] == [
        (0, 1, 2),
        (3, 4),
    ]


def test_take_and_skip():
    src = Observable.from_iterable([env(i) for i in range(5)])
    assert [e.payload for e in src.pipe(skip(2), take(2)).collect()] == [2, 3]


def test_tap_does_not_change_items():
    seen = []
    out = Observable.from_iterable([env(1), env(2)]).pipe(tap(seen.append)).collect()
    assert [e.payload for e in out] == [1, 2]
    assert [e.payload for e in seen] == [1, 2]


def test_flat_map_expands():
    out = (
        Observable.from_iterable([env(2)])
        .pipe(flat_map(lambda e: [e.payload, e.payload * 10]))
        .collect()
    )
    assert [e.payload for e in out] == [2, 20]


def test_from_iterable_replays_second_subscribe():
    obs = Observable.from_iterable([env(1), env(2)])
    assert [e.payload for e in obs.collect()] == [1, 2]
    assert [e.payload for e in obs.collect()] == [1, 2]


def test_oneshot_generator_second_subscribe_is_empty():
    gen = (env(x) for x in [1, 2, 3])
    obs = Observable(lambda: gen)
    assert [e.payload for e in obs.collect()] == [1, 2, 3]
    assert obs.collect() == []


def test_subscribe_cancel():
    from rxflow import Runtime, Subscription

    seen = []
    sub = Subscription()

    def on_next(e):
        seen.append(e.payload)
        if e.payload >= 2:
            sub.cancel()

    Observable.from_iterable([env(i) for i in range(20)]).subscribe(
        on_next, runtime=Runtime(subscription=sub)
    )
    assert seen == [0, 1, 2]


def test_map_skip_policy_drops_poison_and_continues():
    def flaky(e):
        if e.payload == 2:
            raise ValueError("poison")
        return e.payload * 10

    out = (
        Observable.from_iterable([env(1), env(2), env(3)])
        .pipe(map_op(flaky, on_error="skip"))
        .collect()
    )
    assert [e.payload for e in out] == [10, 30]


def test_ingest_time_is_timezone_aware():
    e = Envelope(payload=1)
    assert e.ingest_time.tzinfo == timezone.utc
