"""Drain, share, fan-out, time ops, live sources, window restore, lag."""

from __future__ import annotations

import threading
import time
from datetime import timedelta
from pathlib import Path

from rxflow import (
    FileSource,
    FullQueue,
    KeyValueStore,
    MemorySink,
    Observable,
    QueueSource,
    Runtime,
    Subscription,
    VirtualClock,
    assign_watermarks,
    debounce,
    fan_out,
    sample,
    tap,
    timeout,
    tumbling_window,
)
from tests.conftest import T0, at, env


def test_debounce_emits_last_after_quiet_period():
    events = [at(0, "a", key="k"), at(10, "b", key="k"), at(100, "c", key="k")]
    out = Observable.from_iterable(events).pipe(debounce("30s")).collect()
    assert [e.payload for e in out] == ["b", "c"]


def test_timeout_emits_marker_on_gap():
    events = [at(0, "a", key="k"), at(100, "c", key="k")]
    out = Observable.from_iterable(events).pipe(timeout("30s")).collect()
    payloads = [e.payload for e in out]
    assert payloads[0] == "a"
    assert payloads[1]["timeout"] is True
    assert payloads[2] == "c"


def test_sample_periodic_latest():
    events = [at(0, 1, key="k"), at(10, 2, key="k"), at(70, 3, key="k")]
    out = Observable.from_iterable(events).pipe(sample("1m")).collect()
    assert [e.payload for e in out] == [3]


def test_fan_out_writes_side_sink_and_main():
    extra = MemorySink()
    out = Observable.from_iterable([env(1), env(2)]).pipe(fan_out(extra)).collect()
    assert extra.payloads == [1, 2]
    assert [e.payload for e in out] == [1, 2]


def test_share_replays_oneshot_generator():
    gen = (env(x) for x in [1, 2])
    obs = Observable(lambda: gen).share()
    assert [e.payload for e in obs.collect()] == [1, 2]
    assert [e.payload for e in obs.collect()] == [1, 2]


def test_watermark_lag_is_clock_minus_watermark():
    clock = VirtualClock(T0 + timedelta(minutes=5))
    rt = Runtime(clock=clock)
    Observable.from_iterable([at(0, 1, key="s")]).pipe(
        assign_watermarks("0s")
    ).collect(runtime=rt)
    assert rt.metrics.watermark == T0
    assert rt.metrics.lag_seconds == 300.0


def test_cancel_keeps_open_window_state_for_restore():
    store = KeyValueStore()
    sub = Subscription()
    seen = {"n": 0}

    def count_and_cancel(_env):
        seen["n"] += 1
        if seen["n"] >= 2:
            sub.cancel()

    Observable.from_iterable(
        [at(0, "a", key="s"), at(10, "b", key="s"), at(20, "c", key="s")]
    ).pipe(
        tap(count_and_cancel),
        tumbling_window("1m", state_id="w"),
    ).subscribe(lambda _e: None, runtime=Runtime(state=store, subscription=sub))

    open_items = [
        item.payload
        for _start, items in store.namespace("w")["s"].items()
        for item in items
    ]
    assert open_items == ["a", "b"]

    out = (
        Observable.from_iterable([at(70, "d", key="s")])
        .pipe(tumbling_window("1m", state_id="w"))
        .collect(runtime=Runtime(state=store))
    )
    groups = [[i.payload for i in e.payload.items] for e in out]
    assert ["a", "b"] in groups
    assert ["d"] in groups


def test_drain_flushes_open_windows():
    q = QueueSource(maxsize=8, policy="drop_oldest")
    sink = MemorySink()
    sub = Subscription()

    def worker():
        Observable.from_source(q).pipe(tumbling_window("1m")).subscribe(
            sink.write, runtime=Runtime(subscription=sub)
        )

    t = threading.Thread(target=worker)
    t.start()
    q.push(at(0, "a", key="s"))
    q.push(at(10, "b", key="s"))
    time.sleep(0.08)
    sub.drain()
    t.join(timeout=2)
    assert t.is_alive() is False
    assert len(sink.items) == 1
    assert [i.payload for i in sink.items[0].payload.items] == ["a", "b"]


def test_file_source_follow(tmp_path: Path):
    path = tmp_path / "live.jsonl"
    path.write_text("")
    src = FileSource(path, follow=True, poll_interval=0.02)
    got: list = []

    def consume():
        for item in src:
            got.append(item.payload)

    t = threading.Thread(target=consume)
    t.start()
    time.sleep(0.04)
    with path.open("a") as fh:
        fh.write('{"n": 1}\n')
        fh.flush()
    deadline = time.time() + 2
    while not got and time.time() < deadline:
        time.sleep(0.02)
    src.close()
    t.join(timeout=2)
    assert got == [{"n": 1}]


def test_queue_block_raises_when_timeout_expires():
    q = QueueSource(maxsize=1, policy="block")
    assert q.push("a") is True
    try:
        q.push("b", timeout=0)
        raised = False
    except FullQueue:
        raised = True
    q.close()
    assert raised is True
