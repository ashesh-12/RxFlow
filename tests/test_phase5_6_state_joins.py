"""Phase 5–6: checkpoints, restore, merge, joins, throttle."""

from pathlib import Path

from rxflow import (
    KeyValueStore,
    MemorySink,
    Observable,
    PartitionedLog,
    join_stream,
    join_table,
    merge,
    scan_op,
    take,
    throttle,
)
from rxflow.state import load_checkpoint, save_checkpoint
from tests.conftest import at, env


def test_checkpoint_restore_does_not_double_count(tmp_path: Path):
    store = KeyValueStore()
    log = PartitionedLog()
    log.append({"n": 1}, key="u")
    log.append({"n": 2}, key="u")
    log.append({"n": 3}, key="u")

    consumer = log.consumer()
    sink = MemorySink()
    Observable.from_source(consumer).pipe(
        scan_op(lambda s, e: s + e.payload["n"], 0),
        take(2),
    ).run(sinks=[sink], delivery="at_least_once", state=store, protect_sinks=False)

    save_checkpoint(tmp_path, store, {0: 1})
    saved = load_checkpoint(tmp_path)
    store2 = KeyValueStore()
    store2.load(saved["state"])
    consumer2 = log.consumer()
    consumer2.seek(0, saved["offsets"][0] + 1)

    sink2 = MemorySink()
    Observable.from_source(consumer2).pipe(
        scan_op(lambda s, e: s + e.payload["n"], 0),
    ).run(sinks=[sink2], delivery="at_least_once", state=store2, protect_sinks=False)

    assert sink.payloads == [1, 3]
    assert sink2.payloads == [6]  # 3 + 3, continuing from restored 3, not from 0


def test_run_restores_from_checkpoint_dir(tmp_path: Path):
    log = PartitionedLog()
    for n in (1, 2, 3, 4):
        log.append({"n": n}, key="u")

    sink = MemorySink()
    Observable.from_source(log.consumer()).pipe(
        scan_op(lambda s, e: s + e.payload["n"], 0),
        take(2),
    ).run(
        sinks=[sink],
        delivery="effectively_once",
        checkpoint_dir=str(tmp_path),
        protect_sinks=False,
    )
    assert sink.payloads == [1, 3]

    sink2 = MemorySink()
    Observable.from_source(log.consumer()).pipe(
        scan_op(lambda s, e: s + e.payload["n"], 0),
    ).run(
        sinks=[sink2],
        delivery="effectively_once",
        checkpoint_dir=str(tmp_path),
        protect_sinks=False,
    )
    assert sink2.payloads == [6, 10]


def test_join_table_enriches():
    table = {"u1": {"risk": 0.9}}
    events = [env({"amount": 10}, key="u1"), env({"amount": 1}, key="u2")]
    unmatched = MemorySink()
    from rxflow import Runtime

    rt = Runtime(side_outputs={"unmatched": unmatched})
    out = (
        Observable.from_iterable(events)
        .pipe(join_table(table))
        .collect(runtime=rt)
    )
    assert out[0].payload == {"left": {"amount": 10}, "right": {"risk": 0.9}}
    assert [e.key for e in unmatched.items] == ["u2"]


def test_stream_stream_join_within_window():
    left = Observable.from_iterable(
        [at(0, "tx", key="u"), at(100, "tx2", key="u")]
    )
    right = Observable.from_iterable([at(5, "login", key="u")])
    out = left.pipe(join_stream(right, window="10s")).collect()
    assert len(out) == 1
    assert out[0].payload == {"left": "tx", "right": "login"}


def test_merge_orders_by_event_time():
    a = Observable.from_iterable([at(10, "a"), at(30, "c")])
    b = Observable.from_iterable([at(20, "b")])
    out = merge(a, b).collect()
    assert [e.payload for e in out] == ["a", "b", "c"]


def test_throttle_per_key():
    events = [
        at(0, 1, key="a"),
        at(10, 2, key="a"),
        at(70, 3, key="a"),
        at(5, 9, key="b"),
    ]
    out = Observable.from_iterable(events).pipe(throttle("1m")).collect()
    assert [(e.key, e.payload) for e in out] == [("a", 1), ("a", 3), ("b", 9)]
