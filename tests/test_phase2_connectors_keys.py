"""Phase 2: connectors, key_by, per-key scan/distinct."""

from pathlib import Path

from rxflow import (
    FileSource,
    MemorySink,
    Observable,
    PartitionedLog,
    QueueSource,
    distinct_until_changed,
    key_by,
    scan_op,
)
from tests.conftest import env


def test_key_by_and_per_key_scan():
    events = [
        env({"user": "a", "n": 1}),
        env({"user": "b", "n": 10}),
        env({"user": "a", "n": 1}),
    ]
    out = (
        Observable.from_iterable(events)
        .pipe(
            key_by(lambda e: e.payload["user"]),
            scan_op(lambda s, e: s + e.payload["n"], 0),
        )
        .collect()
    )
    assert [(e.key, e.payload) for e in out] == [("a", 1), ("b", 10), ("a", 2)]


def test_distinct_until_changed_is_per_key():
    events = [
        env("x", key="a"),
        env("x", key="b"),  # different key — keep
        env("x", key="a"),  # same as last for a — drop
        env("y", key="a"),  # change for a — keep
    ]
    out = Observable.from_iterable(events).pipe(distinct_until_changed()).collect()
    assert [(e.key, e.payload) for e in out] == [("a", "x"), ("b", "x"), ("a", "y")]


def test_partitioned_log_offsets_and_replay():
    log = PartitionedLog()
    log.append("a", partition=0)
    log.append("b", partition=0)
    log.append("c", partition=0)

    first = list(log.consumer())
    assert [e.payload for e in first] == ["a", "b", "c"]
    assert [e.offset for e in first] == [0, 1, 2]

    replay = list(log.consumer(start_offsets={0: 1}))
    assert [e.payload for e in replay] == ["b", "c"]


def test_from_source_through_topology():
    log = PartitionedLog()
    log.append({"k": "u", "v": 1}, key="u")
    log.append({"k": "u", "v": 2}, key="u")
    sink = MemorySink()
    Observable.from_source(log.consumer()).pipe(
        scan_op(lambda s, e: s + e.payload["v"], 0)
    ).run(sinks=[sink], delivery="at_least_once", protect_sinks=False)
    assert sink.payloads == [1, 3]


def test_file_source_uses_line_offset(tmp_path: Path):
    path = tmp_path / "app.jsonl"
    path.write_text('{"service": "api", "status": 500}\n{"service": "web", "status": 200}\n')
    src = FileSource(path)
    items = list(src)
    assert items[0].payload == {"service": "api", "status": 500}
    assert items[0].offset == 0
    src.seek(str(path), 1)
    rest = list(src)
    assert [e.payload["service"] for e in rest] == ["web"]


def test_queue_source_drop_oldest():
    q = QueueSource(maxsize=2, policy="drop_oldest")
    q.push("a")
    q.push("b")
    q.push("c")  # drops a
    q.close()
    assert [e.payload for e in q] == ["b", "c"]
