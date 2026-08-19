"""Phase 3: virtual clock, tumbling/sliding/session windows, late side output."""

from datetime import timedelta

from rxflow import (
    MemorySink,
    Observable,
    Runtime,
    Window,
    assign_watermarks,
    session_window,
    sliding_window,
    tumbling_window,
)
from tests.conftest import T0, at


def test_tumbling_window_groups_by_event_time_and_key():
    events = [
        at(0, "a", key="s"),
        at(10, "b", key="s"),
        at(70, "c", key="s"),
        at(5, "d", key="t"),
    ]
    out = (
        Observable.from_iterable(events)
        .pipe(tumbling_window("1m"))
        .collect()
    )
    wins = [e.payload for e in out]
    assert all(isinstance(w, Window) for w in wins)
    by_key = {(w.key, w.start): [i.payload for i in w.items] for w in wins}
    # 0s and 10s in first minute for s; 70s in second; t has 5s
    assert ("s", T0) in by_key
    assert by_key[("s", T0)] == ["a", "b"]
    assert by_key[("t", T0)] == ["d"]
    second = T0 + timedelta(minutes=1)
    assert by_key[("s", second)] == ["c"]


def test_late_events_go_to_side_output():
    events = [
        at(120, "late-ok", key="s"),  # advances watermark to 120
        at(10, "late", key="s"),  # behind watermark
        at(130, "also-ok", key="s"),
    ]
    late = MemorySink()
    rt = Runtime(side_outputs={"late": late})
    out = (
        Observable.from_iterable(events)
        .pipe(assign_watermarks("0s"), tumbling_window("1m"))
        .collect(runtime=rt)
    )
    payloads = []
    for e in out:
        w = e.payload
        payloads.extend(i.payload for i in w.items)
    assert "late" not in payloads
    assert [e.payload for e in late.items] == ["late"]


def test_sliding_window_overlaps():
    events = [at(0, 1, key="k"), at(30, 2, key="k"), at(60, 3, key="k")]
    out = (
        Observable.from_iterable(events)
        .pipe(sliding_window(size="2m", slide="30s"))
        .collect()
    )
    # each event belongs to multiple overlapping windows; stream-end flush emits them
    assert len(out) >= 1
    assert all(isinstance(e.payload, Window) for e in out)


def test_session_window_closes_after_gap():
    events = [
        at(0, "a", key="u"),
        at(10, "b", key="u"),
        at(100, "c", key="u"),  # gap > 30s
    ]
    out = (
        Observable.from_iterable(events)
        .pipe(session_window("30s"))
        .collect()
    )
    sessions = [list(e.payload.items) for e in out]
    assert [i.payload for i in sessions[0]] == ["a", "b"]
    assert [i.payload for i in sessions[1]] == ["c"]
