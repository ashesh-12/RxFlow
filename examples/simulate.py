"""End-to-end simulation: same engine, two topologies, one run.

Nothing extra to install. From the repo root:

    python examples/simulate.py
    python examples/simulate.py --live
"""

from __future__ import annotations

import argparse
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from rxflow import (  # noqa: E402
    CallbackSink,
    MemorySink,
    Observable,
    PartitionedLog,
    QueueSource,
    VirtualClock,
    assign_watermarks,
    distinct_until_changed,
    filter_op,
    join_table,
    key_by,
    map_payload,
    sliding_window,
    throttle,
    tumbling_window,
    validate,
)

UTC = timezone.utc
T0 = datetime(2024, 6, 1, 12, 0, tzinfo=UTC)


def transaction_schema(payload) -> bool:
    return (
        isinstance(payload, dict)
        and "user" in payload
        and "amount" in payload
        and "type" in payload
    )


def score_window(window) -> dict:
    total = sum(item.payload["amount"] for item in window.items)
    return {
        "user": window.key,
        "risk_score": total,
        "n": len(window.items),
        "window_end": window.end.isoformat(),
    }


def fraud_events(t0: datetime) -> list[tuple[datetime, object]]:
    """(event_time, payload) — includes duplicate, small buy, poison, late, large buys."""
    return [
        (t0 + timedelta(seconds=0), {"user": "alice", "amount": 6000.0, "type": "BUY"}),
        (t0 + timedelta(seconds=5), {"user": "alice", "amount": 6000.0, "type": "BUY"}),
        (t0 + timedelta(seconds=15), {"user": "bob", "amount": 100.0, "type": "BUY"}),
        (t0 + timedelta(seconds=20), {"user": "alice", "amount": 8000.0, "type": "BUY"}),
        (t0 + timedelta(seconds=25), {"user": "carol", "amount": 9000.0, "type": "BUY"}),
        (t0 + timedelta(seconds=30), {"user": "alice", "amount": 100.0, "type": "SELL"}),
        (t0 + timedelta(seconds=35), "not-a-transaction"),
        (t0 + timedelta(seconds=40), {"user": "bob", "amount": 7500.0, "type": "BUY"}),
        (t0 - timedelta(minutes=10), {"user": "alice", "amount": 9999.0, "type": "BUY"}),
        (t0 + timedelta(seconds=90), {"user": "carol", "amount": 6100.0, "type": "BUY"}),
    ]


def log_events(t0: datetime) -> list[tuple[datetime, dict]]:
    return [
        (t0 + timedelta(seconds=0), {"service": "api", "status": 500}),
        (t0 + timedelta(seconds=5), {"service": "api", "status": 500}),
        (t0 + timedelta(seconds=8), {"service": "web", "status": 200}),
        (t0 + timedelta(seconds=12), {"service": "api", "status": 500}),
        (t0 + timedelta(seconds=20), {"service": "web", "status": 503}),
        (t0 + timedelta(seconds=70), {"service": "api", "status": 500}),
    ]


def _banner(title: str) -> None:
    print()
    print("=" * 60)
    print(title)
    print("=" * 60)


def _print_metrics(sub) -> None:
    metrics = getattr(sub, "metrics", None)
    if metrics is None:
        return
    summary = metrics.summary()
    print("\nmetrics")
    if summary["watermark"]:
        print(f"  watermark: {summary['watermark']}")
        print(f"  lag_seconds: {summary['lag_seconds']}")
    for stage, stats in summary["stages"].items():
        interesting = {k: v for k, v in stats.items() if v}
        if interesting:
            print(f"  {stage}: {interesting}")


def run_fraud(source, *, dlq, late, alerts) -> object:
    user_risk = {
        "alice": {"tier": "high"},
        "bob": {"tier": "low"},
        "carol": {"tier": "high"},
    }
    return (
        Observable.from_source(source)
        .pipe(
            validate(transaction_schema),
            assign_watermarks("0s"),
            key_by(lambda env: env.payload["user"]),
            distinct_until_changed(
                key_fn=lambda env: (
                    env.payload["user"],
                    env.payload["amount"],
                    env.payload["type"],
                )
            ),
            filter_op(
                lambda env: env.payload["type"] == "BUY"
                and env.payload["amount"] > 5000.0
            ),
            join_table(user_risk),
            map_payload(lambda p: p["left"]),
            sliding_window(size="2m", slide="30s"),
            map_payload(score_window),
            throttle(interval="1m"),
        )
        .run(
            sinks=[CallbackSink(lambda e: alerts.append(e.payload) or print("ALERT", e.payload))],
            dlq=dlq,
            side_outputs={"late": late, "unmatched": late},
            delivery="at_least_once",
            protect_sinks=False,
            clock=VirtualClock(T0 + timedelta(minutes=2)),
        )
    )


def run_errors(source, *, late, summaries) -> object:
    return (
        Observable.from_source(source)
        .pipe(
            filter_op(lambda env: env.payload["status"] >= 500),
            key_by(lambda env: env.payload["service"]),
            tumbling_window(size="1m"),
            map_payload(
                lambda window: {
                    "service": window.key,
                    "errors_in_window": len(window.items),
                    "window_end": window.end.isoformat(),
                }
            ),
            throttle(interval="1m"),
        )
        .run(
            sinks=[
                CallbackSink(lambda e: summaries.append(e.payload) or print("RATE", e.payload))
            ],
            side_outputs={"late": late},
            delivery="at_least_once",
            protect_sinks=False,
            clock=VirtualClock(T0 + timedelta(minutes=2)),
        )
    )


def load_log(events) -> PartitionedLog:
    log = PartitionedLog()
    for ts, payload in events:
        key = payload["user"] if isinstance(payload, dict) and "user" in payload else (
            payload["service"] if isinstance(payload, dict) else None
        )
        log.append(payload, key=key, event_time=ts)
    return log


def run_live(delay: float) -> None:
    _banner("LIVE simulation (producer thread → QueueSource)")
    q = QueueSource(maxsize=32, policy="block")
    dlq, late, alerts = MemorySink(), MemorySink(), []

    def produce():
        for ts, payload in fraud_events(T0):
            q.push({"payload": payload, "_ts": ts})
            time.sleep(delay)
        q.close()

    threading.Thread(target=produce, daemon=True).start()

    def with_event_time(env):
        body = env.payload
        return env.with_payload(body["payload"]).with_event_time(body["_ts"])

    from rxflow import map_op

    user_risk = {"alice": {"tier": "high"}, "bob": {"tier": "low"}, "carol": {"tier": "high"}}
    sub = (
        Observable.from_source(q)
        .pipe(
            map_op(with_event_time),
            validate(transaction_schema),
            assign_watermarks("0s"),
            key_by(lambda env: env.payload["user"]),
            filter_op(
                lambda env: env.payload["type"] == "BUY" and env.payload["amount"] > 5000.0
            ),
            join_table(user_risk),
            map_payload(lambda p: p["left"]),
            sliding_window(size="2m", slide="30s"),
            map_payload(score_window),
        )
        .run(
            sinks=[CallbackSink(lambda e: alerts.append(e.payload) or print("ALERT", e.payload))],
            dlq=dlq,
            side_outputs={"late": late},
            delivery="at_least_once",
            protect_sinks=False,
            clock=VirtualClock(T0 + timedelta(minutes=2)),
        )
    )
    print(f"\nalerts={len(alerts)}  dlq={len(dlq.items)}  late={len(late.items)}")
    _print_metrics(sub)


def run_batch() -> None:
    _banner("FRAUD  (simulated transaction log)")
    print("poison JSON → DLQ   |   late event → side output   |   large BUYs → windows")
    dlq, late, alerts = MemorySink(), MemorySink(), []
    log = load_log(fraud_events(T0))
    sub = run_fraud(log.consumer(), dlq=dlq, late=late, alerts=alerts)
    print(f"\n{len(alerts)} alert(s)  |  {len(dlq.items)} dlq  |  {len(late.items)} late")
    if dlq.items:
        print("dlq payloads:", [e.payload for e in dlq.items])
    if late.items:
        print("late payloads:", [e.payload for e in late.items])
    _print_metrics(sub)

    _banner("ERROR RATE  (same engine, different lambdas)")
    late2, summaries = MemorySink(), []
    logs = load_log(log_events(T0))
    sub2 = run_errors(logs.consumer(), late=late2, summaries=summaries)
    print(f"\n{len(summaries)} summary(ies)")
    _print_metrics(sub2)

    _banner("What you just ran")
    print("No Kafka, no extra packages. PartitionedLog is the Kafka stand-in.")
    print("To replay: same log, consumer(start_offsets=...).")
    print("Live push: python examples/simulate.py --live")
    print("Webhook (optional): WebhookSource().start() then POST JSON to its .url")


def main() -> None:
    parser = argparse.ArgumentParser(description="RxFlow end-to-end simulation")
    parser.add_argument(
        "--live",
        action="store_true",
        help="Producer thread pushes events with a short delay",
    )
    parser.add_argument("--delay", type=float, default=0.08, help="Live delay between events")
    args = parser.parse_args()
    if args.live:
        run_live(args.delay)
    else:
        run_batch()


if __name__ == "__main__":
    main()
