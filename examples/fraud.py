"""Fraud detection topology — configuration only; the engine does not change."""

from datetime import datetime, timedelta, timezone

from rxflow import (
    Observable,
    PartitionedLog,
    distinct_until_changed,
    filter_op,
    join_table,
    key_by,
    map_payload,
    sliding_window,
    throttle,
    validate,
    StdoutSink,
)


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
    }


def main() -> None:
    t0 = datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc)
    log = PartitionedLog()
    rows = [
        {"user": "alice", "amount": 6000.0, "type": "BUY"},
        {"user": "alice", "amount": 6000.0, "type": "BUY"},  # dup
        {"user": "bob", "amount": 100.0, "type": "BUY"},
        {"user": "alice", "amount": 8000.0, "type": "BUY"},
        {"user": "alice", "amount": 100.0, "type": "SELL"},
        "not-a-transaction",
    ]
    for i, row in enumerate(rows):
        key = row["user"] if isinstance(row, dict) else None
        log.append(row, key=key, event_time=t0 + timedelta(seconds=i * 10))

    user_risk = {"alice": {"tier": "high"}, "bob": {"tier": "low"}}

    Observable.from_source(log.consumer()).pipe(
        validate(transaction_schema),
        key_by(lambda env: env.payload["user"]),
        distinct_until_changed(
            key_fn=lambda env: (
                env.payload["user"],
                env.payload["amount"],
                env.payload["type"],
            )
        ),
        filter_op(
            lambda env: env.payload["type"] == "BUY" and env.payload["amount"] > 5000.0
        ),
        join_table(user_risk),
        map_payload(lambda p: p["left"]),
        sliding_window(size="2m", slide="30s"),
        map_payload(score_window),
        throttle(interval="1m"),
    ).run(sinks=[StdoutSink()], delivery="at_least_once", protect_sinks=False)


if __name__ == "__main__":
    main()
