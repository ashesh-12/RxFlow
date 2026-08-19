"""Server error-rate monitoring — same engine, different topology and lambdas."""

from datetime import datetime, timedelta, timezone

from rxflow import (
    FileSource,
    MemorySink,
    Observable,
    StdoutSink,
    filter_op,
    key_by,
    map_payload,
    throttle,
    tumbling_window,
)


def main(path: str | None = None) -> None:
    t0 = datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc)
    if path is None:
        from rxflow import PartitionedLog

        log = PartitionedLog()
        lines = [
            {"service": "api", "status": 500},
            {"service": "api", "status": 500},
            {"service": "web", "status": 200},
            {"service": "api", "status": 500},
            {"service": "web", "status": 503},
        ]
        for i, row in enumerate(lines):
            log.append(row, key=row["service"], event_time=t0 + timedelta(seconds=i * 5))
        source = log.consumer()
    else:
        source = FileSource(path, jsonl=True)

    late = MemorySink()
    Observable.from_source(source).pipe(
        filter_op(lambda env: env.payload["status"] >= 500),
        key_by(lambda env: env.payload["service"]),
        tumbling_window(size="1m"),
        map_payload(
            lambda window: {
                "service": window.key,
                "errors_in_window": len(window.items),
            }
        ),
        throttle(interval="1m"),
    ).run(
        sinks=[StdoutSink()],
        side_outputs={"late": late},
        delivery="at_least_once",
        protect_sinks=False,
    )


if __name__ == "__main__":
    main()
