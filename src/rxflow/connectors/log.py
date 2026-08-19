"""Simulated partitioned log: Kafka-shaped offsets and replay, zero dependencies."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Hashable, Iterator, Mapping

from rxflow.envelope import Envelope


class PartitionedLog:
    """Append-only in-memory log. ``consumer()`` reads envelopes with offsets."""

    def __init__(self, partitions: int = 1) -> None:
        self._parts: dict[int, list[Envelope]] = defaultdict(list)
        for p in range(partitions):
            self._parts[p]  # ensure present
        self.closed = False

    def append(
        self,
        payload: Any,
        *,
        key: Hashable | None = None,
        event_time: datetime | None = None,
        partition: int = 0,
        headers: Mapping[str, Any] | None = None,
    ) -> Envelope:
        offset = len(self._parts[partition])
        env = Envelope(
            payload=payload,
            key=key,
            event_time=event_time,
            offset=offset,
            partition=partition,
            headers=headers or {},
        )
        self._parts[partition].append(env)
        return env

    def records(self, partition: int) -> list[Envelope]:
        return self._parts[partition]

    def partitions(self) -> list[int]:
        return sorted(self._parts)

    def close(self) -> None:
        self.closed = True

    def consumer(self, start_offsets: dict[int, int] | None = None) -> "LogConsumer":
        return LogConsumer(self, start_offsets)


class LogConsumer:
    """Pulls from a PartitionedLog. commit/seek enable at-least-once replay."""

    def __init__(
        self, log: PartitionedLog, start_offsets: dict[int, int] | None = None
    ) -> None:
        self._log = log
        self._pos: dict[int, int] = {
            p: (start_offsets or {}).get(p, 0) for p in log.partitions()
        }
        self._committed: dict[int, int] = dict(self._pos)
        self._closed = False

    def __iter__(self) -> Iterator[Envelope]:
        while not self._closed:
            progressed = False
            for p in list(self._pos):
                records = self._log.records(p)
                pos = self._pos[p]
                while pos < len(records) and not self._closed:
                    env = records[pos]
                    pos += 1
                    self._pos[p] = pos
                    progressed = True
                    yield env
            if not progressed:
                return

    def seek(self, partition: int | str, offset: int) -> None:
        self._pos[int(partition)] = offset

    def commit(self, partition: int | str, offset: int) -> None:
        """Last fully processed offset (inclusive). Next read after restore is offset+1."""
        self._committed[int(partition)] = offset

    def committed(self) -> dict[int, int]:
        return dict(self._committed)

    def position(self) -> dict[int, int]:
        return dict(self._pos)

    def close(self) -> None:
        self._closed = True
