"""Source and sink contracts. Real Kafka/HTTP clients would implement these."""

from __future__ import annotations

from typing import Iterator, Protocol, runtime_checkable

from rxflow.envelope import Envelope


@runtime_checkable
class Source(Protocol):
    def __iter__(self) -> Iterator[Envelope]: ...

    def seek(self, partition: int | str, offset: int) -> None: ...

    def commit(self, partition: int | str, offset: int) -> None: ...

    def committed(self) -> dict: ...

    def close(self) -> None: ...


@runtime_checkable
class Sink(Protocol):
    def write(self, envelope: Envelope) -> None: ...

    def flush(self) -> None: ...

    def close(self) -> None: ...
