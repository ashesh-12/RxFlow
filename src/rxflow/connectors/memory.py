"""In-memory queue source (webhook/push adapter) with bounded-buffer policy."""

from __future__ import annotations

from collections import deque
from typing import Any, Iterator, Literal

from rxflow.envelope import Envelope, as_envelope

Backpressure = Literal["block", "drop_oldest", "sample"]


class QueueSource:
    """Push at the edge, pull from the topology. Memory stays bounded."""

    def __init__(self, maxsize: int = 256, policy: Backpressure = "block") -> None:
        self.maxsize = maxsize
        self.policy = policy
        self._q: deque[Envelope] = deque()
        self._closed = False
        self._pos = 0
        self._committed: dict[str, int] = {"q": -1}

    def push(self, item: Any) -> bool:
        """Return False if the item was dropped (sample policy / full)."""
        env = as_envelope(item).with_offset(self._pos, "q")
        if len(self._q) >= self.maxsize:
            if self.policy == "drop_oldest":
                self._q.popleft()
            elif self.policy == "sample":
                return False
            elif self.policy == "block":
                raise FullQueue(f"QueueSource full ({self.maxsize})")
        self._q.append(env)
        self._pos += 1
        return True

    def __iter__(self) -> Iterator[Envelope]:
        while self._q or not self._closed:
            if self._q:
                yield self._q.popleft()
            else:
                return

    def seek(self, partition: int | str, offset: int) -> None:
        return None

    def commit(self, partition: int | str, offset: int) -> None:
        self._committed[str(partition)] = int(offset)

    def committed(self) -> dict:
        return dict(self._committed)

    def close(self) -> None:
        self._closed = True

    @property
    def size(self) -> int:
        return len(self._q)


class FullQueue(Exception):
    pass
