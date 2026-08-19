"""In-memory queue source (webhook/push adapter) with bounded-buffer policy."""

from __future__ import annotations

import threading
from collections import deque
from queue import Empty, Full, Queue
from typing import Any, Iterator, Literal

from rxflow.envelope import Envelope, as_envelope

Backpressure = Literal["block", "drop_oldest", "sample"]


class QueueSource:
    """Push at the edge, pull from the topology. Memory stays bounded.

    ``block`` waits when the buffer is full (true backpressure).
    ``drop_oldest`` / ``sample`` never wait; they evict or reject.
    After ``close()`` / drain, remaining buffered items are still yielded.
    """

    def __init__(self, maxsize: int = 256, policy: Backpressure = "block") -> None:
        self.maxsize = maxsize
        self.policy = policy
        self._closed = threading.Event()
        self._pos = 0
        self._lock = threading.Lock()
        self._committed: dict[str, int] = {"q": -1}
        if policy == "block":
            self._q: Queue | deque = Queue(maxsize=maxsize)
        else:
            self._q = deque()
            self._cv = threading.Condition(self._lock)

    def push(self, item: Any, *, timeout: float | None = None) -> bool:
        """Return False if the item was dropped (sample policy / full with timeout)."""
        if self._closed.is_set():
            return False
        with self._lock:
            offset = self._pos
            self._pos += 1
        env = as_envelope(item).with_offset(offset, "q")
        if self.policy == "block":
            try:
                self._q.put(env, timeout=timeout)  # type: ignore[union-attr]
                return True
            except Full:
                raise FullQueue(f"QueueSource full ({self.maxsize})") from None
        with self._cv:
            if len(self._q) >= self.maxsize:
                if self.policy == "drop_oldest":
                    self._q.popleft()
                elif self.policy == "sample":
                    return False
            self._q.append(env)
            self._cv.notify()
            return True

    def __iter__(self) -> Iterator[Envelope]:
        while True:
            env = self._take()
            if env is None:
                return
            yield env

    def _take(self) -> Envelope | None:
        if self.policy == "block":
            while True:
                try:
                    return self._q.get(timeout=0.05)  # type: ignore[union-attr]
                except Empty:
                    if self._closed.is_set() and self._q.empty():  # type: ignore[union-attr]
                        return None
        with self._cv:
            while not self._q and not self._closed.is_set():
                self._cv.wait(timeout=0.05)
            if not self._q:
                return None
            return self._q.popleft()

    def seek(self, partition: int | str, offset: int) -> None:
        return None

    def commit(self, partition: int | str, offset: int) -> None:
        self._committed[str(partition)] = int(offset)

    def committed(self) -> dict:
        return dict(self._committed)

    def close(self) -> None:
        self._closed.set()
        if self.policy != "block":
            with self._cv:
                self._cv.notify_all()

    @property
    def size(self) -> int:
        if self.policy == "block":
            return self._q.qsize()  # type: ignore[union-attr]
        return len(self._q)


class FullQueue(Exception):
    pass
