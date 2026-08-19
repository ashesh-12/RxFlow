"""HTTP POST adapter: push JSON at the edge into a bounded QueueSource."""

from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from typing import Any, Iterator

from rxflow.connectors.memory import QueueSource
from rxflow.envelope import Envelope


class WebhookSource:
    """Stdlib HTTP server. POST JSON (object or list) to ``path``; topology pulls."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 0,
        path: str = "/events",
        **queue_kw: Any,
    ) -> None:
        self.host = host
        self.port = port
        self.path = path.rstrip("/") or "/events"
        self.queue = QueueSource(**queue_kw)
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: Thread | None = None

    def start(self) -> "WebhookSource":
        source = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt: str, *args: Any) -> None:
                return None

            def do_POST(self) -> None:  # noqa: N802
                if self.path.rstrip("/") != source.path:
                    self.send_error(404)
                    return
                length = int(self.headers.get("Content-Length", "0"))
                raw = self.rfile.read(length)
                try:
                    body = json.loads(raw.decode("utf-8") or "null")
                except json.JSONDecodeError:
                    self.send_error(400, "invalid json")
                    return
                items = body if isinstance(body, list) else [body]
                for item in items:
                    source.queue.push(item)
                self.send_response(202)
                self.end_headers()

        self._httpd = ThreadingHTTPServer((self.host, self.port), Handler)
        self.port = int(self._httpd.server_address[1])
        self._thread = Thread(target=self._httpd.serve_forever, daemon=True)
        self._thread.start()
        return self

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}{self.path}"

    def __iter__(self) -> Iterator[Envelope]:
        yield from self.queue

    def seek(self, partition: int | str, offset: int) -> None:
        self.queue.seek(partition, offset)

    def commit(self, partition: int | str, offset: int) -> None:
        self.queue.commit(partition, offset)

    def committed(self) -> dict:
        return self.queue.committed()

    def close(self) -> None:
        self.queue.close()
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
        if self._thread is not None:
            self._thread.join(timeout=2)
