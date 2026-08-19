"""Engine exceptions. Downstream stages must not re-wrap these."""

from __future__ import annotations


class StreamProcessingError(Exception):
    """Wraps any exception raised inside an operator with stage context."""

    def __init__(self, stage: str, original: BaseException):
        super().__init__(f"Stage '{stage}' failed: {original}")
        self.stage = stage
        self.original = original
