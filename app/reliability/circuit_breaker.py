from __future__ import annotations

from datetime import datetime, timedelta, timezone


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class CircuitBreaker:
    def __init__(self, failure_threshold: int, recovery_timeout_ms: int) -> None:
        self.failure_threshold = failure_threshold
        self.recovery_timeout_ms = recovery_timeout_ms
        self.failure_count = 0
        self.opened_at: datetime | None = None

    def allow_request(self) -> bool:
        if self.opened_at is None:
            return True

        elapsed = utcnow() - self.opened_at
        if elapsed >= timedelta(milliseconds=self.recovery_timeout_ms):
            self.reset()
            return True

        return False

    def record_success(self) -> None:
        self.reset()

    def record_failure(self) -> None:
        self.failure_count += 1
        if self.failure_count >= self.failure_threshold:
            self.opened_at = utcnow()

    def reset(self) -> None:
        self.failure_count = 0
        self.opened_at = None
