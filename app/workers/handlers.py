from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

HandlerFunc = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


class HandlerRegistry:
    def __init__(self) -> None:
        self._handlers: dict[str, HandlerFunc] = {}

    def register(self, task_type: str, handler: HandlerFunc) -> None:
        self._handlers[task_type] = handler

    def get(self, task_type: str) -> HandlerFunc:
        return self._handlers.get(task_type, self._handlers["default"])


async def default_handler(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "echo": payload,
        "processed_at": datetime.now(timezone.utc).isoformat(),
    }


def build_default_registry() -> HandlerRegistry:
    registry = HandlerRegistry()
    registry.register("default", default_handler)
    return registry
