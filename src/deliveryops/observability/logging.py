"""Structured logging setup with correlation context binding via structlog."""

import logging
import sys
from typing import Any

import structlog


def configure_logging(log_level: str = "INFO", json_output: bool = True) -> None:
    """Configure structlog and standard library logging."""
    level = getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level, force=True)

    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    renderer: Any = (
        structlog.processors.JSONRenderer()
        if json_output
        else structlog.dev.ConsoleRenderer(colors=False)
    )

    structlog.configure(
        processors=[*shared_processors, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=False,
    )


def get_logger(name: str = "deliveryops") -> Any:
    """Return a structured logger bound with component name."""
    return structlog.get_logger().bind(component=name)


def bind_context(**kwargs: Any) -> None:
    """Bind correlation/trace variables into contextvars."""
    structlog.contextvars.bind_contextvars(**kwargs)


def clear_context() -> None:
    """Clear bound contextvars."""
    structlog.contextvars.clear_contextvars()
