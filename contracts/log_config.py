"""
contracts/log_config.py -- Structured JSON logging for the data-contract-enforcer.

All modules obtain their logger with::

    import logging
    logger = logging.getLogger(__name__)

The root handler and formatter are configured once per process by calling
``configure_logging()`` from the entry-point (runner.py / generator.py /
cli tools).  When a run_id is supplied (e.g. the runner's report_id) it is
injected into every log record automatically via a shared Filter so that
every log line can be correlated back to the triggering run.

Output format (JSON, one object per line)::

    {"timestamp":"2024-01-01T00:00:00+00:00","level":"INFO",
     "logger":"contracts.runner","run_id":"<uuid>","message":"..."}
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone


class _JsonFormatter(logging.Formatter):
    """Emit one JSON object per log record."""

    def format(self, record: logging.LogRecord) -> str:
        log_obj: dict = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "run_id": getattr(record, "run_id", ""),
            "message": record.getMessage(),
        }
        if record.exc_info:
            log_obj["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(log_obj, ensure_ascii=False)


class _RunIdFilter(logging.Filter):
    """Injects the current run_id into every log record."""

    def __init__(self) -> None:
        super().__init__()
        self.run_id: str = ""

    def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[override]
        record.run_id = self.run_id  # type: ignore[attr-defined]
        return True


# Module-level filter instance — shared across all handlers so that updating
# ``_run_id_filter.run_id`` immediately affects every subsequent log record.
_run_id_filter = _RunIdFilter()

_configured = False


def configure_logging(run_id: str = "", level: int | None = None) -> None:
    """Configure the root logger with a JSON handler.

    Call once per process, typically at the top of ``main()``.

    Args:
        run_id: Correlation ID (e.g. runner's report_id) injected into every
                log record.  Pass an empty string when not applicable.
        level:  Override log level.  Defaults to the ``LOG_LEVEL`` env var
                (``INFO`` if unset).
    """
    global _configured

    _run_id_filter.run_id = run_id

    if _configured:
        # Just update the run_id if logging is already set up (e.g. in tests)
        return

    if level is None:
        env_level = os.environ.get("LOG_LEVEL", "INFO").upper()
        level = getattr(logging, env_level, logging.INFO)

    handler = logging.StreamHandler()
    handler.setFormatter(_JsonFormatter())
    handler.addFilter(_run_id_filter)

    root = logging.getLogger()
    root.setLevel(level)
    # Remove any pre-existing handlers (e.g. basicConfig added by imported libs)
    root.handlers.clear()
    root.addHandler(handler)

    _configured = True
