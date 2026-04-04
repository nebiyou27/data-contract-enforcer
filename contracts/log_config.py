"""
contracts/log_config.py -- Structured JSON logging + OpenTelemetry tracing.

Logging
-------
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

OpenTelemetry tracing
---------------------
Call ``configure_telemetry()`` once at startup (runner.py does this).
Then obtain a tracer with ``get_tracer(__name__)`` and use it normally::

    tracer = get_tracer(__name__)
    with tracer.start_as_current_span("my.operation") as span:
        span.set_attribute("key", "value")
        ...

Configuration is driven by standard OTEL env vars:

    OTEL_SERVICE_NAME              (default: "data-contract-enforcer")
    OTEL_EXPORTER_OTLP_ENDPOINT    (e.g. "http://localhost:4318")
    OTEL_EXPORTER_OTLP_HEADERS     (e.g. "Authorization=Bearer <token>")

If no OTLP endpoint is set, a no-op tracer is used so the runner works
without any tracing backend.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# OpenTelemetry — optional; falls back to no-op if sdk is not installed
# ---------------------------------------------------------------------------

try:
    from opentelemetry import trace as _otel_trace
    from opentelemetry.sdk.resources import Resource, SERVICE_NAME
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    _OTEL_AVAILABLE = True
except ImportError:
    _OTEL_AVAILABLE = False

_telemetry_configured = False


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


# ---------------------------------------------------------------------------
# OpenTelemetry helpers
# ---------------------------------------------------------------------------


def configure_telemetry(service_name: str | None = None) -> None:
    """Initialise the global TracerProvider.

    Call once per process (runner.py does this after configure_logging).
    Safe to call multiple times — subsequent calls are no-ops.

    If ``opentelemetry-sdk`` is not installed, or if
    ``OTEL_EXPORTER_OTLP_ENDPOINT`` is not set, a no-op tracer is used so
    the process runs normally without any tracing backend.

    Args:
        service_name: Overrides ``OTEL_SERVICE_NAME`` env var.
                      Defaults to ``"data-contract-enforcer"``.
    """
    global _telemetry_configured
    if _telemetry_configured:
        return

    _telemetry_configured = True

    if not _OTEL_AVAILABLE:
        logging.getLogger(__name__).debug(
            "opentelemetry-sdk not installed — tracing disabled"
        )
        return

    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT", "")
    svc = service_name or os.environ.get("OTEL_SERVICE_NAME", "data-contract-enforcer")

    resource = Resource(attributes={SERVICE_NAME: svc})
    provider = TracerProvider(resource=resource)

    if endpoint:
        exporter = OTLPSpanExporter(endpoint=endpoint)
        provider.add_span_processor(BatchSpanProcessor(exporter))
        logging.getLogger(__name__).info(
            "OpenTelemetry tracing enabled: endpoint=%s service=%s", endpoint, svc
        )
    else:
        logging.getLogger(__name__).debug(
            "OTEL_EXPORTER_OTLP_ENDPOINT not set — spans collected but not exported"
        )

    _otel_trace.set_tracer_provider(provider)


def get_tracer(name: str = "contracts") -> "Any":
    """Return an OpenTelemetry Tracer for *name*.

    Returns a no-op tracer when the sdk is unavailable or telemetry has not
    been configured, so callers never need to guard against None.
    """
    if _OTEL_AVAILABLE:
        return _otel_trace.get_tracer(name)
    # Return a minimal no-op shim so callers don't need try/except guards
    return _NoOpTracer()


class _NoOpSpan:
    """Minimal no-op span for when OpenTelemetry is unavailable."""
    def set_attribute(self, key: str, value: object) -> None: ...
    def set_status(self, *args: object, **kwargs: object) -> None: ...
    def record_exception(self, exc: Exception) -> None: ...
    def __enter__(self) -> "_NoOpSpan": return self
    def __exit__(self, *args: object) -> None: ...


class _NoOpTracer:
    """Minimal no-op tracer for when OpenTelemetry is unavailable."""
    def start_as_current_span(self, name: str, **kwargs: object) -> "_NoOpSpan":
        return _NoOpSpan()


# Type alias used in the get_tracer return annotation above
from typing import Any  # noqa: E402 — kept at module bottom to avoid circular issues
