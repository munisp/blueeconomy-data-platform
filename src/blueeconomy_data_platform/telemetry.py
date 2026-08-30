"""OpenTelemetry wiring for the data platform lakehouse (Phase-7 OTel wave).

Contract (OTEL_DESIGN.md §1/§3 lakehouse row):

- ``OTEL_EXPORTER_OTLP_ENDPOINT`` unset => telemetry is DISABLED; every
  entry point is a no-op that never breaks a pipeline run. This is the
  platform's one sanctioned fail-open.
- When set: OTLP gRPC span and metric exporters behind batch/async
  processors (non-blocking). A down collector means spans are dropped and
  counted on ``telemetry_dropped_total`` — never an ingestion failure.
- Graceful shutdown flushes with a hard 5s bound.
- Propagation is W3C tracecontext + baggage, carried in Kafka record
  headers; ``tenant.id``/``agency`` baggage from consumed records is copied
  onto the DAG-level pipeline span as attributes. Metrics stay
  low-cardinality.

Coverage notes (honesty):
- The lakehouse storage client is ``deltalake``/``object_store`` (S3/MinIO
  via ``s3://`` URIs); there is no boto3/httpx S3 client, so the
  ``lakehouse.*.append`` spans around Delta writes ARE the S3/MinIO client
  spans (``storage.uri_scheme`` distinguishes s3/file).
- DuckDB and Polars are NOT used anywhere in this repository (verified by
  survey); there are no DuckDB/Polars spans to add — not fabricated.
- Apache Sedona IS used, Sedona-on-Spark batch-only, in
  ``jobs/vessel_trajectory_silver.py``. Driver-side spans are emitted by
  that job; executor/JVM coverage requires the OTel Java agent on the
  Spark driver/executors plus the Spark Prometheus (JMX) sink — a
  deployment concern of the ``sedona-spark-jobs`` gitops chart, documented
  in the job module.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Iterable, MutableMapping
from typing import Any

from opentelemetry import baggage, context, metrics, propagate, trace
from opentelemetry.baggage.propagation import W3CBaggagePropagator
from opentelemetry.propagators.composite import CompositePropagator
from opentelemetry.propagators.textmap import Setter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace.export import SpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

log = logging.getLogger(__name__)

ENDPOINT_ENV = "OTEL_EXPORTER_OTLP_ENDPOINT"
TENANT_ATTRIBUTES = ("tenant.id", "agency")
SHUTDOWN_FLUSH_TIMEOUT_MILLIS = 5_000
DROPPED_METRIC = "telemetry_dropped_total"
MAX_MESSAGE_LINKS = 8

_propagator = CompositePropagator([TraceContextTextMapPropagator(), W3CBaggagePropagator()])


class _DictSetter(Setter[MutableMapping[str, str]]):
    def set(self, carrier: MutableMapping[str, str], key: str, value: str) -> None:
        carrier[key] = value


def telemetry_enabled(environ: dict[str, str] | None = None) -> bool:
    env = os.environ if environ is None else environ
    return bool(env.get(ENDPOINT_ENV, "").strip())


def get_tracer(name: str = "blueeconomy_data_platform") -> trace.Tracer:
    """A tracer from the global provider (no-op when telemetry is disabled)."""
    return trace.get_tracer(name)


def get_meter(name: str = "blueeconomy_data_platform") -> metrics.Meter:
    """A meter from the global provider (no-op when telemetry is disabled)."""
    return metrics.get_meter(name)


def inject_context(carrier: MutableMapping[str, str]) -> MutableMapping[str, str]:
    """Inject the current W3C tracecontext+baggage into a message carrier."""
    _propagator.inject(carrier, setter=_DictSetter())
    return carrier


def extract_context(carrier: MutableMapping[str, str]) -> context.Context:
    """Extract a W3C tracecontext+baggage context from a message carrier."""
    return _propagator.extract(carrier)


def kafka_headers_to_carrier(headers: Any) -> dict[str, str]:
    """confluent-kafka headers (list of (key, bytes|None)) -> text carrier."""
    carrier: dict[str, str] = {}
    if not headers:
        return carrier
    for key, value in headers:
        if value is None:
            continue
        carrier[str(key)] = value.decode("utf-8", errors="replace")
    return carrier


def carrier_to_kafka_headers(carrier: MutableMapping[str, str]) -> list[tuple[str, bytes]]:
    """Text carrier -> confluent-kafka produce headers."""
    return [(key, value.encode("utf-8")) for key, value in carrier.items()]


def extract_message_links(
    messages: Iterable[Any],
) -> tuple[context.Context | None, list[trace.Link]]:
    """Extract trace contexts from consumed Kafka messages.

    Returns the first message context found (parent for the DAG span) plus
    span links (bounded) to the remaining upstream trace contexts.
    """
    parent: context.Context | None = None
    links: list[trace.Link] = []
    seen: set[int] = set()
    for message in messages:
        try:
            headers = message.headers()
        except Exception:
            headers = None
        carrier = kafka_headers_to_carrier(headers)
        if "traceparent" not in carrier:
            continue
        ctx = extract_context(carrier)
        span_context = trace.get_current_span(ctx).get_span_context()
        if not span_context.is_valid or span_context.trace_id in seen:
            continue
        seen.add(span_context.trace_id)
        if parent is None:
            parent = ctx
        elif len(links) < MAX_MESSAGE_LINKS:
            links.append(trace.Link(span_context))
    return parent, links


def baggage_span_attributes(ctx: context.Context | None) -> dict[str, str]:
    """tenant.id/agency baggage entries from an extracted context."""
    if ctx is None:
        return {}
    attributes: dict[str, str] = {}
    for key in TENANT_ATTRIBUTES:
        value = baggage.get_baggage(key, context=ctx)
        if value:
            attributes[key] = str(value)
    return attributes


class _DropCountingSpanExporter(SpanExporter):
    """SpanExporter wrapper: collector-down = drop + count, never raise."""

    def __init__(self, inner: Any, dropped_counter: Any = None) -> None:
        self._inner = inner
        self._dropped = dropped_counter

    def export(self, spans: Any) -> Any:
        from opentelemetry.sdk.trace.export import SpanExportResult

        try:
            return self._inner.export(spans)
        except Exception as exc:  # collector down: drop-with-metric
            if self._dropped is not None:
                self._dropped.add(len(spans))
            log.warning("otel span export dropped %d span(s): %s", len(spans), exc)
            return SpanExportResult.FAILURE

    def shutdown(self) -> None:
        self._inner.shutdown()

    def force_flush(self, timeout_millis: int = SHUTDOWN_FLUSH_TIMEOUT_MILLIS) -> bool:
        return bool(self._inner.force_flush(timeout_millis))


def _resource(service_name: str, version: str) -> Resource:
    return Resource.create(
        {
            "service.name": os.environ.get("OTEL_SERVICE_NAME", service_name),
            "service.namespace": "blueeconomy",
            "service.version": version,
            "deployment.environment": os.environ.get("OTEL_ENVIRONMENT", "production"),
        }
    )


def init_telemetry(*, service_name: str, version: str) -> bool:
    """Configure providers. No-op when disabled; never raises (fail-open)."""
    if not telemetry_enabled():
        log.info("otel disabled (%s unset)", ENDPOINT_ENV)
        return False
    try:
        from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (
            OTLPMetricExporter,
        )
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
            OTLPSpanExporter,
        )
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        resource = _resource(service_name, version)

        meter_provider = MeterProvider(
            resource=resource,
            metric_readers=[
                PeriodicExportingMetricReader(OTLPMetricExporter(), export_interval_millis=30_000)
            ],
        )
        metrics.set_meter_provider(meter_provider)
        dropped = meter_provider.get_meter(service_name).create_counter(
            DROPPED_METRIC,
            description="telemetry items dropped because the collector was unavailable",
        )

        tracer_provider = TracerProvider(resource=resource)
        tracer_provider.add_span_processor(
            BatchSpanProcessor(
                _DropCountingSpanExporter(OTLPSpanExporter(), dropped),
                export_timeout_millis=SHUTDOWN_FLUSH_TIMEOUT_MILLIS,
            )
        )
        trace.set_tracer_provider(tracer_provider)
        propagate.set_global_textmap(_propagator)

        import atexit

        atexit.register(shutdown_telemetry)
        log.info("otel enabled -> %s", os.environ[ENDPOINT_ENV])
        return True
    except Exception as exc:  # fail-open: telemetry must never break a run
        log.warning("otel init failed; telemetry disabled: %s", exc)
        return False


def shutdown_telemetry() -> None:
    """Flush + shutdown providers, bounded at <=5s (graceful shutdown)."""
    tracer_provider = trace.get_tracer_provider()
    shutdown = getattr(tracer_provider, "shutdown", None)
    if callable(shutdown):
        try:
            shutdown()
        except Exception:
            log.warning("otel tracer shutdown failed", exc_info=True)
    meter_provider = metrics.get_meter_provider()
    shutdown = getattr(meter_provider, "shutdown", None)
    if callable(shutdown):
        try:
            shutdown()
        except Exception:
            log.warning("otel meter shutdown failed", exc_info=True)
