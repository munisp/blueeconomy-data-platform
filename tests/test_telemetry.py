"""Phase-7 OTel tests: disabled-mode, Kafka header carrier round-trip, tenant baggage."""

from __future__ import annotations

import pytest
from opentelemetry import baggage, context, trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from blueeconomy_data_platform import telemetry


@pytest.fixture()
def memory_exporter():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return exporter, provider


class _FakeMessage:
    def __init__(self, headers):
        self._headers = headers

    def headers(self):
        return self._headers


def test_disabled_by_default_without_endpoint(monkeypatch):
    monkeypatch.delenv(telemetry.ENDPOINT_ENV, raising=False)
    assert telemetry.telemetry_enabled() is False
    assert telemetry.init_telemetry(service_name="t", version="0") is False
    with telemetry.get_tracer().start_as_current_span("noop") as span:
        assert span.is_recording() is False


def test_disabled_mode_ais_decode_unaffected(monkeypatch):
    """Telemetry-off: pyais decode path works and metrics are no-ops."""
    monkeypatch.delenv(telemetry.ENDPOINT_ENV, raising=False)
    from pyais.encode import encode_dict

    from blueeconomy_data_platform.ais_decode import decode_aivdm

    sentences = list(
        encode_dict(
            {
                "msg_type": 1,
                "repeat": 0,
                "mmsi": "366123456",
                "status": 0,
                "turn": 0,
                "speed": 12.3,
                "accuracy": 0,
                "lon": -70.5,
                "lat": 42.1,
                "course": 90.0,
                "heading": 90,
                "second": 10,
                "maneuver": 0,
                "raim": False,
                "radio": 0,
            },
            radio_channel="A",
            talker_id="AI",
            sentence_type="VDM",
        )
    )
    report = decode_aivdm(sentences)
    assert report.mmsi == "366123456"
    with pytest.raises(ValueError):
        decode_aivdm(["!AIVDM,1,1,,A,garbage,0*00"])


def test_kafka_header_carrier_round_trip(memory_exporter):
    """W3C tracecontext+baggage round-trips through confluent-kafka headers."""
    exporter, provider = memory_exporter
    tracer = provider.get_tracer("test")
    with tracer.start_as_current_span("producer") as span:
        ctx = baggage.set_baggage("tenant.id", "tenant-3")
        ctx = baggage.set_baggage("agency", "NIMASA", context=ctx)
        token = context.attach(ctx)
        try:
            carrier = telemetry.inject_context({})
        finally:
            context.detach(token)
        expected_trace_id = span.get_span_context().trace_id

    headers = telemetry.carrier_to_kafka_headers(carrier)
    assert ("traceparent", carrier["traceparent"].encode()) in headers
    # Consumer side: headers back to carrier, parent context + baggage attrs.
    restored = telemetry.kafka_headers_to_carrier(headers)
    extracted = telemetry.extract_context(restored)
    assert (
        trace.get_current_span(extracted).get_span_context().trace_id
        == expected_trace_id
    )
    assert telemetry.baggage_span_attributes(extracted) == {
        "tenant.id": "tenant-3",
        "agency": "NIMASA",
    }


def test_extract_message_links_and_tenant_attributes(memory_exporter):
    """Consumed-record traceparents become the DAG span parent + links, and
    tenant.id/agency baggage lands on the pipeline span as attributes."""
    exporter, provider = memory_exporter
    tracer = provider.get_tracer("test")

    carriers = []
    for name, tenant in (("upstream-a", "tenant-3"), ("upstream-b", "tenant-3")):
        with tracer.start_as_current_span(name):
            ctx = baggage.set_baggage("tenant.id", tenant)
            ctx = baggage.set_baggage("agency", "NIMASA", context=ctx)
            token = context.attach(ctx)
            try:
                carriers.append(telemetry.inject_context({}))
            finally:
                context.detach(token)

    messages = [
        _FakeMessage(telemetry.carrier_to_kafka_headers(c)) for c in carriers
    ] + [_FakeMessage(None)]
    parent, links = telemetry.extract_message_links(messages)
    assert parent is not None
    assert len(links) == 1  # second carrier + headerless message skipped

    with tracer.start_as_current_span(
        "lakehouse.pipeline.kafka_ingest",
        context=parent,
        links=links,
        attributes=telemetry.baggage_span_attributes(parent),
    ):
        pass
    finished = {s.name: s for s in exporter.get_finished_spans()}
    dag = finished["lakehouse.pipeline.kafka_ingest"]
    assert dag.attributes["tenant.id"] == "tenant-3"
    assert dag.attributes["agency"] == "NIMASA"
    assert dag.parent.trace_id == finished["upstream-a"].context.trace_id
    assert len(dag.links) == 1
    assert dag.links[0].context.trace_id == finished["upstream-b"].context.trace_id


def test_baggage_span_attributes_empty_without_baggage():
    assert telemetry.baggage_span_attributes(None) == {}
    assert telemetry.baggage_span_attributes(context.Context()) == {}


def test_medallion_spans_noop_when_disabled(tmp_path, monkeypatch):
    """Telemetry-off: bronze append (Delta write span) behaves unchanged."""
    monkeypatch.delenv(telemetry.ENDPOINT_ENV, raising=False)
    from test_ingest import internal_event

    from blueeconomy_data_platform.ingest import append_events

    version, written, already_present = append_events(
        str(tmp_path / "bronze"), [internal_event()]
    )
    assert (version, written, already_present) == (0, 1, 0)


def test_drop_counting_exporter_never_raises():
    class FailingExporter:
        def export(self, spans):
            raise ConnectionError("collector down")

        def shutdown(self):
            pass

        def force_flush(self, timeout_millis=5000):
            return False

    counts = []

    class Counter:
        def add(self, n):
            counts.append(n)

    wrapped = telemetry._DropCountingSpanExporter(FailingExporter(), Counter())
    from opentelemetry.sdk.trace.export import SpanExportResult

    assert wrapped.export([object()]) is SpanExportResult.FAILURE
    assert counts == [1]
