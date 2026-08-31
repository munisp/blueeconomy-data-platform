"""Fail-closed AIS NMEA 0183 (AIVDM/AIVDO) decoding for bronze vessel ingestion.

Raw AIS payloads arrive as ITU-R M.1371 position reports encapsulated in
NMEA 0183 ``!AIVDM``/``!AIVDO`` sentences. This module decodes them with the
pinned ``pyais`` library into a validated position report for the bronze
``vessel_observations`` path. Producers that already emit decoded aisstream
JSON bypass this module entirely — it is only invoked when an envelope
payload carries raw ``nmeaSentences``.

Every deviation (malformed sentence, bad checksum, unsupported message type,
unavailable position) fails closed with ``ValueError``; nothing is guessed.
"""

from __future__ import annotations

import math
import re
import time
from collections.abc import Sequence
from dataclasses import dataclass

from pyais import decode as _pyais_decode
from pyais.exceptions import InvalidNMEAMessageException, UnknownMessageException

from blueeconomy_data_platform.telemetry import get_meter as _get_meter

MAX_NMEA_SENTENCES = 9
MAX_NMEA_SENTENCE_BYTES = 1024
MMSI_PATTERN = re.compile(r"^[0-9]{9}$")
NMEA_SENTENCE_PATTERN = re.compile(r"^!AI(VDM|VDO),[0-9]+,[0-9]+,[0-9A-Za-z]?,[AB],")

# AIS position-report message types (ITU-R M.1371): class A (1-3) and
# class B (18 standard, 19 extended) position reports.
POSITION_MESSAGE_TYPES = frozenset({1, 2, 3, 18, 19})

# Protocol markers for "not available" field values.
AIS_LATITUDE_UNAVAILABLE = 91.0
AIS_LONGITUDE_UNAVAILABLE = 181.0
AIS_SPEED_UNAVAILABLE = 102.3
AIS_COURSE_UNAVAILABLE = 360.0
AIS_HEADING_UNAVAILABLE = 511


@dataclass(frozen=True)
class AisPositionReport:
    """One validated AIS position report decoded from raw NMEA sentences."""

    mmsi: str
    latitude: float
    longitude: float
    speed_knots: float | None
    heading_degrees: float | None
    course_degrees: float | None
    message_type: int


def normalize_mmsi(value: object) -> str:
    """Validate and normalize an MMSI to its canonical 9-digit text form."""
    if isinstance(value, bool):
        raise ValueError("mmsi must be a 9-digit identifier, not a boolean")
    if isinstance(value, int):
        text = str(value)
    elif isinstance(value, str):
        text = value
    else:
        raise ValueError("mmsi must be a 9-digit identifier")
    if not MMSI_PATTERN.fullmatch(text):
        raise ValueError("mmsi must be exactly 9 decimal digits")
    return text


def nmea_checksum_valid(sentence: str) -> bool:
    """Verify the NMEA 0183 XOR checksum (``*hh`` trailer) of one sentence."""
    body, delimiter, trailer = sentence.rpartition("*")
    if delimiter != "*" or len(trailer) != 2:
        return False
    try:
        expected = int(trailer, 16)
    except ValueError:
        return False
    checksum = 0
    for character in body[1:]:
        checksum ^= ord(character)
    return checksum == expected


def validate_aivdm_sentences(sentences: Sequence[str]) -> list[str]:
    """Validate the raw NMEA sentence envelope before decoding."""
    if not isinstance(sentences, Sequence) or isinstance(sentences, (str, bytes)):
        raise ValueError("nmeaSentences must be an array of NMEA 0183 sentences")
    if not 1 <= len(sentences) <= MAX_NMEA_SENTENCES:
        raise ValueError(f"nmeaSentences must contain 1 to {MAX_NMEA_SENTENCES} sentences")
    validated: list[str] = []
    for sentence in sentences:
        if not isinstance(sentence, str):
            raise ValueError("each NMEA sentence must be text")
        encoded = sentence.encode("ascii", errors="strict") if sentence.isascii() else None
        if encoded is None or not 1 <= len(encoded) <= MAX_NMEA_SENTENCE_BYTES:
            raise ValueError(
                f"NMEA sentences must be ASCII of at most {MAX_NMEA_SENTENCE_BYTES} bytes"
            )
        if not NMEA_SENTENCE_PATTERN.match(sentence):
            raise ValueError("only !AIVDM/!AIVDO AIS sentences are accepted")
        if "*" not in sentence:
            raise ValueError("NMEA sentence is missing its checksum delimiter")
        if not nmea_checksum_valid(sentence):
            raise ValueError("NMEA sentence checksum mismatch")
        validated.append(sentence)
    return validated


def _optional_speed(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("AIS speed over ground must be numeric")
    if float(value) >= AIS_SPEED_UNAVAILABLE:
        return None
    speed = float(value)
    if not math.isfinite(speed) or speed < 0:
        raise ValueError("AIS speed over ground must be a finite non-negative value")
    return speed


def _optional_degrees(value: object, unavailable: float, label: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"AIS {label} must be numeric")
    if float(value) >= unavailable:
        return None
    degrees = float(value)
    if not math.isfinite(degrees) or not 0.0 <= degrees < 360.0:
        raise ValueError(f"AIS {label} must be finite and within [0, 360)")
    return degrees


# Phase-7 OTel pyais decode metrics (no-op counters/histogram when
# OTEL_EXPORTER_OTLP_ENDPOINT is unset). Low-cardinality only: no MMSI, no
# message payloads. Rows/s is derived collector-side as
# rate(ais_decode_rows_total); parse errors are ais_decode_errors_total.
_meter = _get_meter("blueeconomy_data_platform.ais")
_ais_rows_total = _meter.create_counter(
    "ais_decode_rows_total",
    description="AIS position reports successfully decoded by pyais",
)
_ais_errors_total = _meter.create_counter(
    "ais_decode_errors_total",
    description="AIS decode failures (fail-closed ValueError), by stage",
)
_ais_decode_seconds = _meter.create_histogram(
    "ais_decode_duration_seconds",
    unit="s",
    description="pyais decode latency per NMEA sentence group",
)


def decode_aivdm(sentences: Sequence[str]) -> AisPositionReport:
    """Decode raw AIVDM/AIVDO sentences into a validated position report.

    Decoding uses the pinned ``pyais`` library, which verifies the NMEA
    checksum and the ITU-R M.1371 payload. Anything that is not a complete,
    checksum-valid class A or class B position report with an available
    position fails closed.
    """
    started = time.perf_counter()
    try:
        report = _decode_aivdm(sentences)
    except ValueError as error:
        stage = "nmea_decode" if "could not be decoded" in str(error) else "validation"
        _ais_errors_total.add(1, {"error_stage": stage})
        raise
    finally:
        _ais_decode_seconds.record(time.perf_counter() - started)
    _ais_rows_total.add(1)
    return report


def _decode_aivdm(sentences: Sequence[str]) -> AisPositionReport:
    validated = validate_aivdm_sentences(sentences)
    try:
        message = _pyais_decode(*validated)
    except (InvalidNMEAMessageException, UnknownMessageException, ValueError) as error:
        raise ValueError(f"AIS NMEA payload could not be decoded: {error}") from error
    fields = message.asdict()
    message_type = fields.get("msg_type")
    if message_type not in POSITION_MESSAGE_TYPES:
        raise ValueError(
            f"AIS message type {message_type!r} is not a class A/B position report "
            f"({sorted(POSITION_MESSAGE_TYPES)})"
        )
    mmsi = normalize_mmsi(fields.get("mmsi"))
    latitude = fields.get("lat")
    longitude = fields.get("lon")
    if (
        latitude is None
        or longitude is None
        or float(latitude) == AIS_LATITUDE_UNAVAILABLE
        or float(longitude) == AIS_LONGITUDE_UNAVAILABLE
    ):
        raise ValueError("AIS position report carries no valid latitude/longitude")
    lat = float(latitude)
    lon = float(longitude)
    if not math.isfinite(lat) or not -90.0 <= lat <= 90.0:
        raise ValueError("AIS latitude must be finite and within [-90, 90]")
    if not math.isfinite(lon) or not -180.0 <= lon <= 180.0:
        raise ValueError("AIS longitude must be finite and within [-180, 180]")
    return AisPositionReport(
        mmsi=mmsi,
        latitude=lat,
        longitude=lon,
        speed_knots=_optional_speed(fields.get("speed")),
        heading_degrees=_optional_degrees(
            fields.get("heading"), float(AIS_HEADING_UNAVAILABLE), "heading"
        ),
        course_degrees=_optional_degrees(
            fields.get("course"), AIS_COURSE_UNAVAILABLE, "course over ground"
        ),
        message_type=int(message_type),
    )


__all__ = [
    "AIS_HEADING_UNAVAILABLE",
    "POSITION_MESSAGE_TYPES",
    "AisPositionReport",
    "decode_aivdm",
    "normalize_mmsi",
    "validate_aivdm_sentences",
]
