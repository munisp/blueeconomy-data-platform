from __future__ import annotations

import pytest
from pyais import encode_dict

from blueeconomy_data_platform.ais_decode import (
    AisPositionReport,
    decode_aivdm,
    normalize_mmsi,
    validate_aivdm_sentences,
)


def encode_position(
    mmsi: int = 366123456,
    lat: float = 42.1,
    lon: float = -70.5,
    speed: float = 12.3,
    course: float = 90.0,
    heading: int = 90,
    msg_type: int = 1,
) -> list[str]:
    data: dict[str, str | int | float | bytes | bool] = {
        "msg_type": msg_type,
        "repeat": 0,
        "mmsi": mmsi,
        "status": 0,
        "turn": 0,
        "speed": speed,
        "accuracy": 0,
        "lon": lon,
        "lat": lat,
        "course": course,
        "heading": heading,
        "second": 10,
        "maneuver": 0,
        "raim": False,
        "radio": 0,
    }
    return list(encode_dict(data, radio_channel="A", talker_id="AI", sentence_type="VDM"))


def test_decode_real_aivdm_position_report() -> None:
    sentences = encode_position()
    assert sentences[0].startswith("!AIVDM,")
    report = decode_aivdm(sentences)
    assert isinstance(report, AisPositionReport)
    assert report.mmsi == "366123456"
    assert report.latitude == pytest.approx(42.1, abs=1e-4)
    assert report.longitude == pytest.approx(-70.5, abs=1e-4)
    assert report.speed_knots == pytest.approx(12.3, abs=1e-4)
    assert report.heading_degrees == pytest.approx(90.0, abs=1e-4)
    assert report.course_degrees == pytest.approx(90.0, abs=1e-4)
    assert report.message_type == 1


def test_decode_class_b_position_report() -> None:
    data: dict[str, str | int | float | bytes | bool] = {
        "msg_type": 18,
        "repeat": 0,
        "mmsi": 244123456,
        "reserved": 0,
        "speed": 8.4,
        "accuracy": 0,
        "lon": 4.8952,
        "lat": 52.3702,
        "course": 181.2,
        "heading": 511,
        "second": 15,
        "regional": 0,
        "cs": 1,
        "display": 0,
        "dsc": 1,
        "band": 1,
        "msg22": 1,
        "assigned": 0,
        "raim": False,
        "radio": 0,
    }
    sentences = list(encode_dict(data, radio_channel="A", talker_id="AI"))
    report = decode_aivdm(sentences)
    assert report.mmsi == "244123456"
    assert report.message_type == 18
    assert report.heading_degrees is None  # 511 is the AIS "not available" marker
    assert report.latitude == pytest.approx(52.3702, abs=1e-4)


def test_unavailable_speed_and_course_decode_as_none() -> None:
    sentences = encode_position(speed=102.3, course=360.0)
    report = decode_aivdm(sentences)
    assert report.speed_knots is None
    assert report.course_degrees is None


def test_corrupted_checksum_fails_closed() -> None:
    sentences = encode_position()
    corrupted = sentences[0][:-2] + ("00" if not sentences[0].endswith("00") else "7F")
    with pytest.raises(ValueError, match="could not be decoded|checksum"):
        decode_aivdm([corrupted])


def test_non_position_message_type_fails_closed() -> None:
    data: dict[str, str | int | float | bytes | bool] = {
        "msg_type": 5,
        "repeat": 0,
        "mmsi": 366123456,
        "ais_version": 0,
        "imo": 9131211,
        "callsign": "WDE1234",
        "shipname": "TEST VESSEL",
        "shiptype": 70,
        "to_bow": 50,
        "to_stern": 100,
        "to_port": 10,
        "to_starboard": 10,
        "epfd": 1,
        "month": 8,
        "day": 12,
        "hour": 10,
        "minute": 30,
        "draught": 5.5,
        "destination": "LAGOS",
        "dte": 0,
    }
    sentences = list(encode_dict(data, radio_channel="A", talker_id="AI"))
    with pytest.raises(ValueError, match="not a class A/B position report"):
        decode_aivdm(sentences)


def test_unavailable_position_fails_closed() -> None:
    sentences = encode_position(lat=91.0, lon=181.0)
    with pytest.raises(ValueError, match="no valid latitude/longitude"):
        decode_aivdm(sentences)


def test_malformed_sentences_fail_closed() -> None:
    with pytest.raises(ValueError):
        decode_aivdm([])
    with pytest.raises(ValueError, match="!AIVDM"):
        decode_aivdm(["$GPGGA,123519,4807.038,N,01131.000,E,1,08,0.9,545.4,M,,,*47"])
    with pytest.raises(ValueError, match="checksum"):
        decode_aivdm(["!AIVDM,1,1,,A,15M:Ih001sJuAe0H5gp3Q2lD0000,0"])
    with pytest.raises(ValueError):
        decode_aivdm(["!AIVDM,1,1,,A,garbage,0*00"])
    with pytest.raises(ValueError):
        decode_aivdm("!AIVDM,1,1,,A,15M:Ih001sJuAe0H5gp3Q2lD0000,0*0D")


def test_validate_aivdm_sentences_bounds() -> None:
    with pytest.raises(ValueError, match="1 to"):
        validate_aivdm_sentences(["!AIVDM,1,1,,A,x,0*00"] * 10)
    with pytest.raises(ValueError, match="ASCII"):
        validate_aivdm_sentences(["!AIVDM,1,1,,A,x,0*00é"])


def test_normalize_mmsi() -> None:
    assert normalize_mmsi(366123456) == "366123456"
    assert normalize_mmsi("366123456") == "366123456"
    for bad in (True, "12345678", "1234567890", "abcdefghi", None, 12.5):
        with pytest.raises(ValueError):
            normalize_mmsi(bad)
