"""Fail-closed envelope provenance signature verification (fleet scheme).

Covers the RFC 8785 canonicalizer, the mounted key-directory loader and the
reject-and-count behaviour of the ingest-path verifier. The canonicalization
vector is cross-checked byte-for-byte against the TypeScript fleet
implementation (blueeconomy-credential-verification src/vc/jcs.ts).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from blueeconomy_data_platform.jcs import canonicalize
from blueeconomy_data_platform.signature_verification import (
    REASON_INVALID_SIGNATURE,
    REASON_MALFORMED_JWS,
    REASON_UNKNOWN_KID,
    REASON_UNSUPPORTED_ALG,
    EnvelopeSignatureVerifier,
    KeyDirectory,
    SignatureVerificationError,
    SignatureVerificationMetrics,
    export_public_key_for_test,
    load_key_directory,
    load_key_directory_from_env,
    private_key_from_raw_for_test,
    sign_envelope_for_test,
)
from signing_helpers import (
    DEFAULT_TEST_KID,
    fixture_private_key,
    sign_envelope,
    signed_envelope_bytes,
    single_use_verifier,
)

CANONICALIZATION_INPUT = {
    "z": [1, 0.1, 1e21, 1e-7, 0.000001, 0, 3.141592653589793, 123456789012345680000],
    "é": "unicode",
    "a": {"b": None, "a": [True, False]},
    "😀": "astral",
    "": "private-use",
    "num": [1e20, 1e-6, 5e-7, 0.3, 100, 2.5e-8, 9007199254740991],
}

# Produced by blueeconomy-credential-verification src/vc/jcs.ts canonicalizeJson.
CANONICALIZATION_EXPECTED = (
    '{"":"private-use","a":{"a":[true,false],"b":null},'
    '"num":[100000000000000000000,0.000001,5e-7,0.3,100,2.5e-8,9007199254740991],'
    '"z":[1,0.1,1e+21,1e-7,0.000001,0,3.141592653589793,123456789012345680000],'
    '"é":"unicode","😀":"astral"}'
)


def envelope() -> dict[str, object]:
    return {
        "envelopeVersion": "1.0",
        "eventId": "2b3c4d5e-6f70-4819-8a2b-3c4d5e6f7081",
        "eventType": "safety.telemetry.validated",
        "occurredAt": "2026-08-12T12:00:00Z",
        "producer": "blueeconomy-waterway-safety",
        "correlationId": "correlation-kafka-001",
        "fhir": {
            "resourceType": "Bundle",
            "type": "message",
            "entry": [{"resource": {"payload_sha256": "a" * 64, "sequence": 7}}],
        },
        "provenance": {
            "principalId": "svc-waterway-safety",
            "principalRole": "telemetry-gateway",
            "signature": "placeholder",
            "ledgerCommitHash": "c" * 64,
        },
        "classification": "INTERNAL",
    }


def es_numeric_view(document: dict[str, object]) -> dict[str, object]:
    """Reparse under the ECMAScript numeric model used by the verifier."""
    view: dict[str, object] = json.loads(json.dumps(document), parse_int=float, parse_float=float)
    return view


class TestRfc8785Canonicalization:
    def test_matches_typescript_fleet_implementation(self) -> None:
        view = json.loads(json.dumps(CANONICALIZATION_INPUT), parse_int=float, parse_float=float)
        assert canonicalize(view) == CANONICALIZATION_EXPECTED

    def test_string_escaping_is_minimal(self) -> None:
        assert (
            canonicalize('quote" backslash\\ slash/\x00\x1f')
            == '"quote\\" backslash\\\\ slash/\\u0000\\u001f"'
        )
        assert canonicalize("tab\tnewline\n") == '"tab\\tnewline\\n"'
        assert canonicalize("café") == '"café"'

    def test_key_order_uses_utf16_code_units(self) -> None:
        # Astral characters sort by their leading UTF-16 surrogate (0xD800-0xDBFF),
        # i.e. after BMP "z" (0x7A) but before U+FFFF; code-point order would put
        # U+FFFF first. Explicit escapes keep the vector unambiguous.
        assert (
            canonicalize({"\U0001f600": 1, "\uffff": 2, "z": 3})
            == '{"z":3,"\U0001f600":1,"\uffff":2}'
        )

    def test_rejects_non_finite_numbers(self) -> None:
        with pytest.raises(ValueError, match="non-finite"):
            canonicalize(float("nan"))
        with pytest.raises(ValueError, match="non-finite"):
            canonicalize(float("inf"))

    def test_rejects_out_of_range_integers(self) -> None:
        with pytest.raises(ValueError, match="IEEE-754"):
            canonicalize(10**21)


class TestKeyDirectoryLoading:
    def write_directory(self, path: Path, entries: dict[str, str]) -> Path:
        path.write_text(json.dumps(entries), encoding="utf-8")
        return path

    def test_loads_valid_directory(self, tmp_path: Path) -> None:
        key = fixture_private_key(DEFAULT_TEST_KID).public_key()
        directory = load_key_directory(
            self.write_directory(
                tmp_path / "keys.json", {DEFAULT_TEST_KID: export_public_key_for_test(key)}
            )
        )
        assert directory.resolve(DEFAULT_TEST_KID) is not None

    def test_fails_closed_on_missing_file(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="regular non-symlink"):
            load_key_directory(tmp_path / "absent.json")

    def test_fails_closed_on_symlink(self, tmp_path: Path) -> None:
        target = self.write_directory(
            tmp_path / "real.json",
            {"k-0": export_public_key_for_test(fixture_private_key("k-0").public_key())},
        )
        link = tmp_path / "link.json"
        link.symlink_to(target)
        with pytest.raises(ValueError, match="non-symlink"):
            load_key_directory(link)

    def test_fails_closed_on_invalid_json(self, tmp_path: Path) -> None:
        path = tmp_path / "keys.json"
        path.write_text("{not json", encoding="utf-8")
        with pytest.raises(ValueError, match="not valid JSON"):
            load_key_directory(path)

    def test_fails_closed_on_empty_object(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="non-empty"):
            load_key_directory(self.write_directory(tmp_path / "keys.json", {}))

    def test_fails_closed_on_malformed_key(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="32-byte"):
            load_key_directory(self.write_directory(tmp_path / "keys.json", {"k-0": "AAAA"}))
        with pytest.raises(ValueError, match="malformed kid"):
            load_key_directory(self.write_directory(tmp_path / "keys.json", {"bad kid!": "AAAA"}))
        with pytest.raises(ValueError, match="unpadded base64url"):
            load_key_directory(
                self.write_directory(
                    tmp_path / "keys.json",
                    {
                        "k-0": export_public_key_for_test(fixture_private_key("k-0").public_key())
                        + "="
                    },
                )
            )

    def test_env_loader_fails_closed_without_variable(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("KEY_DIRECTORY_PATH", raising=False)
        with pytest.raises(ValueError, match="KEY_DIRECTORY_PATH is required"):
            load_key_directory_from_env()

    def test_env_loader_reads_configured_path(self, tmp_path: Path) -> None:
        key = fixture_private_key(DEFAULT_TEST_KID).public_key()
        path = self.write_directory(
            tmp_path / "keys.json", {DEFAULT_TEST_KID: export_public_key_for_test(key)}
        )
        directory = load_key_directory_from_env({"KEY_DIRECTORY_PATH": str(path)})
        assert directory.resolve(DEFAULT_TEST_KID) is not None


class TestEnvelopeSignatureVerifier:
    def test_valid_signature_verifies_and_returns_kid(self) -> None:
        signed = sign_envelope(envelope(), DEFAULT_TEST_KID)
        verifier = single_use_verifier(DEFAULT_TEST_KID)
        assert verifier.verify(es_numeric_view(signed)) == DEFAULT_TEST_KID
        assert verifier.metrics.verified == 1
        assert verifier.metrics.rejected == {}

    def test_unknown_kid_is_rejected_and_counted(self) -> None:
        signed = sign_envelope(envelope(), DEFAULT_TEST_KID)
        metrics = SignatureVerificationMetrics()
        verifier = EnvelopeSignatureVerifier(KeyDirectory({}), metrics)
        with pytest.raises(SignatureVerificationError, match="unknown-kid"):
            verifier.verify(es_numeric_view(signed))
        assert metrics.rejected == {REASON_UNKNOWN_KID: 1}

    def test_tampered_signature_is_rejected(self) -> None:
        signed = sign_envelope(envelope(), DEFAULT_TEST_KID)
        provenance = signed["provenance"]
        assert isinstance(provenance, dict)
        header, payload, signature = str(provenance["signature"]).split(".")
        forged_signature = ("A" if signature[0] != "A" else "B") + signature[1:]
        provenance["signature"] = f"{header}.{payload}.{forged_signature}"
        verifier = single_use_verifier(DEFAULT_TEST_KID)
        with pytest.raises(SignatureVerificationError, match="invalid-signature"):
            verifier.verify(es_numeric_view(signed))
        assert verifier.metrics.rejected == {REASON_INVALID_SIGNATURE: 1}

    def test_post_signing_envelope_mutation_is_rejected(self) -> None:
        signed = sign_envelope(envelope(), DEFAULT_TEST_KID)
        signed["correlationId"] = "correlation-attacker-override"
        verifier = single_use_verifier(DEFAULT_TEST_KID)
        with pytest.raises(SignatureVerificationError, match="payload-mismatch"):
            verifier.verify(es_numeric_view(signed))

    def test_foreign_payload_substitution_is_rejected(self) -> None:
        signed = sign_envelope(envelope(), DEFAULT_TEST_KID)
        other = envelope()
        other["eventId"] = "00000000-0000-4000-8000-000000000042"
        signed_other = sign_envelope(other, DEFAULT_TEST_KID)
        provenance = signed["provenance"]
        other_provenance = signed_other["provenance"]
        assert isinstance(provenance, dict) and isinstance(other_provenance, dict)
        header = str(provenance["signature"]).split(".")[0]
        attacker_segments = str(other_provenance["signature"]).split(".")
        provenance["signature"] = f"{header}.{attacker_segments[1]}.{attacker_segments[2]}"
        verifier = single_use_verifier(DEFAULT_TEST_KID)
        with pytest.raises(SignatureVerificationError, match="payload-mismatch"):
            verifier.verify(es_numeric_view(signed))

    def test_unsupported_algorithm_is_rejected(self) -> None:
        signed = sign_envelope(envelope(), DEFAULT_TEST_KID)
        provenance = signed["provenance"]
        assert isinstance(provenance, dict)
        header, payload, _ = str(provenance["signature"]).split(".")
        import base64

        forged_header = (
            base64.urlsafe_b64encode(b'{"alg":"none","kid":"x-0"}').rstrip(b"=").decode()
        )
        provenance["signature"] = f"{forged_header}.{payload}.AAAA"
        verifier = single_use_verifier(DEFAULT_TEST_KID)
        with pytest.raises(SignatureVerificationError, match="unsupported-alg"):
            verifier.verify(es_numeric_view(signed))
        assert header != forged_header

    @pytest.mark.parametrize(
        "signature",
        [
            "not-a-jws",
            "two.segments",
            "four.segments.are.bad.here",
            "aa==.bb.cc",
            "e30.e30",  # two segments with valid base64url
        ],
    )
    def test_malformed_compact_serializations_are_rejected(self, signature: str) -> None:
        document = envelope()
        provenance = document["provenance"]
        assert isinstance(provenance, dict)
        provenance["signature"] = signature
        verifier = single_use_verifier(DEFAULT_TEST_KID)
        with pytest.raises(SignatureVerificationError) as excinfo:
            verifier.verify(es_numeric_view(document))
        assert excinfo.value.reason in {REASON_MALFORMED_JWS, REASON_UNSUPPORTED_ALG}

    def test_missing_provenance_or_signature_is_rejected(self) -> None:
        verifier = single_use_verifier(DEFAULT_TEST_KID)
        document = envelope()
        del document["provenance"]
        with pytest.raises(SignatureVerificationError, match="malformed-jws"):
            verifier.verify(es_numeric_view(document))
        document = envelope()
        provenance = document["provenance"]
        assert isinstance(provenance, dict)
        provenance["signature"] = 42
        with pytest.raises(SignatureVerificationError, match="malformed-jws"):
            verifier.verify(es_numeric_view(document))

    def test_wrong_key_fails_even_with_known_kid(self) -> None:
        attacker_key = Ed25519PrivateKey.generate()
        signed = envelope()
        provenance = signed["provenance"]
        assert isinstance(provenance, dict)
        provenance["signature"] = sign_envelope_for_test(
            es_numeric_view(signed), attacker_key, DEFAULT_TEST_KID
        )
        verifier = single_use_verifier(DEFAULT_TEST_KID)
        with pytest.raises(SignatureVerificationError, match="invalid-signature"):
            verifier.verify(es_numeric_view(signed))

    def test_signer_helper_rejects_envelope_without_provenance(self) -> None:
        with pytest.raises(ValueError, match="provenance"):
            sign_envelope_for_test({}, private_key_from_raw_for_test(b"\x01" * 32), "k-0")


class TestDecodeEventIntegration:
    def test_decode_event_rejects_forged_envelope_before_normalization(self) -> None:
        from blueeconomy_data_platform.ingest import load_schema
        from blueeconomy_data_platform.kafka_ingest import decode_event

        schema = Path(__file__).resolve().parents[1] / "schemas" / "event-envelope.schema.json"
        validator = load_schema(schema)
        forged = envelope()
        provenance = forged["provenance"]
        assert isinstance(provenance, dict)
        provenance["signature"] = sign_envelope_for_test(
            es_numeric_view(forged), Ed25519PrivateKey.generate(), DEFAULT_TEST_KID
        )
        verifier = single_use_verifier(DEFAULT_TEST_KID)
        with pytest.raises(SignatureVerificationError, match="invalid-signature"):
            decode_event(json.dumps(forged).encode("utf-8"), validator, verifier)
        assert verifier.metrics.rejected == {REASON_INVALID_SIGNATURE: 1}

    def test_decode_event_accepts_signed_envelope(self) -> None:
        from blueeconomy_data_platform.ingest import load_schema
        from blueeconomy_data_platform.kafka_ingest import decode_event

        schema = Path(__file__).resolve().parents[1] / "schemas" / "event-envelope.schema.json"
        validator = load_schema(schema)
        verifier = single_use_verifier(DEFAULT_TEST_KID)
        normalized = decode_event(
            signed_envelope_bytes(envelope(), DEFAULT_TEST_KID), validator, verifier
        )
        assert normalized["event_id"] == "2b3c4d5e-6f70-4819-8a2b-3c4d5e6f7081"
        assert verifier.metrics.verified == 1
