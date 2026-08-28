"""Fail-closed verification of fleet envelope provenance signatures.

Implements the consumer side of the scheme normatively specified in
blueeconomy-contracts ``docs/envelope-signature.md``:

- ``provenance.signature`` is a JWS compact serialization (EdDSA/Ed25519)
  over the JCS-canonicalized (RFC 8785) JSON of the full envelope excluding
  the signature field, with protected header ``{"alg":"EdDSA","kid":...}``;
- producer public keys are resolved from a mounted key directory shaped
  ``{kid: base64url-ed25519-pubkey}`` whose path comes from the
  ``KEY_DIRECTORY_PATH`` environment variable;
- the directory is loaded once at startup and any load failure is fatal
  (fail closed); unknown ``kid``, malformed compact serializations, payload
  mismatches and invalid signatures are rejected, logged with a reason code
  and counted, and rejected envelopes are never persisted.
"""

from __future__ import annotations

import base64
import binascii
import json
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    PublicFormat,
)

from blueeconomy_data_platform.jcs import canonicalize

LOGGER = logging.getLogger("blueeconomy_data_platform.signature_verification")

KID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,256}$")
MAX_KEY_DIRECTORY_BYTES = 1 * 1024 * 1024
ED25519_PUBLIC_KEY_BYTES = 32

REASON_MALFORMED_JWS = "malformed-jws"
REASON_UNSUPPORTED_ALG = "unsupported-alg"
REASON_UNKNOWN_KID = "unknown-kid"
REASON_PAYLOAD_MISMATCH = "payload-mismatch"
REASON_INVALID_SIGNATURE = "invalid-signature"


class SignatureVerificationError(ValueError):
    """Raised when an envelope provenance signature fails verification."""

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(f"envelope signature rejected ({reason}): {detail}")
        self.reason = reason


@dataclass
class SignatureVerificationMetrics:
    """In-memory rejection/acceptance counters for the ingest report path."""

    verified: int = 0
    rejected: dict[str, int] = field(default_factory=dict)

    def record_verified(self) -> None:
        self.verified += 1

    def record_rejected(self, reason: str) -> None:
        self.rejected[reason] = self.rejected.get(reason, 0) + 1


@dataclass(frozen=True)
class KeyDirectory:
    """Immutable kid -> Ed25519 public key mapping loaded at startup."""

    keys: Mapping[str, Ed25519PublicKey]

    def resolve(self, kid: str) -> Ed25519PublicKey | None:
        return self.keys.get(kid)


def _decode_base64url(segment: str, what: str) -> bytes:
    if not segment or "=" in segment or not re.fullmatch(r"[A-Za-z0-9_-]+", segment):
        raise SignatureVerificationError(REASON_MALFORMED_JWS, f"{what} is not unpadded base64url")
    try:
        return base64.urlsafe_b64decode(segment + "=" * (-len(segment) % 4))
    except (binascii.Error, ValueError) as error:
        raise SignatureVerificationError(
            REASON_MALFORMED_JWS, f"{what} is not valid base64url"
        ) from error


def load_key_directory(path: Path) -> KeyDirectory:
    """Load the mounted producer public-key directory, failing closed.

    Any deviation — absent path, symlink, oversized or unreadable file,
    invalid JSON, non-object shape, malformed kid or malformed key — is a
    startup-fatal ``ValueError``; there is no degraded mode.
    """
    if not path.is_file() or path.is_symlink():
        raise ValueError(f"key directory {path} must be a regular non-symlink file (fail-closed)")
    if path.stat().st_size > MAX_KEY_DIRECTORY_BYTES or path.stat().st_size == 0:
        raise ValueError(f"key directory {path} must contain 1 to {MAX_KEY_DIRECTORY_BYTES} bytes")
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"key directory {path} is not valid JSON (fail-closed)") from error
    if not isinstance(document, dict) or not document:
        raise ValueError(f"key directory {path} must be a non-empty JSON object of kid to key")
    keys: dict[str, Ed25519PublicKey] = {}
    for kid, encoded in document.items():
        if not isinstance(kid, str) or not KID_PATTERN.fullmatch(kid):
            raise ValueError(f"key directory {path} carries a malformed kid (fail-closed)")
        if (
            not isinstance(encoded, str)
            or "=" in encoded
            or not re.fullmatch(r"[A-Za-z0-9_-]+", encoded)
        ):
            raise ValueError(f"key directory {path} key for {kid!r} is not unpadded base64url")
        try:
            raw = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
        except (binascii.Error, ValueError) as error:
            raise ValueError(
                f"key directory {path} key for {kid!r} is not valid base64url"
            ) from error
        if len(raw) != ED25519_PUBLIC_KEY_BYTES:
            raise ValueError(
                f"key directory {path} key for {kid!r} is not a 32-byte Ed25519 public key"
            )
        keys[kid] = Ed25519PublicKey.from_public_bytes(raw)
    return KeyDirectory(keys)


def load_key_directory_from_env(env: Mapping[str, str] | None = None) -> KeyDirectory:
    """Fail-closed startup loader bound to the KEY_DIRECTORY_PATH variable."""
    source = os.environ if env is None else env
    raw = source.get("KEY_DIRECTORY_PATH", "")
    if not raw.strip():
        raise ValueError("KEY_DIRECTORY_PATH is required (fail-closed)")
    return load_key_directory(Path(raw))


class EnvelopeSignatureVerifier:
    """Verifies provenance signatures and counts rejections by reason."""

    def __init__(
        self, directory: KeyDirectory, metrics: SignatureVerificationMetrics | None = None
    ) -> None:
        self._directory = directory
        self.metrics = metrics if metrics is not None else SignatureVerificationMetrics()

    def verify(self, envelope: dict[str, Any]) -> str:
        """Verify *envelope* and return the authenticated kid; raise otherwise."""
        try:
            return self._verify(envelope)
        except SignatureVerificationError as error:
            self.metrics.record_rejected(error.reason)
            LOGGER.warning(
                "envelope signature rejected",
                extra={"reason": error.reason, "event_id": _event_id(envelope)},
            )
            raise

    def _verify(self, envelope: dict[str, Any]) -> str:
        provenance = envelope.get("provenance")
        if not isinstance(provenance, dict):
            raise SignatureVerificationError(
                REASON_MALFORMED_JWS, "envelope carries no provenance object"
            )
        signature = provenance.get("signature")
        if not isinstance(signature, str):
            raise SignatureVerificationError(
                REASON_MALFORMED_JWS, "provenance.signature is not text"
            )
        segments = signature.split(".")
        if len(segments) != 3:
            raise SignatureVerificationError(
                REASON_MALFORMED_JWS, "JWS compact form must have three segments"
            )
        encoded_header, encoded_payload, encoded_signature = segments
        header_bytes = _decode_base64url(encoded_header, "protected header")
        payload_bytes = _decode_base64url(encoded_payload, "payload")
        signature_bytes = _decode_base64url(encoded_signature, "signature")
        try:
            header = json.loads(header_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise SignatureVerificationError(
                REASON_MALFORMED_JWS, "protected header is not valid JSON"
            ) from error
        if not isinstance(header, dict):
            raise SignatureVerificationError(
                REASON_MALFORMED_JWS, "protected header must be a JSON object"
            )
        if header.get("alg") != "EdDSA":
            raise SignatureVerificationError(
                REASON_UNSUPPORTED_ALG, "protected header alg must be EdDSA"
            )
        kid = header.get("kid")
        if not isinstance(kid, str) or not KID_PATTERN.fullmatch(kid):
            raise SignatureVerificationError(
                REASON_MALFORMED_JWS, "protected header kid is malformed"
            )
        public_key = self._directory.resolve(kid)
        if public_key is None:
            raise SignatureVerificationError(
                REASON_UNKNOWN_KID, f"kid {kid!r} is not in the key directory"
            )

        signed_document = {key: value for key, value in envelope.items() if key != "provenance"}
        signed_document["provenance"] = {
            key: value for key, value in provenance.items() if key != "signature"
        }
        try:
            expected_payload = canonicalize(signed_document).encode("utf-8")
        except ValueError as error:
            raise SignatureVerificationError(
                REASON_PAYLOAD_MISMATCH, f"envelope cannot be canonicalized: {error}"
            ) from error
        # The payload segment must carry exactly the canonical envelope bytes:
        # the compact serialization is self-verifying by specification.
        if payload_bytes != expected_payload:
            raise SignatureVerificationError(
                REASON_PAYLOAD_MISMATCH, "JWS payload does not match the canonical envelope"
            )
        try:
            public_key.verify(
                signature_bytes, f"{encoded_header}.{encoded_payload}".encode("ascii")
            )
        except InvalidSignature as error:
            raise SignatureVerificationError(
                REASON_INVALID_SIGNATURE, "Ed25519 signature does not verify"
            ) from error
        self.metrics.record_verified()
        return kid


def sign_envelope_for_test(
    envelope: dict[str, Any], private_key: Ed25519PrivateKey, kid: str
) -> str:
    """Produce the fleet JWS for *envelope*; test/fixture helper only."""
    provenance = envelope.get("provenance")
    if not isinstance(provenance, dict):
        raise ValueError("envelope carries no provenance object")
    signed_document = {key: value for key, value in envelope.items() if key != "provenance"}
    signed_document["provenance"] = {
        key: value for key, value in provenance.items() if key != "signature"
    }
    header = (
        base64.urlsafe_b64encode(
            json.dumps({"alg": "EdDSA", "kid": kid}, separators=(",", ":")).encode("utf-8")
        )
        .rstrip(b"=")
        .decode("ascii")
    )
    payload = (
        base64.urlsafe_b64encode(canonicalize(signed_document).encode("utf-8"))
        .rstrip(b"=")
        .decode("ascii")
    )
    signature = (
        base64.urlsafe_b64encode(private_key.sign(f"{header}.{payload}".encode("ascii")))
        .rstrip(b"=")
        .decode("ascii")
    )
    return f"{header}.{payload}.{signature}"


def export_public_key_for_test(public_key: Ed25519PublicKey) -> str:
    """Encode an Ed25519 public key as unpadded base64url; test helper only."""
    raw = public_key.public_bytes(Encoding.Raw, PublicFormat.Raw)
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def private_key_from_raw_for_test(raw: bytes) -> Ed25519PrivateKey:
    """Load an Ed25519 private key from raw seed bytes; test helper only."""
    if len(raw) != 32:
        raise ValueError("Ed25519 seed must be 32 bytes")
    return Ed25519PrivateKey.from_private_bytes(raw)


def _event_id(envelope: dict[str, Any]) -> str:
    value = envelope.get("eventId")
    return value if isinstance(value, str) else "unknown"
