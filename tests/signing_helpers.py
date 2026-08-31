"""Deterministic fleet-scheme signing helpers shared by the ingest tests.

Keys are derived from fixed seeds so the committed fixtures and the key
directory are reproducible; they exist only for tests and must never be used
outside this suite.
"""

from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path
from typing import Any

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from blueeconomy_data_platform.signature_verification import (
    EnvelopeSignatureVerifier,
    KeyDirectory,
    export_public_key_for_test,
    load_key_directory,
    private_key_from_raw_for_test,
    sign_envelope_for_test,
)

FIXTURE_KEY_DIRECTORY = Path(__file__).resolve().parent / "fixtures" / "key-directory.json"
SEED_DOMAIN = "blueeconomy.fixture-signing-key.v1|"
DEFAULT_TEST_KID = "blueeconomy-data-platform-test-0"


def fixture_seed(kid: str) -> bytes:
    return hashlib.sha256((SEED_DOMAIN + kid).encode("utf-8")).digest()


def fixture_private_key(kid: str) -> Ed25519PrivateKey:
    return private_key_from_raw_for_test(fixture_seed(kid))


def fixture_kid_for_producer(producer: str) -> str:
    return f"{producer}-0"


def fixture_key_directory_entries(producers: list[str]) -> dict[str, str]:
    entries: dict[str, str] = {}
    for producer in producers:
        kid = fixture_kid_for_producer(producer)
        public_key = fixture_private_key(kid).public_key()
        entries[kid] = export_public_key_for_test(public_key)
    return entries


def load_test_verifier() -> EnvelopeSignatureVerifier:
    """Verifier over the committed fixture key directory plus the default kid."""
    return EnvelopeSignatureVerifier(load_key_directory(FIXTURE_KEY_DIRECTORY))


def sign_envelope(envelope: dict[str, Any], kid: str | None = None) -> dict[str, Any]:
    """Return a copy of *envelope* carrying a valid fleet provenance signature."""
    signed = copy.deepcopy(envelope)
    resolved_kid = kid
    if resolved_kid is None:
        producer = signed.get("producer")
        if not isinstance(producer, str):
            raise ValueError("envelope producer is required to derive the fixture kid")
        resolved_kid = fixture_kid_for_producer(producer)
    provenance = signed.get("provenance")
    if not isinstance(provenance, dict):
        raise ValueError("envelope carries no provenance object")
    provenance["signature"] = sign_envelope_for_test(
        signed, fixture_private_key(resolved_kid), resolved_kid
    )
    return signed


def signed_envelope_bytes(envelope: dict[str, Any], kid: str | None = None) -> bytes:
    return json.dumps(sign_envelope(envelope, kid), separators=(",", ":")).encode("utf-8")


def single_use_verifier(kid: str) -> EnvelopeSignatureVerifier:
    """Verifier trusting exactly one (deterministically derived) kid."""
    public_key = fixture_private_key(kid).public_key()
    directory = KeyDirectory({kid: public_key})
    return EnvelopeSignatureVerifier(directory)
