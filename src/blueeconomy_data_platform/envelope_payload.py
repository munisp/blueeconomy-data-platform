"""Domain-field extraction from retained envelope payloads (phase 8).

The lakehouse retains, per event, the FHIR message entry's primary resource
plus the envelope provenance block (``payload_json``). Governed producers
carry the domain record either directly on the resource (typed payloads, for
example the geo-service ``geo.*.v1`` events) or as a JSON document inside the
``domain-payload`` FHIR Basic extension (the port-interoperability outbox
pattern, ``internal/events/envelope.go``). This module resolves both shapes
into one flat domain-field mapping; anything malformed fails closed.
"""

from __future__ import annotations

import json
from typing import Any

DOMAIN_PAYLOAD_EXTENSION_URL = "https://blueeconomy.gov.ng/fhir/StructureDefinition/domain-payload"

# Envelope-carriage fields that never form part of the domain record.
_CARRIAGE_FIELDS = frozenset(
    {"resourceType", "id", "code", "extension", "provenance", "meta", "text"}
)


def extract_domain_fields(payload: dict[str, Any]) -> dict[str, Any]:
    """Return the flat domain record carried by a retained event payload.

    Direct resource fields (minus FHIR carriage fields) are merged with the
    parsed ``domain-payload`` extension document; on overlap the extension
    document wins because it is the producer's canonical domain record. A
    malformed extension document fails closed.
    """
    if not isinstance(payload, dict):
        raise ValueError("event payload must be a JSON object")
    fields = {key: value for key, value in payload.items() if key not in _CARRIAGE_FIELDS}
    extension = payload.get("extension")
    if extension is None:
        return fields
    if not isinstance(extension, list):
        raise ValueError("payload extension must be a JSON array")
    for entry in extension:
        if not isinstance(entry, dict):
            raise ValueError("payload extension entries must be JSON objects")
        if entry.get("url") != DOMAIN_PAYLOAD_EXTENSION_URL:
            continue
        document_text = entry.get("valueString")
        if not isinstance(document_text, str) or not document_text.strip():
            raise ValueError("domain-payload extension must carry a non-empty JSON document")
        try:
            document = json.loads(document_text)
        except json.JSONDecodeError as error:
            raise ValueError("domain-payload extension is not valid JSON") from error
        if not isinstance(document, dict):
            raise ValueError("domain-payload extension document must be a JSON object")
        fields.update(document)
    return fields


__all__ = ["DOMAIN_PAYLOAD_EXTENSION_URL", "extract_domain_fields"]
