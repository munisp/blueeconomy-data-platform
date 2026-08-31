#!/usr/bin/env python3
"""Re-sign the committed producer envelope fixtures under the fleet scheme.

Regenerates ``tests/fixtures/key-directory.json`` and rewrites every fixture's
``provenance.signature`` as a JWS compact serialization (EdDSA/Ed25519) over
the JCS-canonicalized envelope-minus-signature, using deterministic
fixture-only keys (see ``tests/signing_helpers.py``). Run after changing a
fixture or the fleet signature scheme:

    python scripts/sign-fixture-envelopes.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "tests"))

from signing_helpers import (  # noqa: E402
    FIXTURE_KEY_DIRECTORY,
    fixture_key_directory_entries,
    fixture_kid_for_producer,
    fixture_private_key,
)
from blueeconomy_data_platform.signature_verification import sign_envelope_for_test  # noqa: E402

FIXTURES = REPO_ROOT / "tests" / "fixtures" / "envelopes"


def main() -> None:
    fixtures = sorted(FIXTURES.glob("*.json"))
    if not fixtures:
        raise SystemExit("no envelope fixtures found")
    producers: list[str] = []
    for path in fixtures:
        envelope = json.loads(path.read_text(encoding="utf-8"))
        producer = envelope["producer"]
        producers.append(producer)
        kid = fixture_kid_for_producer(producer)
        envelope["provenance"]["signature"] = sign_envelope_for_test(
            envelope, fixture_private_key(kid), kid
        )
        path.write_text(json.dumps(envelope, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"re-signed {path.name} with kid {kid}")
    # The directory also trusts the synthetic producers used by unit tests
    # (the waterway-safety sample envelope and the default test kid).
    directory = fixture_key_directory_entries(
        producers + ["blueeconomy-waterway-safety", "blueeconomy-data-platform-test"]
    )
    FIXTURE_KEY_DIRECTORY.write_text(json.dumps(directory, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {FIXTURE_KEY_DIRECTORY.relative_to(REPO_ROOT)} with {len(directory)} keys")


if __name__ == "__main__":
    main()
