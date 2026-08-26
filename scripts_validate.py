from __future__ import annotations

import compileall
import importlib
import json
import sys
from pathlib import Path

repository = Path(__file__).resolve().parent
sys.path.insert(0, str(repository / "src"))
ingest = importlib.import_module("blueeconomy_data_platform.ingest")

if not compileall.compile_dir(repository / "src", quiet=1):
    raise SystemExit("Python source compilation failed")

validator = ingest.load_schema(repository / "schemas" / "event-envelope.schema.json")
if validator.schema.get("title") != "Blue Economy Lakehouse Event Envelope":
    raise SystemExit("Unexpected event envelope schema title")
if not validator.schema.get("required"):
    raise SystemExit("Event envelope schema has no required fields")

codeowners = repository / ".github" / "CODEOWNERS"
branch_policy = repository / ".github" / "branch-protection.main.json"
if not codeowners.is_file() or not codeowners.read_text(encoding="utf-8").strip():
    raise SystemExit("CODEOWNERS is required and must not be empty")
try:
    policy = json.loads(branch_policy.read_text(encoding="utf-8"))
except (OSError, json.JSONDecodeError) as exc:
    raise SystemExit(f"branch protection policy is invalid: {exc}") from exc

reviews = policy.get("required_pull_request_reviews", {})
if not (
    reviews.get("required_approving_review_count", 0) >= 2
    and reviews.get("require_code_owner_reviews") is True
    and reviews.get("require_last_push_approval") is True
    and policy.get("enforce_admins") is True
    and policy.get("required_linear_history") is True
    and policy.get("required_conversation_resolution") is True
    and policy.get("allow_force_pushes") is False
    and policy.get("allow_deletions") is False
):
    raise SystemExit("branch protection policy does not meet strict review requirements")

print("Validated lakehouse package, governed event-envelope schema, and strict review controls.")
