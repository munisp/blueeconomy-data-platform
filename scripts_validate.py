from __future__ import annotations

import compileall
import importlib
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

print("Validated lakehouse package compilation and governed event-envelope schema.")
