#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gc
import json
from pathlib import Path

from deltalake import DeltaTable


def read_object(path: Path) -> dict[str, object]:
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True)
    parser.add_argument("--first-report", required=True, type=Path)
    parser.add_argument("--second-report", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    arguments = parser.parse_args()

    first = read_object(arguments.first_report)
    second = read_object(arguments.second_report)
    table = DeltaTable(arguments.table)
    arrow_table = table.to_pyarrow_table()
    rows = arrow_table.to_pylist()
    if len(rows) != 1:
        raise ValueError(f"expected one Delta event after replay, got {len(rows)}")
    if first.get("messages_received") != 1 or first.get("records_written") != 1:
        raise ValueError("first Kafka consumption did not persist exactly one event")
    if second.get("messages_received") != 1:
        raise ValueError("second Kafka group did not receive the replayed event")
    if second.get("records_written") != 0 or second.get("records_already_present") != 1:
        raise ValueError("second Kafka consumption was not idempotent in Delta")
    for report in (first, second):
        offsets = report.get("committed_offsets")
        if not isinstance(offsets, dict) or list(offsets.values()) != [1]:
            raise ValueError("Kafka offset 1 was not confirmed for the consumed partition")

    result = {
        "kafka_messages_consumed": 2,
        "consumer_groups_verified": 2,
        "committed_offset": 1,
        "delta_rows": 1,
        "delta_table_version": table.version(),
        "first_records_written": first["records_written"],
        "replay_records_already_present": second["records_already_present"],
        "event_id": rows[0]["event_id"],
        "event_type": rows[0]["event_type"],
    }
    arguments.output.parent.mkdir(parents=True, exist_ok=True)
    arguments.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, sort_keys=True))
    del rows
    del arrow_table
    del table
    gc.collect()


if __name__ == "__main__":
    main()
