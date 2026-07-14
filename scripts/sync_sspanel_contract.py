#!/usr/bin/env python3
"""Synchronize the executor fixture from the canonical SS-panel JSON Schema."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

FIELDS = (
    "contractVersions",
    "moduleFeatures",
    "platforms",
    "jobStatuses",
    "eventTypes",
    "instagramActionTypes",
    "workflowTypes",
    "instagramImplementedCapabilities",
)


def _default_schema_path() -> Path:
    executor_root = Path(__file__).resolve().parents[1]
    modules_root = executor_root.parent
    return modules_root / "ss-toolkit" / "contracts" / "external-executor.schema.json"


def _enum_values(schema: dict[str, Any], field: str) -> list[str]:
    property_schema = schema["properties"][field]
    items = property_schema["items"]
    if "$ref" in items:
        ref = items["$ref"]
        if not ref.startswith("#/properties/"):
            raise ValueError(f"Unsupported enum reference for {field}: {ref}")
        ref_field = ref.removeprefix("#/properties/").removesuffix("/items").removesuffix("/items/enum")
        items = schema["properties"][ref_field]["items"]
    values = items.get("enum")
    if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
        raise ValueError(f"Schema field {field} must contain a string enum")
    return values


def build_fixture(schema: dict[str, Any]) -> dict[str, list[str]]:
    return {field: _enum_values(schema, field) for field in FIELDS}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", type=Path, default=_default_schema_path())
    parser.add_argument(
        "--output",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "aiograpi_rest" / "sspanel_contract.json",
    )
    args = parser.parse_args()

    schema = json.loads(args.schema.read_text(encoding="utf-8"))
    fixture = build_fixture(schema)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(fixture, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
