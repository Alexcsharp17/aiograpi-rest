#!/usr/bin/env python3
"""Synchronize canonical contract values without advertising unimplemented actions."""

from __future__ import annotations

import argparse
import ast
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


def _executor_actions(source: str) -> set[str]:
    module = ast.parse(source)
    action_types: tuple[str, ...] = ()
    input_actions: set[str] = set()
    dispatched_actions: set[str] = set()
    for node in module.body:
        if isinstance(node, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "ActionType" for target in node.targets
        ):
            if not isinstance(node.value, ast.Subscript):
                raise ValueError("ActionType must declare literal executor actions")
            values = ast.literal_eval(node.value.slice)
            action_types = values if isinstance(values, tuple) else (values,)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id == "CAPABILITY_INPUT_SCHEMAS" and isinstance(node.value, ast.Dict):
                input_actions = {ast.literal_eval(key) for key in node.value.keys}
        elif isinstance(node, ast.AsyncFunctionDef) and node.name == "execute_supported_job":
            for condition in ast.walk(node):
                if (
                    isinstance(condition, ast.Compare)
                    and isinstance(condition.left, ast.Attribute)
                    and isinstance(condition.left.value, ast.Name)
                    and condition.left.value.id == "request"
                    and condition.left.attr == "actionType"
                    and len(condition.ops) == 1
                    and isinstance(condition.ops[0], (ast.Eq, ast.In))
                ):
                    values = ast.literal_eval(condition.comparators[0])
                    dispatched_actions.update([values] if isinstance(values, str) else values)
    if not action_types or not all(isinstance(action, str) for action in action_types):
        raise ValueError("ActionType must contain executor action strings")
    actions = set(action_types)
    if len(actions) != len(action_types):
        raise ValueError("ActionType must contain unique executor actions")
    if actions != input_actions or actions != dispatched_actions:
        raise ValueError("Executor actions, input schemas, and dispatcher must advertise the same capabilities")
    return actions


def build_fixture(schema: dict[str, Any], *, executor_source: str | None = None) -> dict[str, list[str]]:
    fixture = {field: _enum_values(schema, field) for field in FIELDS}
    if executor_source is None:
        facade = Path(__file__).resolve().parents[1] / "aiograpi_rest" / "routers" / "sspanel.py"
        executor_source = facade.read_text(encoding="utf-8")
    actions = _executor_actions(executor_source)
    for field in ("instagramActionTypes", "instagramImplementedCapabilities"):
        unknown = actions - set(fixture[field])
        if unknown:
            raise ValueError(f"Executor capabilities are absent from canonical {field}: {sorted(unknown)}")
        fixture[field] = [action for action in fixture[field] if action in actions]
    if not set(fixture["workflowTypes"]).issubset(actions):
        raise ValueError("Canonical workflows must be backed by executor capabilities")
    return fixture


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
