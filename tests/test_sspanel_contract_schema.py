import ast
import json
import subprocess
import sys
from pathlib import Path
from typing import get_args

import pytest

from aiograpi_rest.routers import sspanel
from aiograpi_rest.routers.sspanel import CONTRACT_FIXTURE
from scripts.sync_sspanel_contract import build_fixture


def _schema_path() -> Path:
    return Path(__file__).resolve().parents[2] / "ss-toolkit" / "contracts" / "external-executor.schema.json"


def _schema_enum(schema, field):
    items = schema["properties"][field]["items"]
    if "$ref" in items:
        ref_field = (
            items["$ref"]
            .removeprefix("#/properties/")
            .removesuffix("/items/enum")
            .removesuffix("/items")
        )
        items = schema["properties"][ref_field]["items"]
    return items["enum"]


def test_executor_fixture_matches_canonical_schema_and_implemented_subset():
    schema_path = _schema_path()
    if not schema_path.is_file():
        pytest.skip(
            "canonical ss-toolkit schema is not mounted; run this assertion from the root checkout or CI contract job"
        )
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    fields = (
        "contractVersions",
        "platforms",
        "jobStatuses",
        "eventTypes",
        "moduleFeatures",
        "workflowTypes",
    )
    assert {field: CONTRACT_FIXTURE[field] for field in fields} == {
        field: _schema_enum(schema, field) for field in fields
    }
    assert CONTRACT_FIXTURE == build_fixture(schema)
    for field in ("instagramActionTypes", "instagramImplementedCapabilities"):
        assert set(CONTRACT_FIXTURE[field]).issubset(_schema_enum(schema, field))


def test_account_selector_constraints_keep_known_fields_typed_and_extensible():
    schema_path = _schema_path()
    if not schema_path.is_file():
        pytest.skip(
            "canonical ss-toolkit schema is not mounted; run this assertion from the root checkout or CI contract job"
        )

    constraints = json.loads(schema_path.read_text(encoding="utf-8"))["$defs"][
        "externalExecutorAccountConstraints"
    ]

    assert constraints["type"] == "object"
    assert constraints["additionalProperties"] is True
    assert constraints["properties"]["allowedExecutorAccountIds"]["minItems"] == 1
    assert constraints["properties"]["excludeExecutorAccountIds"]["minItems"] == 0


def _schema_with_additional_known_action():
    schema = {"properties": {
        field: {"items": {"enum": list(values)}} for field, values in CONTRACT_FIXTURE.items()
    }}
    for field in ("instagramActionTypes", "instagramImplementedCapabilities"):
        schema["properties"][field]["items"]["enum"].append("instagram.media.like")
    return schema


def _executor_source(actions, input_actions=None, dispatched_actions=None):
    return (
        f"ActionType = Literal[{', '.join(repr(action) for action in actions)}]\n"
        f"CAPABILITY_INPUT_SCHEMAS: dict = {dict.fromkeys(input_actions if input_actions is not None else actions, {})!r}\n"
        "async def execute_supported_job(job, request, store):\n"
        f"    if request.actionType in {set(dispatched_actions if dispatched_actions is not None else actions)!r}:\n"
        "        return job\n"
    )


def test_sync_keeps_unsupported_known_actions_out_of_executor_fixture(tmp_path):
    schema = _schema_with_additional_known_action()
    assert build_fixture(schema) == CONTRACT_FIXTURE
    schema_path = tmp_path / "schema.json"
    output = tmp_path / "fixture.json"
    schema_path.write_text(json.dumps(schema))
    subprocess.run(
        [sys.executable, "scripts/sync_sspanel_contract.py", "--schema", str(schema_path), "--output", str(output)],
        check=True,
        cwd=Path(__file__).resolve().parents[1],
        timeout=10,
    )
    assert json.loads(output.read_text()) == CONTRACT_FIXTURE
    assert "instagram.media.like" not in json.loads(output.read_text())["instagramImplementedCapabilities"]


@pytest.mark.parametrize("mismatch", ["unknown", "input_schema", "dispatcher", "canonical_implemented", "workflow"])
def test_sync_rejects_unknown_or_inconsistent_executor_advertisements(mismatch):
    schema = _schema_with_additional_known_action()
    actions = list(CONTRACT_FIXTURE["instagramActionTypes"])
    input_actions = list(actions)
    dispatched_actions = list(actions)
    if mismatch == "unknown":
        actions.append("instagram.unknown")
        input_actions.append("instagram.unknown")
        dispatched_actions.append("instagram.unknown")
    elif mismatch == "input_schema":
        input_actions.remove("instagram.profile.get")
    elif mismatch == "dispatcher":
        dispatched_actions.remove("instagram.profile.get")
    elif mismatch == "canonical_implemented":
        schema["properties"]["instagramImplementedCapabilities"]["items"]["enum"].remove("instagram.profile.get")
    else:
        schema["properties"]["workflowTypes"]["items"]["enum"].append("instagram.media.like")
    with pytest.raises(ValueError):
        build_fixture(schema, executor_source=_executor_source(actions, input_actions, dispatched_actions))


def test_executor_fixture_advertises_exactly_its_typed_dispatcher_and_input_schemas():
    source = Path(sspanel.__file__).read_text()
    dispatcher = next(
        node for node in ast.parse(source).body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "execute_supported_job"
    )
    dispatched_actions = {
        node.value for node in ast.walk(dispatcher)
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value.startswith("instagram.")
    }
    assert len(dispatched_actions) == 16
    for field in ("instagramActionTypes", "instagramImplementedCapabilities"):
        assert len(CONTRACT_FIXTURE[field]) == len(set(CONTRACT_FIXTURE[field])) == 16
    assert set(CONTRACT_FIXTURE["instagramActionTypes"]) == dispatched_actions
    assert set(CONTRACT_FIXTURE["instagramImplementedCapabilities"]) == dispatched_actions
    assert set(get_args(sspanel.ActionType)) == dispatched_actions
    assert set(sspanel.CAPABILITY_INPUT_SCHEMAS) == dispatched_actions
    assert "instagram.media.like" not in dispatched_actions
