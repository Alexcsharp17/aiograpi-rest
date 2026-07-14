import json
from pathlib import Path

import pytest

from aiograpi_rest.routers.sspanel import CONTRACT_FIXTURE


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


def test_executor_fixture_matches_canonical_schema():
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
        "instagramActionTypes",
        "workflowTypes",
        "instagramImplementedCapabilities",
    )
    assert {field: CONTRACT_FIXTURE[field] for field in fields} == {
        field: _schema_enum(schema, field) for field in fields
    }
