"""Unit tests for simcorp_fields.py.

All tests mock _describe_object so no live-org calls are made. The
describe-check is tested by verifying the logic that compares the
mocked describe response against SCHEMA.
"""

from __future__ import annotations

import pytest

import simcorp_fields
from simcorp_fields import (
    OPPORTUNITY_FIELDS,
    SCHEMA,
    SchemaDriftError,
    assert_org_schema,
)


# --- Constants invariants -------------------------------------------------


def test_opportunity_fields_is_tuple_of_unique_strings():
    assert isinstance(OPPORTUNITY_FIELDS, tuple)
    assert len(set(OPPORTUNITY_FIELDS)) == len(OPPORTUNITY_FIELDS)
    assert all(isinstance(f, str) for f in OPPORTUNITY_FIELDS)
    assert len(OPPORTUNITY_FIELDS) > 0


def test_schema_dict_covers_every_constant_tuple():
    # Every *_FIELDS tuple in the module must be registered in SCHEMA.
    declared_constants = {
        name: value
        for name, value in vars(simcorp_fields).items()
        if name.endswith("_FIELDS") and isinstance(value, tuple)
    }
    registered_values = set(SCHEMA.values())
    for name, tpl in declared_constants.items():
        assert tpl in registered_values, (
            f"{name} is a *_FIELDS tuple but is not in SCHEMA"
        )


# --- assert_org_schema happy path ----------------------------------------


def test_assert_org_schema_passes_when_all_fields_present(monkeypatch):
    def fake_describe(instance_url, access_token, obj):
        return {f: {} for f in SCHEMA[obj]}

    monkeypatch.setattr(simcorp_fields, "_describe_object", fake_describe)
    assert_org_schema("https://x", "tok", objects=["Opportunity"])


# --- assert_org_schema failure paths -------------------------------------


def test_assert_org_schema_raises_on_single_missing_field(monkeypatch):
    missing_field = OPPORTUNITY_FIELDS[0]
    fake_org_fields = set(OPPORTUNITY_FIELDS) - {missing_field}

    def fake_describe(instance_url, access_token, obj):
        return {f: {} for f in fake_org_fields}

    monkeypatch.setattr(simcorp_fields, "_describe_object", fake_describe)
    with pytest.raises(SchemaDriftError, match=missing_field):
        assert_org_schema("https://x", "tok", objects=["Opportunity"])


def test_assert_org_schema_lists_all_missing_fields_in_one_error(monkeypatch):
    missing = set(list(OPPORTUNITY_FIELDS)[:2])
    fake_org_fields = set(OPPORTUNITY_FIELDS) - missing

    def fake_describe(instance_url, access_token, obj):
        return {f: {} for f in fake_org_fields}

    monkeypatch.setattr(simcorp_fields, "_describe_object", fake_describe)
    with pytest.raises(SchemaDriftError) as exc_info:
        assert_org_schema("https://x", "tok", objects=["Opportunity"])
    msg = str(exc_info.value)
    for f in missing:
        assert f in msg, f"Missing field {f} not named in error: {msg}"


def test_assert_org_schema_walks_multiple_objects(monkeypatch):
    seen = []

    def fake_describe(instance_url, access_token, obj):
        seen.append(obj)
        return {f: {} for f in SCHEMA[obj]}

    monkeypatch.setattr(simcorp_fields, "_describe_object", fake_describe)
    assert_org_schema("https://x", "tok", objects=["Opportunity", "Account"])
    assert seen == ["Opportunity", "Account"]


def test_assert_org_schema_default_objects_is_full_schema(monkeypatch):
    seen = []

    def fake_describe(instance_url, access_token, obj):
        seen.append(obj)
        return {f: {} for f in SCHEMA[obj]}

    monkeypatch.setattr(simcorp_fields, "_describe_object", fake_describe)
    assert_org_schema("https://x", "tok")  # objects=None
    assert set(seen) == set(SCHEMA.keys())


def test_schema_drift_error_message_names_the_constant_tuple(monkeypatch):
    # Contract from spec Error Handling section 4.3: the error message
    # must tell the operator WHICH constant tuple to edit.
    missing_field = OPPORTUNITY_FIELDS[0]
    fake_org_fields = set(OPPORTUNITY_FIELDS) - {missing_field}

    def fake_describe(instance_url, access_token, obj):
        return {f: {} for f in fake_org_fields}

    monkeypatch.setattr(simcorp_fields, "_describe_object", fake_describe)
    with pytest.raises(SchemaDriftError) as exc_info:
        assert_org_schema("https://x", "tok", objects=["Opportunity"])
    msg = str(exc_info.value)
    assert "OPPORTUNITY_FIELDS" in msg, (
        f"Error message must name the constant tuple to edit; got: {msg}"
    )
