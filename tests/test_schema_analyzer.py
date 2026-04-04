import unittest

from contracts.schema_analyzer import (
    classify_enum_change,
    classify_range_change,
    classify_type_change,
    diff_schemas,
)


def _contract(tables: list[dict]) -> dict:
    """Minimal Bitol contract fixture."""
    return {"schema": {"tables": tables}}


def _table(name: str, fields: list[dict]) -> dict:
    return {"name": name, "fields": fields}


def _field(name: str, **kwargs) -> dict:
    return {"name": name, **kwargs}


# ---------------------------------------------------------------------------
# classify_type_change
# ---------------------------------------------------------------------------


class ClassifyTypeChangeTest(unittest.TestCase):
    def test_number_to_integer_is_critical(self) -> None:
        severity, _ = classify_type_change("number", "integer")
        self.assertEqual("CRITICAL", severity)

    def test_string_to_uuid_is_critical(self) -> None:
        severity, _ = classify_type_change("string", "uuid")
        self.assertEqual("CRITICAL", severity)

    def test_integer_to_string_is_critical(self) -> None:
        severity, _ = classify_type_change("integer", "string")
        self.assertEqual("CRITICAL", severity)

    def test_boolean_change_is_high(self) -> None:
        severity, _ = classify_type_change("string", "boolean")
        self.assertEqual("HIGH", severity)

    def test_same_type_is_compat(self) -> None:
        severity, _ = classify_type_change("string", "string")
        self.assertEqual("COMPAT", severity)

    def test_unit_float_to_pct_integer_is_critical(self) -> None:
        old_field = {"type": "number", "minimum": 0.0, "maximum": 1.0}
        new_field = {"type": "integer", "minimum": 0, "maximum": 100}
        severity, reason = classify_type_change("number", "integer", old_field, new_field)
        self.assertEqual("CRITICAL", severity)
        self.assertIn("scale change", reason.lower())


# ---------------------------------------------------------------------------
# classify_enum_change
# ---------------------------------------------------------------------------


class ClassifyEnumChangeTest(unittest.TestCase):
    def test_removed_value_is_critical(self) -> None:
        severity, reason = classify_enum_change(["a", "b", "c"], ["a", "b"])
        self.assertEqual("CRITICAL", severity)
        self.assertIn("c", reason)

    def test_added_value_is_compat(self) -> None:
        severity, _ = classify_enum_change(["a", "b"], ["a", "b", "c"])
        self.assertEqual("COMPAT", severity)

    def test_unchanged_is_compat(self) -> None:
        severity, _ = classify_enum_change(["x"], ["x"])
        self.assertEqual("COMPAT", severity)


# ---------------------------------------------------------------------------
# classify_range_change
# ---------------------------------------------------------------------------


class ClassifyRangeChangeTest(unittest.TestCase):
    def test_tightened_minimum_is_critical(self) -> None:
        severity, reason = classify_range_change(0.0, 1.0, 0.5, 1.0)
        self.assertIn(severity, ("CRITICAL", "HIGH"))
        self.assertIn("minimum", reason.lower())

    def test_new_minimum_added_is_critical(self) -> None:
        severity, _ = classify_range_change(None, None, 0.0, None)
        self.assertIn(severity, ("CRITICAL", "HIGH"))

    def test_widened_range_is_compat(self) -> None:
        severity, _ = classify_range_change(0.0, 1.0, 0.0, 2.0)
        self.assertEqual("COMPAT", severity)

    def test_unchanged_range_is_compat(self) -> None:
        severity, _ = classify_range_change(0.0, 1.0, 0.0, 1.0)
        self.assertEqual("COMPAT", severity)


# ---------------------------------------------------------------------------
# diff_schemas
# ---------------------------------------------------------------------------


class DiffSchemasTest(unittest.TestCase):
    def test_identical_schemas_are_compatible(self) -> None:
        schema = _contract([_table("docs", [_field("id", type="string")])])
        diff = diff_schemas(schema, schema)
        self.assertEqual("compatible", diff["verdict"])
        self.assertEqual(0, diff["total_breaking"])

    def test_removed_table_is_breaking(self) -> None:
        baseline = _contract([_table("docs", [_field("id", type="string")])])
        current = _contract([])
        diff = diff_schemas(baseline, current)
        self.assertEqual("breaking", diff["verdict"])
        self.assertTrue(any(c["type"] == "table_removed" for c in diff["breaking_changes"]))

    def test_added_table_is_compatible(self) -> None:
        baseline = _contract([])
        current = _contract([_table("new_table", [_field("id", type="string")])])
        diff = diff_schemas(baseline, current)
        self.assertEqual("compatible", diff["verdict"])
        self.assertTrue(any(c["type"] == "table_added" for c in diff["compatible_changes"]))

    def test_required_field_removed_is_critical(self) -> None:
        baseline = _contract([_table("docs", [_field("id", type="string", required=True)])])
        current = _contract([_table("docs", [])])
        diff = diff_schemas(baseline, current)
        self.assertEqual("breaking", diff["verdict"])
        self.assertTrue(
            any(c["type"] == "required_field_removed" and c["severity"] == "CRITICAL"
                for c in diff["breaking_changes"])
        )

    def test_optional_field_removed_is_high_severity(self) -> None:
        baseline = _contract([_table("docs", [_field("extra", type="string", required=False)])])
        current = _contract([_table("docs", [])])
        diff = diff_schemas(baseline, current)
        self.assertTrue(
            any(c["severity"] == "HIGH" and c["type"] == "optional_field_removed"
                for c in diff["breaking_changes"])
        )

    def test_required_field_added_is_critical(self) -> None:
        baseline = _contract([_table("docs", [])])
        current = _contract([_table("docs", [_field("mandatory_new", type="string", required=True)])])
        diff = diff_schemas(baseline, current)
        self.assertEqual("breaking", diff["verdict"])
        self.assertTrue(
            any(c["type"] == "required_field_added" for c in diff["breaking_changes"])
        )

    def test_nullable_field_added_is_compatible(self) -> None:
        baseline = _contract([_table("docs", [_field("id", type="string")])])
        current = _contract([_table("docs", [
            _field("id", type="string"),
            _field("optional_new", type="string"),
        ])])
        diff = diff_schemas(baseline, current)
        self.assertEqual("compatible", diff["verdict"])
        self.assertTrue(
            any(c["type"] == "nullable_field_added" for c in diff["compatible_changes"])
        )

    def test_type_change_is_breaking(self) -> None:
        baseline = _contract([_table("docs", [_field("score", type="number")])])
        current = _contract([_table("docs", [_field("score", type="integer")])])
        diff = diff_schemas(baseline, current)
        self.assertEqual("breaking", diff["verdict"])

    def test_diff_result_has_required_keys(self) -> None:
        schema = _contract([])
        diff = diff_schemas(schema, schema)
        for key in ("verdict", "breaking_changes", "compatible_changes", "total_breaking", "total_compatible", "timestamp"):
            self.assertIn(key, diff)


if __name__ == "__main__":
    unittest.main()
