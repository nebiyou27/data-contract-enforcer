import unittest

import pandas as pd

from contracts.runner import run_table_checks, summarize_schema_evolution


class SchemaEvolutionChecksTest(unittest.TestCase):
    def test_flags_missing_and_extra_columns_before_field_checks(self) -> None:
        df = pd.DataFrame(
            {
                "kept": ["alpha"],
                "new_field": ["beta"],
            }
        )
        fields = [
            {"name": "kept", "type": "string", "required": True},
            {"name": "missing_field", "type": "string", "required": True},
        ]

        results, stats = run_table_checks("example", fields, df, {}, "contract-a")

        schema_missing = [r for r in results if r["check_type"] == "schema_missing"]
        schema_new = [r for r in results if r["check_type"] == "schema_new_column"]

        self.assertEqual(1, len(schema_missing))
        self.assertEqual("FAIL", schema_missing[0]["status"])
        self.assertEqual("missing_field", schema_missing[0]["column_name"])

        self.assertEqual(1, len(schema_new))
        self.assertEqual("WARN", schema_new[0]["status"])
        self.assertEqual("new_field", schema_new[0]["column_name"])

        self.assertNotIn("missing_field", stats)
        self.assertFalse(any(r["check_id"] == "example.missing_field.required" for r in results))
        self.assertFalse(any(r["check_id"] == "example.missing_field.type" for r in results))

        summary = summarize_schema_evolution(results)
        self.assertEqual(
            [{"table": "example", "column": "missing_field"}],
            summary["missing_columns"],
        )
        self.assertEqual(
            [{"table": "example", "column": "new_field"}],
            summary["new_columns"],
        )

    def test_no_schema_drift_when_columns_match_contract(self) -> None:
        df = pd.DataFrame({"kept": ["alpha"]})
        fields = [{"name": "kept", "type": "string", "required": True}]

        results, _ = run_table_checks("example", fields, df, {}, "contract-a")

        self.assertFalse(any(r["check_type"] in {"schema_missing", "schema_new_column"} for r in results))


if __name__ == "__main__":
    unittest.main()

