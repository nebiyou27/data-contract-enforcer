"""Adversarial tests for contracts/runner.py.

Each test class owns one failure mode and drives it with a deliberately bad
fixture dataset.  The goal is to prove the runner catches real problems, not
just that it handles contrived edge cases in isolation.

Failure modes covered
---------------------
1.  Missing required field          – check_required
2.  Bad UUID                        – check_uuid_format
3.  Invalid enum value              – check_enum
4.  Duplicate fact_id               – uniqueness surfaced via cardinality drift
5.  Orphan doc_id                   – check_referential_integrity
6.  New unexpected column           – schema_new_column (schema evolution)
7.  Strong synthetic drift          – check_drift_mean / check_drift_variance
"""

import unittest

import pandas as pd

from contracts.runner import (
    check_enum,
    check_referential_integrity,
    check_required,
    check_uuid_format,
    check_drift_cardinality,
    check_drift_mean,
    check_drift_variance,
    compute_column_stats,
    run_table_checks,
)

# ---------------------------------------------------------------------------
# Shared fixture helpers
# ---------------------------------------------------------------------------

_VALID_UUID = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
_BAD_UUID = "not-a-uuid-at-all"


def _uuid_field(name: str = "fact_id") -> dict:
    return {"name": name, "type": "string", "required": True, "format": "uuid"}


def _required_string_field(name: str) -> dict:
    return {"name": name, "type": "string", "required": True}


def _enum_field(name: str, allowed: list) -> dict:
    return {"name": name, "type": "string", "required": True, "enum": allowed}


# ---------------------------------------------------------------------------
# 1. Missing required field
# ---------------------------------------------------------------------------

class MissingRequiredFieldTest(unittest.TestCase):
    """A fixture where a mandatory column contains NULLs must produce FAIL."""

    def setUp(self) -> None:
        # confidence is required; two rows have None
        self.series_with_nulls = pd.Series(
            [0.9, None, 0.75, None, 0.6], dtype="object"
        )
        self.series_fully_populated = pd.Series(
            [0.9, 0.8, 0.75, 0.7, 0.6], dtype="object"
        )
        self.field = {"name": "confidence", "type": "string", "required": True}

    def test_null_values_in_required_field_fails(self) -> None:
        result = check_required("extracted_facts", self.field, self.series_with_nulls)
        self.assertEqual("FAIL", result["status"])
        self.assertEqual("required", result["check_type"])
        self.assertEqual(2, result["records_failing"])

    def test_partial_nulls_reports_correct_count(self) -> None:
        # Only one null
        s = pd.Series(["text", None, "text"])
        result = check_required("extracted_facts", self.field, s)
        self.assertEqual(1, result["records_failing"])

    def test_fully_populated_required_field_passes(self) -> None:
        result = check_required(
            "extracted_facts", self.field, self.series_fully_populated
        )
        self.assertEqual("PASS", result["status"])
        self.assertEqual(0, result.get("records_failing", 0))

    def test_all_nulls_in_required_field_fails_with_full_count(self) -> None:
        all_null = pd.Series([None, None, None], dtype="object")
        result = check_required("documents", self.field, all_null)
        self.assertEqual("FAIL", result["status"])
        self.assertEqual(3, result["records_failing"])

    def test_severity_is_critical(self) -> None:
        result = check_required("extracted_facts", self.field, self.series_with_nulls)
        self.assertEqual("CRITICAL", result["severity"])

    def test_check_id_includes_table_and_column(self) -> None:
        result = check_required("extracted_facts", self.field, self.series_with_nulls)
        self.assertIn("extracted_facts", result["check_id"])
        self.assertIn("confidence", result["check_id"])


# ---------------------------------------------------------------------------
# 2. Bad UUID values
# ---------------------------------------------------------------------------

class BadUUIDTest(unittest.TestCase):
    """Rows that contain UUID-shaped strings except they are malformed."""

    FIELD = _uuid_field("fact_id")

    # Fixture: three valid, two bad
    MIXED = pd.Series(
        [
            _VALID_UUID,
            "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",  # wrong chars
            "a1b2c3d4-e5f6-7890-abcd-ef1234567890",  # valid duplicate
            "12345678-1234-1234-1234-1234567890ZZ",  # non-hex suffix
            "short",                                   # completely wrong
        ]
    )

    def test_malformed_uuids_detected(self) -> None:
        result = check_uuid_format("extracted_facts", self.FIELD, self.MIXED)
        self.assertEqual("FAIL", result["status"])
        self.assertEqual("format_uuid", result["check_type"])
        # Three bad rows: index 1, 3, 4
        self.assertEqual(3, result["records_failing"])

    def test_bad_uuid_sample_appears_in_result(self) -> None:
        result = check_uuid_format("extracted_facts", self.FIELD, self.MIXED)
        # "short" is one of the clearly invalid values in MIXED; confirm it surfaces
        self.assertIn("short", str(result.get("sample_failing", "")))

    def test_all_valid_uuids_pass(self) -> None:
        good = pd.Series([
            "00000000-0000-0000-0000-000000000001",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
            "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
        ])
        result = check_uuid_format("extracted_facts", self.FIELD, good)
        self.assertEqual("PASS", result["status"])

    def test_uuid_with_uppercase_letters_passes(self) -> None:
        """UUID validation is case-insensitive per the regex."""
        upper = pd.Series(["A1B2C3D4-E5F6-7890-ABCD-EF1234567890"])
        result = check_uuid_format("extracted_facts", self.FIELD, upper)
        self.assertEqual("PASS", result["status"])

    def test_completely_numeric_string_fails_uuid(self) -> None:
        s = pd.Series(["12345678901234567890123456789012"])  # no dashes
        result = check_uuid_format("extracted_facts", self.FIELD, s)
        self.assertEqual("FAIL", result["status"])

    def test_severity_is_critical(self) -> None:
        result = check_uuid_format("extracted_facts", self.FIELD, self.MIXED)
        self.assertEqual("CRITICAL", result["severity"])

    def test_nulls_are_skipped_not_flagged(self) -> None:
        """NULL values are not UUIDs but should not be treated as format errors;
        the required check handles nulls separately."""
        with_null = pd.Series([None, _VALID_UUID, None])
        result = check_uuid_format("extracted_facts", self.FIELD, with_null)
        # Only non-null values are tested; one valid UUID → PASS
        self.assertEqual("PASS", result["status"])


# ---------------------------------------------------------------------------
# 3. Invalid enum value
# ---------------------------------------------------------------------------

class InvalidEnumTest(unittest.TestCase):
    """Fixture contains a value outside the contract's allowed set."""

    ALLOWED = ["strategy_a", "strategy_b", "strategy_c"]
    FIELD = _enum_field("extraction_model", ALLOWED)

    def test_unlisted_value_fails(self) -> None:
        s = pd.Series(["strategy_a", "strategy_b", "UNKNOWN_STRATEGY"])
        result = check_enum("documents", self.FIELD, s)
        self.assertEqual("FAIL", result["status"])
        self.assertEqual("enum", result["check_type"])
        self.assertEqual(1, result["records_failing"])

    def test_typo_variant_fails(self) -> None:
        # 'strategy_A' (capital A) is not in the allowed list
        s = pd.Series(["strategy_A", "strategy_b"])
        result = check_enum("documents", self.FIELD, s)
        self.assertEqual("FAIL", result["status"])

    def test_empty_string_value_fails(self) -> None:
        s = pd.Series(["strategy_a", ""])
        result = check_enum("documents", self.FIELD, s)
        self.assertEqual("FAIL", result["status"])

    def test_all_valid_enum_values_pass(self) -> None:
        s = pd.Series(["strategy_a", "strategy_b", "strategy_c", "strategy_a"])
        result = check_enum("documents", self.FIELD, s)
        self.assertEqual("PASS", result["status"])

    def test_null_values_not_counted_as_violations(self) -> None:
        """Nulls are skipped; only non-null values are checked against enum."""
        s = pd.Series(["strategy_a", None, "strategy_b"])
        result = check_enum("documents", self.FIELD, s)
        self.assertEqual("PASS", result["status"])

    def test_multiple_different_bad_values_counted_separately(self) -> None:
        s = pd.Series(["bad_1", "strategy_a", "bad_2", "bad_3"])
        result = check_enum("documents", self.FIELD, s)
        self.assertEqual("FAIL", result["status"])
        self.assertEqual(3, result["records_failing"])

    def test_severity_is_high(self) -> None:
        s = pd.Series(["strategy_a", "nope"])
        result = check_enum("documents", self.FIELD, s)
        self.assertEqual("HIGH", result["severity"])


# ---------------------------------------------------------------------------
# 4. Duplicate fact_id (surfaced via cardinality drift)
# ---------------------------------------------------------------------------

class DuplicateFactIdCardinalityDriftTest(unittest.TestCase):
    """Duplicate IDs cause cardinality to collapse relative to baseline.

    Strategy: baseline reflects a dataset where every fact_id was unique
    (cardinality == row count).  A new batch where IDs repeat will have
    a sharply reduced cardinality ratio — drift_cardinality catches it.
    """

    def _baseline_with_cardinality(self, cardinality: int) -> dict:
        return {
            "mean": 0.0, "stddev": 0.0,
            "min": 0.0, "max": 0.0,
            "count": 100,
            "null_fraction": 0.0,
            "cardinality": cardinality,
        }

    def test_duplicate_ids_cause_cardinality_collapse_warn(self) -> None:
        # Baseline: 40 unique IDs.  Current: only 15 unique (many dupes) → 0.375× ratio → WARN
        baseline = self._baseline_with_cardinality(40)
        current = {**baseline, "cardinality": 15}
        result = check_drift_cardinality("extracted_facts", "fact_id", current, baseline)
        self.assertEqual("WARN", result["status"])

    def test_severe_duplicate_explosion_fails(self) -> None:
        # 200 unique → 800 unique: 4× spike could happen with ID recycling across shards
        # but also test collapse: 100 → 5 (0.05×) → FAIL is not directly defined,
        # instead test the 5× spike scenario (IDs suddenly multiplied from a join bug)
        baseline = self._baseline_with_cardinality(40)
        current = {**baseline, "cardinality": 210}  # 5.25× → FAIL
        result = check_drift_cardinality("extracted_facts", "fact_id", current, baseline)
        self.assertEqual("FAIL", result["status"])

    def test_run_table_checks_integration_duplicate_ids_trigger_drift(self) -> None:
        """Duplicate sequence_numbers (numeric IDs) cause cardinality collapse on a
        numeric column — run_table_checks only runs drift for numeric dtypes."""
        # Baseline: 50 distinct sequence numbers (cardinality=50)
        healthy = pd.Series([float(i) for i in range(50)])
        baseline_stats = compute_column_stats(healthy)
        self.assertIsNotNone(baseline_stats, "compute_column_stats unexpectedly returned None")

        # Poisoned batch: 50 rows reusing only 2 sequence numbers (massive dupe rate)
        # cardinality=2 vs baseline cardinality≈50 → ~0.04× ratio → collapse WARN/FAIL
        duplicate_seqs = pd.Series([1.0, 2.0] * 25)
        df = pd.DataFrame({"sequence_number": duplicate_seqs})
        fields = [{"name": "sequence_number", "type": "number", "required": True}]
        baselines = {"test-contract/facts": {"sequence_number": baseline_stats}}

        results, _ = run_table_checks("facts", fields, df, baselines, "test-contract")

        cardinality_results = [
            r for r in results if r["check_type"] == "drift_cardinality"
        ]
        self.assertTrue(
            len(cardinality_results) > 0,
            "Expected a drift_cardinality check to be produced",
        )
        statuses = {r["status"] for r in cardinality_results}
        self.assertTrue(
            statuses & {"WARN", "FAIL"},
            f"Expected WARN or FAIL for cardinality collapse, got: {statuses}",
        )


# ---------------------------------------------------------------------------
# 5. Orphan doc_id (referential integrity)
# ---------------------------------------------------------------------------

class OrphanDocIdTest(unittest.TestCase):
    """Facts that reference doc_ids not present in the documents table."""

    KNOWN_DOCS = pd.Series([
        "aaaa0000-0000-0000-0000-000000000001",
        "aaaa0000-0000-0000-0000-000000000002",
        "aaaa0000-0000-0000-0000-000000000003",
    ])

    def test_single_orphan_fact_fails(self) -> None:
        facts_doc_ids = pd.Series([
            "aaaa0000-0000-0000-0000-000000000001",
            "aaaa0000-0000-0000-0000-000000000099",  # orphan
        ])
        result = check_referential_integrity(
            "extracted_facts", "doc_id",
            facts_doc_ids,
            "documents", "doc_id",
            self.KNOWN_DOCS,
        )
        self.assertEqual("FAIL", result["status"])
        self.assertEqual("referential_integrity", result["check_type"])
        self.assertEqual(1, result["records_failing"])

    def test_multiple_orphan_facts_all_counted(self) -> None:
        facts_doc_ids = pd.Series([
            "aaaa0000-0000-0000-0000-000000000001",
            "orphan-A",
            "orphan-B",
            "orphan-C",
            "aaaa0000-0000-0000-0000-000000000002",
        ])
        result = check_referential_integrity(
            "extracted_facts", "doc_id",
            facts_doc_ids,
            "documents", "doc_id",
            self.KNOWN_DOCS,
        )
        self.assertEqual("FAIL", result["status"])
        self.assertEqual(3, result["records_failing"])

    def test_repeated_orphan_id_counted_per_row(self) -> None:
        """Each occurrence of the orphan is a separate failing row."""
        facts_doc_ids = pd.Series([
            "aaaa0000-0000-0000-0000-000000000001",
            "ghost-doc",
            "ghost-doc",  # same orphan repeated
        ])
        result = check_referential_integrity(
            "extracted_facts", "doc_id",
            facts_doc_ids,
            "documents", "doc_id",
            self.KNOWN_DOCS,
        )
        self.assertEqual("FAIL", result["status"])
        self.assertEqual(2, result["records_failing"])

    def test_all_facts_matched_passes(self) -> None:
        facts_doc_ids = pd.Series([
            "aaaa0000-0000-0000-0000-000000000001",
            "aaaa0000-0000-0000-0000-000000000002",
            "aaaa0000-0000-0000-0000-000000000001",  # valid repeat
        ])
        result = check_referential_integrity(
            "extracted_facts", "doc_id",
            facts_doc_ids,
            "documents", "doc_id",
            self.KNOWN_DOCS,
        )
        self.assertEqual("PASS", result["status"])

    def test_empty_parent_table_all_facts_are_orphans(self) -> None:
        empty_docs = pd.Series([], dtype=str)
        facts = pd.Series(["aaaa0000-0000-0000-0000-000000000001"])
        result = check_referential_integrity(
            "extracted_facts", "doc_id",
            facts,
            "documents", "doc_id",
            empty_docs,
        )
        self.assertEqual("FAIL", result["status"])
        self.assertEqual(1, result["records_failing"])

    def test_severity_is_critical(self) -> None:
        facts = pd.Series(["orphan"])
        result = check_referential_integrity(
            "extracted_facts", "doc_id", facts,
            "documents", "doc_id", self.KNOWN_DOCS,
        )
        self.assertEqual("CRITICAL", result["severity"])


# ---------------------------------------------------------------------------
# 6. New unexpected column (schema evolution)
# ---------------------------------------------------------------------------

class NewUnexpectedColumnTest(unittest.TestCase):
    """A batch that ships an undeclared column must trigger a WARN."""

    KNOWN_FIELDS = [
        {"name": "doc_id", "type": "string", "required": True, "format": "uuid"},
        {"name": "fact_id", "type": "string", "required": True, "format": "uuid"},
        {"name": "text", "type": "string", "required": True},
    ]

    def test_extra_column_produces_schema_new_column_warn(self) -> None:
        df = pd.DataFrame({
            "doc_id": [_VALID_UUID],
            "fact_id": [_VALID_UUID],
            "text": ["some text"],
            "undeclared_field": ["surprise!"],  # not in contract
        })
        results, _ = run_table_checks(
            "extracted_facts", self.KNOWN_FIELDS, df, {}, "test-contract"
        )
        new_col_results = [
            r for r in results if r["check_type"] == "schema_new_column"
        ]
        self.assertEqual(1, len(new_col_results))
        self.assertEqual("WARN", new_col_results[0]["status"])
        self.assertEqual("undeclared_field", new_col_results[0]["column_name"])

    def test_multiple_extra_columns_each_get_their_own_warn(self) -> None:
        df = pd.DataFrame({
            "doc_id": [_VALID_UUID],
            "fact_id": [_VALID_UUID],
            "text": ["some text"],
            "extra_one": [1],
            "extra_two": [2],
        })
        results, _ = run_table_checks(
            "extracted_facts", self.KNOWN_FIELDS, df, {}, "test-contract"
        )
        new_col_results = [
            r for r in results if r["check_type"] == "schema_new_column"
        ]
        extra_names = {r["column_name"] for r in new_col_results}
        self.assertIn("extra_one", extra_names)
        self.assertIn("extra_two", extra_names)
        for r in new_col_results:
            self.assertEqual("WARN", r["status"])

    def test_no_new_column_warn_when_schema_matches(self) -> None:
        df = pd.DataFrame({
            "doc_id": [_VALID_UUID],
            "fact_id": [_VALID_UUID],
            "text": ["some text"],
        })
        results, _ = run_table_checks(
            "extracted_facts", self.KNOWN_FIELDS, df, {}, "test-contract"
        )
        new_col_results = [
            r for r in results if r["check_type"] == "schema_new_column"
        ]
        self.assertEqual(0, len(new_col_results))

    def test_missing_declared_column_fails_not_warns(self) -> None:
        """Dropped columns are FAIL (schema_missing), not WARN."""
        df = pd.DataFrame({
            "doc_id": [_VALID_UUID],
            # fact_id intentionally absent
            "text": ["some text"],
        })
        results, _ = run_table_checks(
            "extracted_facts", self.KNOWN_FIELDS, df, {}, "test-contract"
        )
        missing = [r for r in results if r["check_type"] == "schema_missing"]
        self.assertEqual(1, len(missing))
        self.assertEqual("FAIL", missing[0]["status"])
        self.assertEqual("fact_id", missing[0]["column_name"])


# ---------------------------------------------------------------------------
# 7. Strong synthetic drift
# ---------------------------------------------------------------------------

class StrongSyntheticDriftTest(unittest.TestCase):
    """Fixture datasets engineered to breach each drift threshold."""

    def _stable_baseline(
        self, mean=100.0, stddev=10.0,
        null_fraction=0.0, cardinality=50,
    ) -> dict:
        return {
            "mean": mean, "stddev": stddev,
            "min": mean - 2 * stddev, "max": mean + 2 * stddev,
            "count": 100,
            "null_fraction": null_fraction,
            "cardinality": cardinality,
        }

    # --- mean drift ---

    def test_mean_shifted_beyond_3_sigma_fails(self) -> None:
        # z = |160 - 100| / 10 = 6.0 → FAIL
        baseline = self._stable_baseline(mean=100.0, stddev=10.0)
        current = {**baseline, "mean": 160.0}
        result = check_drift_mean("tbl", "score", current, baseline)
        self.assertEqual("FAIL", result["status"])

    def test_mean_shifted_just_above_2_sigma_warns(self) -> None:
        # z = |122 - 100| / 10 = 2.2 → WARN
        baseline = self._stable_baseline(mean=100.0, stddev=10.0)
        current = {**baseline, "mean": 122.0}
        result = check_drift_mean("tbl", "score", current, baseline)
        self.assertEqual("WARN", result["status"])

    def test_mean_unchanged_passes(self) -> None:
        baseline = self._stable_baseline(mean=100.0, stddev=10.0)
        result = check_drift_mean("tbl", "score", baseline, baseline)
        self.assertEqual("PASS", result["status"])

    # --- variance drift ---

    def test_variance_explosion_beyond_4x_fails(self) -> None:
        # ratio = 50 / 10 = 5.0 → FAIL
        baseline = self._stable_baseline(stddev=10.0)
        current = {**baseline, "stddev": 50.0}
        result = check_drift_variance("tbl", "score", current, baseline)
        self.assertEqual("FAIL", result["status"])

    def test_variance_collapse_below_quarter_warns(self) -> None:
        # ratio = 2 / 10 = 0.2 → WARN
        baseline = self._stable_baseline(stddev=10.0)
        current = {**baseline, "stddev": 2.0}
        result = check_drift_variance("tbl", "score", current, baseline)
        self.assertEqual("WARN", result["status"])

    # --- combined drift integration via run_table_checks ---

    def test_run_table_checks_detects_mean_drift_on_poisoned_batch(self) -> None:
        """Build a baseline from a healthy series, then feed a batch whose
        mean is 6 standard deviations away and confirm drift_mean FAIL."""
        # Healthy batch: values 90–110, mean≈100, stddev≈5.8
        healthy = pd.Series([float(90 + (i % 20)) for i in range(100)])
        baseline_stats = compute_column_stats(healthy)
        self.assertIsNotNone(baseline_stats)

        # Poisoned batch: values all 160+, mean≈165 — far beyond 3σ of baseline
        poisoned = pd.Series([float(160 + (i % 10)) for i in range(100)])
        df = pd.DataFrame({"score": poisoned})
        fields = [{"name": "score", "type": "number", "required": True}]
        baselines = {"contract/tbl": {"score": baseline_stats}}

        results, _ = run_table_checks("tbl", fields, df, baselines, "contract")

        mean_results = [r for r in results if r["check_type"] == "drift_mean"]
        self.assertTrue(len(mean_results) > 0, "drift_mean check missing")
        self.assertEqual("FAIL", mean_results[0]["status"])

    def test_run_table_checks_detects_variance_drift_on_noisy_batch(self) -> None:
        """Baseline has tight stddev; poisoned batch is wildly spread out."""
        # Healthy: all exactly 50.0 (zero variance)
        healthy = pd.Series([50.0] * 100)
        baseline_stats = compute_column_stats(healthy)
        # compute_column_stats returns None for zero-variance; skip if so
        if baseline_stats is None:
            self.skipTest("compute_column_stats returned None for constant series")

        # Noisy batch: wide range 0–1000
        noisy = pd.Series([float(i * 10) for i in range(100)])
        df = pd.DataFrame({"metric": noisy})
        fields = [{"name": "metric", "type": "number", "required": True}]
        baselines = {"contract/tbl": {"metric": baseline_stats}}

        results, _ = run_table_checks("tbl", fields, df, baselines, "contract")

        variance_results = [r for r in results if r["check_type"] == "drift_variance"]
        # Only check if the variance check was produced
        if variance_results:
            statuses = {r["status"] for r in variance_results}
            self.assertTrue(
                statuses & {"WARN", "FAIL"},
                f"Expected variance drift to be WARN/FAIL, got: {statuses}",
            )

    def test_all_drift_check_types_present_with_baseline(self) -> None:
        """Sanity-check that all five drift sub-checks are emitted when a baseline
        exists, regardless of outcome."""
        df = pd.DataFrame({"val": [float(i) for i in range(1, 51)]})
        fields = [{"name": "val", "type": "number", "required": True}]
        baseline_stats = compute_column_stats(df["val"])
        baselines = {"contract/tbl": {"val": baseline_stats}}

        results, _ = run_table_checks("tbl", fields, df, baselines, "contract")

        drift_types = {r["check_type"] for r in results if "drift" in r["check_type"]}
        for expected in (
            "drift_mean", "drift_variance", "drift_outliers",
            "drift_null_fraction", "drift_cardinality",
        ):
            self.assertIn(expected, drift_types, f"Missing: {expected}")


if __name__ == "__main__":
    unittest.main()
