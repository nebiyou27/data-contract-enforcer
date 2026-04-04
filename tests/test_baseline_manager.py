"""
tests/test_baseline_manager.py

Unit tests for contracts/baseline_manager.py.

Coverage targets:
  - _extract_stats_from_report  (stat parsing — the most fragile path)
  - cmd_promote                 (file I/O + merging logic)
  - cmd_clear                   (contract/all variants + safety guard)
  - cmd_list                    (empty + populated baselines)
  - main()                      (CLI dispatch smoke test)

File I/O (_load_baselines / _save_baselines) is patched throughout so the
tests never touch the real schema_snapshots/baselines.json.
"""

from __future__ import annotations

import json
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest.mock import patch

import contracts.baseline_manager as bm
from contracts.baseline_manager import (
    _extract_stats_from_report,
    cmd_clear,
    cmd_list,
    cmd_promote,
    main,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_result(
    check_type: str,
    check_id: str,
    col: str,
    actual_value: str = "",
    expected: str = "",
) -> dict:
    return {
        "check_type": check_type,
        "check_id": check_id,
        "column_name": col,
        "actual_value": actual_value,
        "expected": expected,
        "status": "PASS",
    }


def _report_with_results(results: list, contract_id: str = "week3-contract") -> dict:
    return {"contract_id": contract_id, "results": results}


# ---------------------------------------------------------------------------
# _extract_stats_from_report
# ---------------------------------------------------------------------------

class ExtractStatsDriftMeanTest(unittest.TestCase):
    """drift_mean results carry mean and optional stddev."""

    def test_extracts_mean_from_actual_value(self) -> None:
        r = _make_result(
            "drift_mean",
            "documents.confidence.drift_mean",
            "confidence",
            actual_value="mean=0.73 (z=1.2)",
        )
        stats = _extract_stats_from_report(_report_with_results([r]))
        col = stats["week3-contract/documents"]["confidence"]
        self.assertAlmostEqual(0.73, col["mean"])

    def test_extracts_stddev_from_expected_field(self) -> None:
        r = _make_result(
            "drift_mean",
            "documents.confidence.drift_mean",
            "confidence",
            actual_value="mean=0.73 (z=1.2)",
            expected="baseline mean=0.70 ± 0.05",
        )
        stats = _extract_stats_from_report(_report_with_results([r]))
        col = stats["week3-contract/documents"]["confidence"]
        self.assertAlmostEqual(0.05, col["stddev"])
        self.assertAlmostEqual(0.70, col["mean_baseline"])

    def test_malformed_mean_does_not_crash(self) -> None:
        r = _make_result(
            "drift_mean",
            "documents.confidence.drift_mean",
            "confidence",
            actual_value="GARBAGE",
        )
        # Should not raise; just produces no mean entry
        stats = _extract_stats_from_report(_report_with_results([r]))
        col = stats.get("week3-contract/documents", {}).get("confidence", {})
        self.assertNotIn("mean", col)

    def test_malformed_expected_skips_stddev(self) -> None:
        r = _make_result(
            "drift_mean",
            "documents.confidence.drift_mean",
            "confidence",
            actual_value="mean=0.73 (z=0.5)",
            expected="no pm sign here",
        )
        stats = _extract_stats_from_report(_report_with_results([r]))
        col = stats["week3-contract/documents"]["confidence"]
        self.assertAlmostEqual(0.73, col["mean"])
        self.assertNotIn("stddev", col)


class ExtractStatsDriftNullTest(unittest.TestCase):
    def test_extracts_null_fraction(self) -> None:
        r = _make_result(
            "drift_null_fraction",
            "documents.confidence.drift_null_fraction",
            "confidence",
            actual_value="null_frac=0.02 (z=0.4)",
        )
        stats = _extract_stats_from_report(_report_with_results([r]))
        col = stats["week3-contract/documents"]["confidence"]
        self.assertAlmostEqual(0.02, col["null_fraction"])

    def test_malformed_null_frac_does_not_crash(self) -> None:
        r = _make_result(
            "drift_null_fraction",
            "documents.confidence.drift_null_fraction",
            "confidence",
            actual_value="BAD",
        )
        stats = _extract_stats_from_report(_report_with_results([r]))
        col = stats.get("week3-contract/documents", {}).get("confidence", {})
        self.assertNotIn("null_fraction", col)


class ExtractStatsDriftCardinalityTest(unittest.TestCase):
    def test_extracts_cardinality(self) -> None:
        r = _make_result(
            "drift_cardinality",
            "documents.status.drift_cardinality",
            "status",
            actual_value="cardinality=5 (z=0.1)",
        )
        stats = _extract_stats_from_report(_report_with_results([r]))
        col = stats["week3-contract/documents"]["status"]
        self.assertEqual(5, col["cardinality"])

    def test_malformed_cardinality_does_not_crash(self) -> None:
        r = _make_result(
            "drift_cardinality",
            "documents.status.drift_cardinality",
            "status",
            actual_value="BROKEN",
        )
        stats = _extract_stats_from_report(_report_with_results([r]))
        col = stats.get("week3-contract/documents", {}).get("status", {})
        self.assertNotIn("cardinality", col)


class ExtractStatsEdgeCasesTest(unittest.TestCase):
    def test_empty_results_returns_empty(self) -> None:
        stats = _extract_stats_from_report(_report_with_results([]))
        self.assertEqual({}, stats)

    def test_check_id_with_fewer_than_three_parts_is_skipped(self) -> None:
        r = _make_result(
            "drift_mean", "only_two_parts", "confidence", actual_value="mean=0.5 (z=0)"
        )
        stats = _extract_stats_from_report(_report_with_results([r]))
        self.assertEqual({}, stats)

    def test_missing_column_name_is_skipped(self) -> None:
        result = {
            "check_type": "drift_mean",
            "check_id": "documents.confidence.drift_mean",
            "column_name": "",          # empty → skip
            "actual_value": "mean=0.5 (z=0)",
            "expected": "",
        }
        stats = _extract_stats_from_report(_report_with_results([result]))
        self.assertEqual({}, stats)

    def test_non_drift_check_type_is_ignored(self) -> None:
        r = _make_result(
            "required",
            "documents.confidence.required",
            "confidence",
            actual_value="ok",
        )
        stats = _extract_stats_from_report(_report_with_results([r]))
        self.assertEqual({}, stats)

    def test_multiple_columns_harvested(self) -> None:
        results = [
            _make_result(
                "drift_mean", "docs.col_a.drift_mean", "col_a",
                actual_value="mean=1.0 (z=0)"
            ),
            _make_result(
                "drift_mean", "docs.col_b.drift_mean", "col_b",
                actual_value="mean=2.0 (z=0)"
            ),
        ]
        stats = _extract_stats_from_report(_report_with_results(results))
        self.assertIn("col_a", stats["week3-contract/docs"])
        self.assertIn("col_b", stats["week3-contract/docs"])

    def test_table_key_uses_contract_id_and_table(self) -> None:
        r = _make_result(
            "drift_mean", "mytable.col.drift_mean", "col",
            actual_value="mean=0.5 (z=0)"
        )
        stats = _extract_stats_from_report(
            _report_with_results([r], contract_id="my-contract")
        )
        self.assertIn("my-contract/mytable", stats)


# ---------------------------------------------------------------------------
# cmd_promote
# ---------------------------------------------------------------------------

SAMPLE_REPORT = {
    "contract_id": "week3-document-refinery-extractions",
    "snapshot_id": "abc123def456",
    "results": [
        {
            "check_type": "drift_mean",
            "check_id": "documents.confidence.drift_mean",
            "column_name": "confidence",
            "actual_value": "mean=0.73 (z=1.2)",
            "expected": "baseline mean=0.70 ± 0.05",
            "status": "PASS",
        }
    ],
}


class CmdPromoteTest(unittest.TestCase):
    def _write_report(self, data: dict) -> Path:
        fh = tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, encoding="utf-8"
        )
        json.dump(data, fh)
        fh.close()
        return Path(fh.name)

    def test_nonexistent_report_returns_exit_1(self) -> None:
        args = Namespace(report="/no/such/file.json")
        code = cmd_promote(args)
        self.assertEqual(1, code)

    def test_report_with_no_drift_checks_returns_exit_1(self) -> None:
        empty_report = {
            "contract_id": "week3-contract",
            "snapshot_id": "snap001",
            "results": [
                {
                    "check_type": "required",
                    "check_id": "documents.doc_id.required",
                    "column_name": "doc_id",
                    "actual_value": "ok",
                    "expected": "non-null",
                }
            ],
        }
        path = self._write_report(empty_report)
        try:
            with patch.object(bm, "_load_baselines", return_value={}), \
                 patch.object(bm, "_save_baselines") as mock_save:
                code = cmd_promote(Namespace(report=str(path)))
            self.assertEqual(1, code)
            mock_save.assert_not_called()
        finally:
            path.unlink(missing_ok=True)

    def test_successful_promote_returns_exit_0(self) -> None:
        path = self._write_report(SAMPLE_REPORT)
        saved = {}
        try:
            with patch.object(bm, "_load_baselines", return_value={}), \
                 patch.object(bm, "_save_baselines", side_effect=lambda d: saved.update(d)):
                code = cmd_promote(Namespace(report=str(path)))
            self.assertEqual(0, code)
        finally:
            path.unlink(missing_ok=True)

    def test_promote_writes_correct_stats(self) -> None:
        path = self._write_report(SAMPLE_REPORT)
        saved = {}
        try:
            with patch.object(bm, "_load_baselines", return_value={}), \
                 patch.object(bm, "_save_baselines", side_effect=lambda d: saved.update(d)):
                cmd_promote(Namespace(report=str(path)))
            key = "week3-document-refinery-extractions/documents"
            self.assertIn(key, saved)
            self.assertAlmostEqual(0.73, saved[key]["confidence"]["mean"])
        finally:
            path.unlink(missing_ok=True)

    def test_promote_merges_with_existing_baselines(self) -> None:
        existing = {
            "week3-document-refinery-extractions/documents": {
                "other_col": {"mean": 5.0}
            }
        }
        path = self._write_report(SAMPLE_REPORT)
        saved = {}
        try:
            with patch.object(bm, "_load_baselines", return_value=existing), \
                 patch.object(bm, "_save_baselines", side_effect=lambda d: saved.update(d)):
                cmd_promote(Namespace(report=str(path)))
            key = "week3-document-refinery-extractions/documents"
            # Both old and new columns should be present
            self.assertIn("other_col", saved[key])
            self.assertIn("confidence", saved[key])
        finally:
            path.unlink(missing_ok=True)

    def test_promote_does_not_wipe_unrelated_contracts(self) -> None:
        existing = {"other-contract/table": {"col": {"mean": 1.0}}}
        path = self._write_report(SAMPLE_REPORT)
        saved = {}
        try:
            with patch.object(bm, "_load_baselines", return_value=existing), \
                 patch.object(bm, "_save_baselines", side_effect=lambda d: saved.update(d)):
                cmd_promote(Namespace(report=str(path)))
            self.assertIn("other-contract/table", saved)
        finally:
            path.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# cmd_clear
# ---------------------------------------------------------------------------

SAMPLE_BASELINES = {
    "week3-contract/documents": {"col_a": {"mean": 1.0}},
    "week3-contract/facts": {"col_b": {"mean": 2.0}},
    "week4-contract/nodes": {"col_c": {"mean": 3.0}},
}


class CmdClearAllTest(unittest.TestCase):
    def test_all_without_yes_returns_exit_1(self) -> None:
        args = Namespace(all=True, yes=False, contract=None)
        with patch.object(bm, "_load_baselines", return_value=dict(SAMPLE_BASELINES)), \
             patch.object(bm, "_save_baselines") as mock_save:
            code = cmd_clear(args)
        self.assertEqual(1, code)
        mock_save.assert_not_called()

    def test_all_with_yes_clears_everything(self) -> None:
        args = Namespace(all=True, yes=True, contract=None)
        saved = {"_": True}  # sentinel
        with patch.object(bm, "_load_baselines", return_value=dict(SAMPLE_BASELINES)), \
             patch.object(bm, "_save_baselines", side_effect=lambda d: saved.clear() or saved.update(d)):
            code = cmd_clear(args)
        self.assertEqual(0, code)
        self.assertEqual({}, saved)

    def test_empty_baselines_all_returns_exit_0(self) -> None:
        args = Namespace(all=True, yes=True, contract=None)
        with patch.object(bm, "_load_baselines", return_value={}), \
             patch.object(bm, "_save_baselines"):
            code = cmd_clear(args)
        self.assertEqual(0, code)


class CmdClearContractTest(unittest.TestCase):
    def _clear_contract(self, contract_id: str) -> tuple[int, dict]:
        args = Namespace(all=False, yes=False, contract=contract_id)
        saved = {}
        with patch.object(bm, "_load_baselines", return_value=dict(SAMPLE_BASELINES)), \
             patch.object(bm, "_save_baselines", side_effect=lambda d: saved.update(d)):
            code = cmd_clear(args)
        return code, saved

    def test_clears_all_keys_for_contract(self) -> None:
        code, saved = self._clear_contract("week3-contract")
        self.assertEqual(0, code)
        self.assertNotIn("week3-contract/documents", saved)
        self.assertNotIn("week3-contract/facts", saved)

    def test_leaves_other_contracts_intact(self) -> None:
        _, saved = self._clear_contract("week3-contract")
        self.assertIn("week4-contract/nodes", saved)

    def test_unknown_contract_returns_exit_0_noop(self) -> None:
        args = Namespace(all=False, yes=False, contract="nonexistent-contract")
        with patch.object(bm, "_load_baselines", return_value=dict(SAMPLE_BASELINES)), \
             patch.object(bm, "_save_baselines") as mock_save:
            code = cmd_clear(args)
        self.assertEqual(0, code)
        mock_save.assert_not_called()

    def test_no_contract_no_all_returns_exit_1(self) -> None:
        args = Namespace(all=False, yes=False, contract=None)
        with patch.object(bm, "_load_baselines", return_value=dict(SAMPLE_BASELINES)), \
             patch.object(bm, "_save_baselines") as mock_save:
            code = cmd_clear(args)
        self.assertEqual(1, code)
        mock_save.assert_not_called()


# ---------------------------------------------------------------------------
# cmd_list
# ---------------------------------------------------------------------------

class CmdListTest(unittest.TestCase):
    def test_empty_baselines_returns_exit_0(self) -> None:
        with patch.object(bm, "_load_baselines", return_value={}):
            code = cmd_list(Namespace())
        self.assertEqual(0, code)

    def test_populated_baselines_returns_exit_0(self) -> None:
        with patch.object(bm, "_load_baselines", return_value=SAMPLE_BASELINES):
            code = cmd_list(Namespace())
        self.assertEqual(0, code)


# ---------------------------------------------------------------------------
# main() — CLI dispatch smoke tests
# ---------------------------------------------------------------------------

class MainDispatchTest(unittest.TestCase):
    def test_list_subcommand_dispatches(self) -> None:
        with patch.object(bm, "_load_baselines", return_value={}):
            code = main(["list"])
        self.assertEqual(0, code)

    def test_clear_all_requires_yes(self) -> None:
        with patch.object(bm, "_load_baselines", return_value={"k": {}}):
            code = main(["clear", "--all"])
        self.assertEqual(1, code)

    def test_promote_missing_file_returns_1(self) -> None:
        code = main(["promote", "--report", "/no/such/file.json"])
        self.assertEqual(1, code)


if __name__ == "__main__":
    unittest.main()
