import json
import tempfile
import unittest
from pathlib import Path

from contracts.report_generator import (
    aggregate_ai_results,
    aggregate_schema_evolution,
    aggregate_validation_results,
    compute_data_health_score,
    count_violations,
    generate_report,
)


def _report(total=10, passed=10, failed=0, warned=0, errored=0, contract_id="c1", results=None):
    return {
        "contract_id": contract_id,
        "total_checks": total,
        "passed": passed,
        "failed": failed,
        "warned": warned,
        "errored": errored,
        "results": results or [],
    }


# ---------------------------------------------------------------------------
# compute_data_health_score
# ---------------------------------------------------------------------------


class ComputeDataHealthScoreTest(unittest.TestCase):
    def test_perfect_score_is_100(self) -> None:
        score = compute_data_health_score([_report(total=10, passed=10)])
        self.assertEqual(100, score)

    def test_zero_checks_returns_zero(self) -> None:
        score = compute_data_health_score([_report(total=0, passed=0)])
        self.assertEqual(0, score)

    def test_empty_report_list_returns_zero(self) -> None:
        score = compute_data_health_score([])
        self.assertEqual(0, score)

    def test_critical_violation_deducts_20_points(self) -> None:
        results = [{"status": "FAIL", "severity": "CRITICAL"}]
        score = compute_data_health_score([_report(total=10, passed=9, results=results)])
        # base = 90, deduction = 20 -> 70
        self.assertEqual(70, score)

    def test_score_clamped_at_zero(self) -> None:
        results = [{"status": "FAIL", "severity": "CRITICAL"}] * 10
        score = compute_data_health_score([_report(total=10, passed=0, results=results)])
        self.assertEqual(0, score)

    def test_non_critical_fail_does_not_deduct(self) -> None:
        results = [{"status": "FAIL", "severity": "HIGH"}]
        score = compute_data_health_score([_report(total=10, passed=9, results=results)])
        # base = 90, no CRITICAL deduction
        self.assertEqual(90, score)


# ---------------------------------------------------------------------------
# aggregate_validation_results
# ---------------------------------------------------------------------------


class AggregateValidationResultsTest(unittest.TestCase):
    def test_sums_across_multiple_reports(self) -> None:
        reports = [
            _report(total=10, passed=8, failed=2, contract_id="c1"),
            _report(total=5, passed=5, failed=0, contract_id="c2"),
        ]
        agg = aggregate_validation_results(reports)
        self.assertEqual(15, agg["total_checks"])
        self.assertEqual(13, agg["total_passed"])
        self.assertEqual(2, agg["total_failed"])
        self.assertEqual(2, agg["contract_count"])

    def test_pass_rate_computed_correctly(self) -> None:
        agg = aggregate_validation_results([_report(total=4, passed=3)])
        self.assertAlmostEqual(75.0, agg["pass_rate_pct"])

    def test_zero_checks_pass_rate_is_zero(self) -> None:
        agg = aggregate_validation_results([_report(total=0, passed=0)])
        self.assertEqual(0, agg["pass_rate_pct"])

    def test_deduplicates_contract_ids(self) -> None:
        reports = [_report(contract_id="c1"), _report(contract_id="c1")]
        agg = aggregate_validation_results(reports)
        self.assertEqual(1, agg["contract_count"])


# ---------------------------------------------------------------------------
# aggregate_ai_results
# ---------------------------------------------------------------------------


class AggregateAiResultsTest(unittest.TestCase):
    def test_none_returns_zero_counts(self) -> None:
        agg = aggregate_ai_results(None)
        self.assertEqual(0, agg["checks_run"])
        self.assertEqual(0, agg["passed"])

    def test_extracts_summary_counts(self) -> None:
        ai_checks = {
            "summary": {"total_checks": 3, "passed": 2, "warned": 1, "failed": 0, "errored": 0},
            "checks": [{"check_type": "embedding_drift"}, {"check_type": "prompt_input_schema"}],
        }
        agg = aggregate_ai_results(ai_checks)
        self.assertEqual(3, agg["checks_run"])
        self.assertEqual(2, agg["passed"])
        self.assertIn("embedding_drift", agg["check_types"])


# ---------------------------------------------------------------------------
# aggregate_schema_evolution
# ---------------------------------------------------------------------------


class AggregateSchemaEvolutionTest(unittest.TestCase):
    def test_none_returns_unknown_verdict(self) -> None:
        agg = aggregate_schema_evolution(None)
        self.assertEqual("unknown", agg["verdict"])

    def test_extracts_verdict_and_counts(self) -> None:
        evo = {"verdict": "breaking", "total_breaking": 2, "total_non_breaking": 1}
        agg = aggregate_schema_evolution(evo)
        self.assertEqual("breaking", agg["verdict"])
        self.assertEqual(2, agg["breaking_changes"])


# ---------------------------------------------------------------------------
# count_violations
# ---------------------------------------------------------------------------


class CountViolationsTest(unittest.TestCase):
    def test_nonexistent_path_returns_zero(self) -> None:
        count, ids = count_violations(Path("/no/such/file.jsonl"))
        self.assertEqual(0, count)
        self.assertEqual([], ids)

    def test_counts_records_in_jsonl(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as fh:
            fh.write(json.dumps({"violation_id": "v1"}) + "\n")
            fh.write(json.dumps({"violation_id": "v2"}) + "\n")
            path = Path(fh.name)
        count, ids = count_violations(path)
        self.assertEqual(2, count)
        self.assertIn("v1", ids)
        path.unlink()

    def test_skips_invalid_json_lines(self) -> None:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as fh:
            fh.write("{invalid json\n")
            fh.write(json.dumps({"violation_id": "ok"}) + "\n")
            path = Path(fh.name)
        count, ids = count_violations(path)
        self.assertEqual(1, count)
        path.unlink()


# ---------------------------------------------------------------------------
# generate_report (integration)
# ---------------------------------------------------------------------------


class GenerateReportTest(unittest.TestCase):
    def test_all_pass_verdict_is_pass(self) -> None:
        reports = [_report(total=5, passed=5, failed=0)]
        ai = {"summary": {"total_checks": 1, "passed": 1, "warned": 0, "failed": 0, "errored": 0}, "checks": []}
        evo = {"verdict": "compatible", "total_breaking": 0, "total_non_breaking": 0}
        report = generate_report(reports, ai_checks=ai, schema_evolution=evo)
        self.assertEqual("PASS", report["verdict"])

    def test_failures_produce_issues_detected_verdict(self) -> None:
        results = [{"status": "FAIL", "severity": "HIGH"}]
        reports = [_report(total=5, passed=4, failed=1, results=results)]
        report = generate_report(reports)
        self.assertEqual("ISSUES_DETECTED", report["verdict"])

    def test_report_has_required_keys(self) -> None:
        report = generate_report([_report()])
        for key in ("data_health_score", "validation", "ai_checks", "schema_evolution", "violations", "verdict", "recommendations", "timestamp"):
            self.assertIn(key, report)

    def test_health_score_within_range(self) -> None:
        report = generate_report([_report(total=10, passed=7, failed=3)])
        self.assertGreaterEqual(report["data_health_score"], 0)
        self.assertLessEqual(report["data_health_score"], 100)


if __name__ == "__main__":
    unittest.main()
