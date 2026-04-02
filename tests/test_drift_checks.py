"""Tests for the five drift sub-checks and compute_column_stats."""
import unittest

import pandas as pd

from contracts.runner import (
    check_drift_cardinality,
    check_drift_mean,
    check_drift_null_fraction,
    check_drift_outliers,
    check_drift_variance,
    compute_column_stats,
    run_table_checks,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _baseline(
    mean=100.0, stddev=10.0, min_=80.0, max_=120.0,
    count=50, null_fraction=0.0, cardinality=40,
) -> dict:
    return {
        "mean": mean, "stddev": stddev,
        "min": min_, "max": max_,
        "count": count,
        "null_fraction": null_fraction,
        "cardinality": cardinality,
    }


def _current(**overrides) -> dict:
    base = _baseline()
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# compute_column_stats
# ---------------------------------------------------------------------------

class ComputeColumnStatsTest(unittest.TestCase):
    def test_includes_null_fraction_and_cardinality(self) -> None:
        s = pd.Series([1.0, 2.0, None, 4.0, 5.0])
        stats = compute_column_stats(s)
        self.assertIsNotNone(stats)
        self.assertIn("null_fraction", stats)
        self.assertIn("cardinality", stats)

    def test_null_fraction_correct(self) -> None:
        s = pd.Series([1.0, None, None, 4.0])   # 2 nulls out of 4
        stats = compute_column_stats(s)
        self.assertAlmostEqual(stats["null_fraction"], 0.5, places=5)

    def test_null_fraction_zero_when_no_nulls(self) -> None:
        s = pd.Series([1.0, 2.0, 3.0])
        stats = compute_column_stats(s)
        self.assertEqual(0.0, stats["null_fraction"])

    def test_cardinality_counts_unique_non_null_values(self) -> None:
        s = pd.Series([1.0, 1.0, 2.0, 3.0, None])
        stats = compute_column_stats(s)
        self.assertEqual(3, stats["cardinality"])

    def test_returns_none_for_all_null_series(self) -> None:
        s = pd.Series([None, None], dtype="float64")
        self.assertIsNone(compute_column_stats(s))

    def test_returns_none_for_single_value(self) -> None:
        s = pd.Series([42.0])
        self.assertIsNone(compute_column_stats(s))


# ---------------------------------------------------------------------------
# check_drift_mean
# ---------------------------------------------------------------------------

class DriftMeanTest(unittest.TestCase):
    def test_pass_within_2_sigma(self) -> None:
        r = check_drift_mean("t", "col", _current(mean=115.0), _baseline())
        self.assertEqual("PASS", r["status"])

    def test_warn_between_2_and_3_sigma(self) -> None:
        # z = |125 - 100| / 10 = 2.5
        r = check_drift_mean("t", "col", _current(mean=125.0), _baseline())
        self.assertEqual("WARN", r["status"])

    def test_fail_beyond_3_sigma(self) -> None:
        # z = |140 - 100| / 10 = 4.0
        r = check_drift_mean("t", "col", _current(mean=140.0), _baseline())
        self.assertEqual("FAIL", r["status"])

    def test_zero_variance_baseline_unchanged_is_pass(self) -> None:
        r = check_drift_mean("t", "col", _current(mean=5.0), _baseline(stddev=0, mean=5.0))
        self.assertEqual("PASS", r["status"])

    def test_zero_variance_baseline_changed_is_warn(self) -> None:
        r = check_drift_mean("t", "col", _current(mean=6.0), _baseline(stddev=0, mean=5.0))
        self.assertEqual("WARN", r["status"])

    def test_check_type_is_drift_mean(self) -> None:
        r = check_drift_mean("t", "col", _current(), _baseline())
        self.assertEqual("drift_mean", r["check_type"])


# ---------------------------------------------------------------------------
# check_drift_variance
# ---------------------------------------------------------------------------

class DriftVarianceTest(unittest.TestCase):
    def test_pass_within_normal_range(self) -> None:
        r = check_drift_variance("t", "col", _current(stddev=15.0), _baseline(stddev=10.0))
        self.assertEqual("PASS", r["status"])   # ratio=1.5×

    def test_warn_inflation_above_2x(self) -> None:
        r = check_drift_variance("t", "col", _current(stddev=25.0), _baseline(stddev=10.0))
        self.assertEqual("WARN", r["status"])   # ratio=2.5×

    def test_fail_explosion_above_4x(self) -> None:
        r = check_drift_variance("t", "col", _current(stddev=50.0), _baseline(stddev=10.0))
        self.assertEqual("FAIL", r["status"])   # ratio=5.0×

    def test_warn_collapse_below_0_25x(self) -> None:
        r = check_drift_variance("t", "col", _current(stddev=2.0), _baseline(stddev=10.0))
        self.assertEqual("WARN", r["status"])   # ratio=0.2×

    def test_zero_baseline_variance_new_variance_is_warn(self) -> None:
        r = check_drift_variance("t", "col", _current(stddev=1.0), _baseline(stddev=0.0))
        self.assertEqual("WARN", r["status"])

    def test_zero_baseline_variance_still_zero_is_pass(self) -> None:
        r = check_drift_variance("t", "col", _current(stddev=0.0), _baseline(stddev=0.0))
        self.assertEqual("PASS", r["status"])

    def test_returns_none_when_baseline_missing_stddev(self) -> None:
        baseline = {k: v for k, v in _baseline().items() if k != "stddev"}
        self.assertIsNone(check_drift_variance("t", "col", _current(), baseline))

    def test_check_type_is_drift_variance(self) -> None:
        r = check_drift_variance("t", "col", _current(), _baseline())
        self.assertEqual("drift_variance", r["check_type"])


# ---------------------------------------------------------------------------
# check_drift_outliers
# ---------------------------------------------------------------------------

def _with_range(min_val: float, max_val: float) -> dict:
    """Return a current-stats dict with the given observed min/max."""
    s = _current()
    s["min"] = min_val
    s["max"] = max_val
    return s


class DriftOutliersTest(unittest.TestCase):
    def test_pass_within_baseline_range(self) -> None:
        r = check_drift_outliers("t", "col", _with_range(85.0, 115.0), _baseline())
        self.assertEqual("PASS", r["status"])

    def test_warn_new_high_outlier(self) -> None:
        r = check_drift_outliers("t", "col", _with_range(80.0, 130.0), _baseline())
        self.assertEqual("WARN", r["status"])

    def test_warn_new_low_outlier(self) -> None:
        r = check_drift_outliers("t", "col", _with_range(70.0, 120.0), _baseline())
        self.assertEqual("WARN", r["status"])

    def test_fail_both_ends_breached(self) -> None:
        r = check_drift_outliers("t", "col", _with_range(60.0, 150.0), _baseline())
        self.assertEqual("FAIL", r["status"])

    def test_returns_none_when_baseline_missing_min(self) -> None:
        baseline = {k: v for k, v in _baseline().items() if k != "min"}
        self.assertIsNone(check_drift_outliers("t", "col", _current(), baseline))

    def test_check_type_is_drift_outliers(self) -> None:
        r = check_drift_outliers("t", "col", _current(), _baseline())
        self.assertEqual("drift_outliers", r["check_type"])


# ---------------------------------------------------------------------------
# check_drift_null_fraction
# ---------------------------------------------------------------------------

class DriftNullFractionTest(unittest.TestCase):
    def test_pass_stable_null_fraction(self) -> None:
        r = check_drift_null_fraction(
            "t", "col",
            _current(null_fraction=0.03),
            _baseline(null_fraction=0.02),
        )
        self.assertEqual("PASS", r["status"])   # delta=1 pp

    def test_warn_growth_above_5pp(self) -> None:
        r = check_drift_null_fraction(
            "t", "col",
            _current(null_fraction=0.10),
            _baseline(null_fraction=0.03),
        )
        self.assertEqual("WARN", r["status"])   # delta=7 pp

    def test_fail_growth_above_20pp(self) -> None:
        r = check_drift_null_fraction(
            "t", "col",
            _current(null_fraction=0.25),
            _baseline(null_fraction=0.02),
        )
        self.assertEqual("FAIL", r["status"])   # delta=23 pp

    def test_warn_nulls_appeared_on_clean_column(self) -> None:
        r = check_drift_null_fraction(
            "t", "col",
            _current(null_fraction=0.05),
            _baseline(null_fraction=0.0),
        )
        self.assertEqual("WARN", r["status"])

    def test_fail_large_nulls_appeared_on_clean_column(self) -> None:
        r = check_drift_null_fraction(
            "t", "col",
            _current(null_fraction=0.25),
            _baseline(null_fraction=0.0),
        )
        self.assertEqual("FAIL", r["status"])

    def test_returns_none_when_baseline_missing_null_fraction(self) -> None:
        baseline = {k: v for k, v in _baseline().items() if k != "null_fraction"}
        self.assertIsNone(check_drift_null_fraction("t", "col", _current(), baseline))

    def test_check_type_is_drift_null_fraction(self) -> None:
        r = check_drift_null_fraction("t", "col", _current(), _baseline())
        self.assertEqual("drift_null_fraction", r["check_type"])


# ---------------------------------------------------------------------------
# check_drift_cardinality
# ---------------------------------------------------------------------------

class DriftCardinalityTest(unittest.TestCase):
    def test_pass_stable_cardinality(self) -> None:
        r = check_drift_cardinality(
            "t", "col", _current(cardinality=45), _baseline(cardinality=40)
        )
        self.assertEqual("PASS", r["status"])   # ratio=1.125×

    def test_warn_spike_above_2x(self) -> None:
        r = check_drift_cardinality(
            "t", "col", _current(cardinality=90), _baseline(cardinality=40)
        )
        self.assertEqual("WARN", r["status"])   # ratio=2.25×

    def test_fail_explosion_above_5x(self) -> None:
        r = check_drift_cardinality(
            "t", "col", _current(cardinality=210), _baseline(cardinality=40)
        )
        self.assertEqual("FAIL", r["status"])   # ratio=5.25×

    def test_warn_collapse_below_0_5x(self) -> None:
        r = check_drift_cardinality(
            "t", "col", _current(cardinality=15), _baseline(cardinality=40)
        )
        self.assertEqual("WARN", r["status"])   # ratio=0.375×

    def test_warn_cardinality_appeared_from_empty_baseline(self) -> None:
        r = check_drift_cardinality(
            "t", "col", _current(cardinality=10), _baseline(cardinality=0)
        )
        self.assertEqual("WARN", r["status"])

    def test_pass_both_empty(self) -> None:
        r = check_drift_cardinality(
            "t", "col", _current(cardinality=0), _baseline(cardinality=0)
        )
        self.assertEqual("PASS", r["status"])

    def test_returns_none_when_baseline_missing_cardinality(self) -> None:
        baseline = {k: v for k, v in _baseline().items() if k != "cardinality"}
        self.assertIsNone(check_drift_cardinality("t", "col", _current(), baseline))

    def test_check_type_is_drift_cardinality(self) -> None:
        r = check_drift_cardinality("t", "col", _current(), _baseline())
        self.assertEqual("drift_cardinality", r["check_type"])


# ---------------------------------------------------------------------------
# Integration: run_table_checks produces all five drift check types
# ---------------------------------------------------------------------------

class DriftIntegrationTest(unittest.TestCase):
    def _baseline_record(self) -> dict:
        s = pd.Series([float(i) for i in range(1, 51)])   # 50 values, no nulls
        from contracts.runner import compute_column_stats
        return compute_column_stats(s)

    def test_all_five_drift_check_types_present_when_baseline_exists(self) -> None:
        # Build a DataFrame that matches the baseline exactly (all checks PASS).
        df = pd.DataFrame({"score": [float(i) for i in range(1, 51)]})
        fields = [{"name": "score", "type": "number", "required": True}]
        baseline_record = self._baseline_record()
        baselines = {"contract-x/tbl": {"score": baseline_record}}

        results, _ = run_table_checks("tbl", fields, df, baselines, "contract-x")

        drift_types = {r["check_type"] for r in results}
        for expected in (
            "drift_mean",
            "drift_variance",
            "drift_outliers",
            "drift_null_fraction",
            "drift_cardinality",
        ):
            self.assertIn(expected, drift_types, f"Missing drift check type: {expected}")

    def test_no_drift_checks_without_baseline(self) -> None:
        df = pd.DataFrame({"score": [1.0, 2.0, 3.0, 4.0, 5.0]})
        fields = [{"name": "score", "type": "number", "required": True}]
        results, _ = run_table_checks("tbl", fields, df, {}, "contract-x")
        drift_types = {r["check_type"] for r in results if "drift" in r["check_type"]}
        self.assertEqual(set(), drift_types)


if __name__ == "__main__":
    unittest.main()
