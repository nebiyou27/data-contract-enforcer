import unittest

from contracts.remediation_generator import build_remediation_plan, load_remediation_rules


class RemediationGeneratorTest(unittest.TestCase):
    def test_loads_packaged_remediation_rules(self) -> None:
        rules = load_remediation_rules()
        self.assertEqual(1, rules["version"])
        self.assertIn("schema_missing", rules["exact"])
        self.assertIn("drift_", rules["prefix"])

    def test_uses_externalized_templates(self) -> None:
        report = {
            "contract_id": "contract-a",
            "report_id": "report-1",
            "results": [
                {
                    "check_id": "t.value.schema_missing",
                    "check_type": "schema_missing",
                    "status": "FAIL",
                    "severity": "CRITICAL",
                    "column_name": "value",
                    "message": "missing",
                },
                {
                    "check_id": "t.value.drift_mean",
                    "check_type": "drift_mean",
                    "status": "WARN",
                    "severity": "MEDIUM",
                    "column_name": "value",
                    "message": "drifted",
                },
            ],
        }
        rules = {
            "default": "Default for `{label}` ({check_type})",
            "exact": {"schema_missing": "Fix `{label}` now"},
            "prefix": {"drift_": "Watch `{label}` for `{check_type}`"},
        }

        plan = build_remediation_plan(report, contract={"info": {"title": "Sample"}}, rules=rules)

        self.assertEqual(2, plan["remediation_count"])
        self.assertEqual("Fix `t.value` now", plan["items"][0]["suggestion"])
        self.assertEqual("Watch `t.value` for `drift_mean`", plan["items"][1]["suggestion"])
