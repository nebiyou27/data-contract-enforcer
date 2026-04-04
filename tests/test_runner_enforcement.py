import unittest

import pandas as pd

from contracts.runner import load_enforcement_config, run_table_checks


class LoadEnforcementConfigTest(unittest.TestCase):
    def test_merges_contract_and_registry_overrides(self) -> None:
        contract = {
            "enforcement": {
                "validation_mode": "AUDIT",
                "skip_checks": ["type"],
                "field_rules": [
                    {
                        "field": "value",
                        "table": "t",
                        "drift_z_warn": 2.0,
                        "severity": "HIGH",
                    }
                ],
            }
        }
        registry = {
            "subscriptions": [
                {
                    "contract_id": "contract-a",
                    "validation_overrides": {
                        "validation_mode": "ENFORCE",
                        "skip_checks": ["enum"],
                        "field_rules": [
                            {"field": "value", "table": "t", "drift_z_fail": 5.0},
                            {"field": "other", "table": "t", "drift_z_warn": 1.0},
                        ],
                    },
                }
            ]
        }

        merged = load_enforcement_config(contract, registry, "contract-a")

        self.assertEqual("ENFORCE", merged["validation_mode"])
        self.assertEqual({"type", "enum"}, set(merged["skip_checks"]))

        field_rules = {rule["field"]: rule for rule in merged["field_rules"]}
        self.assertEqual(2.0, field_rules["value"]["drift_z_warn"])
        self.assertEqual(5.0, field_rules["value"]["drift_z_fail"])
        self.assertEqual("HIGH", field_rules["value"]["severity"])
        self.assertEqual(1.0, field_rules["other"]["drift_z_warn"])


class RunTableChecksEnforcementTest(unittest.TestCase):
    def test_field_rules_skip_checks_and_override_severity(self) -> None:
        df = pd.DataFrame({"value": [1.0, 5.0, 9.0]})
        fields = [{"name": "value", "type": "number", "required": True}]
        baselines = {
            "contract-a/t": {
                "value": {
                    "mean": 1.0,
                    "stddev": 1.0,
                    "min": 1.0,
                    "max": 1.0,
                    "count": 3,
                    "null_fraction": 0.0,
                    "cardinality": 1,
                }
            }
        }
        enforcement_cfg = {
            "field_rules": [
                {
                    "field": "value",
                    "table": "t",
                    "skip_checks": ["type"],
                    "severity": "LOW",
                }
            ]
        }

        results, _ = run_table_checks("t", fields, df, baselines, "contract-a", enforcement_cfg)

        self.assertFalse(any(r["check_type"] == "type" for r in results))
        drift_mean = next(r for r in results if r["check_id"] == "t.value.drift_mean")
        self.assertEqual("LOW", drift_mean["severity"])
