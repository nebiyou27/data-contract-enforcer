import tempfile
import unittest
from pathlib import Path

from scripts.migrate_week2 import build_record, find_report_files, split_sections


SAMPLE_REPORT = """# AUTOMATON AUDITOR - FINAL VERDICT

## Executive Summary
- **Repository:** src/synthetic_module.py
- **Overall Score:** 3.75
- **Overall Verdict:** PASS
- **Confidence:** 0.92
- **Evaluated At:** 2026-04-01T19:46:06.376133Z

### Dimension: code_quality
**Final Score:** 3/5

**Judge Opinions (All Three):**
#### Prosecutor
- Cited Evidence: src/synthetic_module.py

**Dissent Summary:**
Low dissent: score spread 3-4.

**File-level Remediation Targets:** src/synthetic_module.py

### Criterion: test_coverage
**Final Score:** 4/5

- Cited: tests/test_synthetic_module.py

**Deterministic Resolution:**
Weighted scoring: P=3, D=4, T=4 (x2).
"""


class MigrateWeek2Test(unittest.TestCase):
    def test_split_sections_extracts_both_heading_styles(self) -> None:
        sections = split_sections(SAMPLE_REPORT)
        self.assertEqual(["code_quality", "test_coverage"], [section.criterion_id for section in sections])

    def test_build_record_matches_friend_style_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "audit_report.md"
            report_path.write_text(SAMPLE_REPORT, encoding="utf-8")

            record = build_record(report_path)

        self.assertEqual("src/synthetic_module.py", record["target_ref"])
        self.assertEqual("PASS", record["overall_verdict"])
        self.assertEqual(3.75, record["overall_score"])
        self.assertEqual(0.92, record["confidence"])
        self.assertEqual("1.0.0", record["rubric_version"])
        self.assertIn("verdict_id", record)
        self.assertIn("rubric_id", record)
        self.assertEqual({"code_quality", "test_coverage"}, set(record["scores"].keys()))
        self.assertEqual(3.0, record["scores"]["code_quality"]["score"])
        self.assertIn("src/synthetic_module.py", record["scores"]["code_quality"]["evidence"])
        self.assertIn("tests/test_synthetic_module.py", record["scores"]["test_coverage"]["evidence"])

    def test_find_report_files_accepts_peer_folder_alias(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "report_on_peer_generated"
            path.mkdir()
            (path / "audit_report.md").write_text(SAMPLE_REPORT, encoding="utf-8")

            reports = find_report_files(str(path))

        self.assertEqual(1, len(reports))
        self.assertEqual("audit_report.md", reports[0].name)


if __name__ == "__main__":
    unittest.main()
