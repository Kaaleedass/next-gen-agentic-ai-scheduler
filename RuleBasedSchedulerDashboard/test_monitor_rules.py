"""Offline regression tests for Scheduler Studio log/output evidence rules.
Run: python test_monitor_rules.py
No Flask, CrewAI, OpenAI key or live scheduler DB is needed for these tests.
"""
import os
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import crewai_monitor as monitor  # noqa: E402


class MonitorRuleTests(unittest.TestCase):
    def setUp(self):
        self.no_output = {
            "configured": False, "exists": False, "has_files": False,
            "files": [], "text": "No output path configured.",
        }

    def verdict(self, status, log, output=None, message=""):
        return monitor.classify_evidence(status, message, log, output or self.no_output)["verdict"]

    def test_new_log_file_without_success_marker_is_not_success(self):
        log = {"configured": True, "path": "logs", "files": [{"path": "run.log"}],
               "text": "Log file created. Process started."}
        self.assertEqual(self.verdict("SUCCESS", log), "UNKNOWN")

    def test_explicit_success_marker_confirms_success(self):
        log = {"configured": True, "path": "logs", "files": [{"path": "run.log"}],
               "text": "Report saved successfully. Exit code: 0"}
        self.assertEqual(self.verdict("SUCCESS", log), "SUCCESS")

    def test_failure_evidence_overrides_success(self):
        log = {"configured": True, "path": "logs", "files": [{"path": "run.log"}],
               "text": "Report saved successfully.\nTraceback (most recent call last):\nValueError: failed"}
        self.assertEqual(self.verdict("SUCCESS", log), "FAILURE")

    def test_missing_output_after_success_marker_is_failure(self):
        log = {"configured": True, "path": "logs", "files": [{"path": "run.log"}],
               "text": "Report saved successfully."}
        output = {"configured": True, "exists": False, "has_files": False, "files": [], "path": "missing"}
        self.assertEqual(self.verdict("SUCCESS", log, output), "FAILURE")

    def test_success_status_without_log_configuration_is_unknown(self):
        log = {"configured": False, "path": "", "files": [], "text": "No log path configured."}
        self.assertEqual(self.verdict("SUCCESS", log), "UNKNOWN")

    def test_nested_csv_output_profiles_all_rows(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            folder = root / "daily" / "run"
            folder.mkdir(parents=True)
            output = folder / "orders.csv"
            output.write_text(
                "Vendor,Orders,Amount\nA,2,100\nB,3,250\nC,4,150\n",
                encoding="utf-8",
            )
            evidence = monitor.collect_output_evidence(str(root))
            self.assertTrue(evidence["has_files"])
            self.assertTrue(evidence["content_available"])
            metadata = evidence["files"][0]
            self.assertEqual(metadata["record_count"], 3)
            self.assertEqual(metadata["numeric_summary_all_rows"]["Orders"]["sum"], "9")
            self.assertEqual(metadata["numeric_summary_all_rows"]["Amount"]["sum"], "500")


    def test_full_log_scan_detects_error_before_tail_excerpt(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = Path(temp_dir) / "full.log"
            with log_path.open("w", encoding="utf-8") as stream:
                stream.write("ValueError: failure occurred at start\n")
                for i in range(3000):
                    stream.write(f"ordinary processing line {i}\n")
                stream.write("Task completed successfully.\n")
            evidence = monitor.collect_log_evidence(str(log_path))
            self.assertEqual(evidence["line_count"], 3002)
            self.assertGreaterEqual(evidence["failure_count"], 1)
            self.assertTrue(any("ValueError" in line for line in evidence["failure_lines"]))
            verdict = monitor.classify_evidence("SUCCESS", "", evidence, self.no_output)
            self.assertEqual(verdict["verdict"], "FAILURE")

    def test_full_log_scan_returns_line_count_and_success_marker(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            log_path = Path(temp_dir) / "ok.log"
            log_path.write_text("starting\nReport generated successfully.\nfinished\n", encoding="utf-8")
            evidence = monitor.collect_log_evidence(str(log_path))
            self.assertEqual(evidence["line_count"], 3)
            self.assertEqual(evidence["failure_count"], 0)
            self.assertGreaterEqual(evidence["success_count"], 1)
            verdict = monitor.classify_evidence("SUCCESS", "", evidence, self.no_output)
            self.assertEqual(verdict["verdict"], "SUCCESS")

    def test_missing_output_question_returns_data_unavailable(self):
        result = monitor.answer_output_question(
            {"TASK_ID": 123, "TASK_NAME": "Test task"},
            "What is the amount?",
            {"configured": False, "exists": False, "has_files": False, "files": [], "path": "", "text": ""},
        )
        self.assertIn("Data not available", result["answer"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
