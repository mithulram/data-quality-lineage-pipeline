import json
import shutil
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

import duckdb

from quality_lineage.cli import main
from quality_lineage.pipeline import QUALITY_RULES, run_pipeline


ROOT = Path(__file__).resolve().parents[1]
SAMPLE_SOURCE = ROOT / "examples" / "source"
FIXED_RUN_TIMESTAMP = "2026-06-20T12:00:00Z"


class PipelineTests(unittest.TestCase):
    def test_pipeline_quarantines_invalid_rows_and_writes_curated_outputs(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            output = Path(temporary_directory) / "output"
            result = run_pipeline(SAMPLE_SOURCE, output, run_timestamp=FIXED_RUN_TIMESTAMP)
            self.assertEqual(result.source_rows, 6)
            self.assertEqual(result.valid_rows, 2)
            self.assertEqual(result.quarantined_rows, 4)
            self.assertAlmostEqual(result.error_rate, 4 / 6)
            self.assertEqual(
                result.rule_counts,
                {
                    "amount_positive": 1,
                    "customer_reference_exists": 1,
                    "order_id_unique": 1,
                    "timestamp_iso8601": 1,
                },
            )
            summary = json.loads((output / "run_summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary["valid_rows"], 2)
            self.assertEqual(summary["schema_version"], "1")
            self.assertEqual(summary["pipeline_version"], "0.1.0")
            self.assertEqual(summary["run_timestamp"], FIXED_RUN_TIMESTAMP)
            self.assertEqual(summary["quality_rules"], list(QUALITY_RULES))
            lineage = json.loads((output / "lineage_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(lineage["schema_version"], "1")
            self.assertIn("amount_decimal", lineage["transformations"][0]["rules"])
            self.assertTrue(lineage["datasets"][0]["content_sha256"])
            self.assertEqual(lineage["datasets"][0]["path"], "orders.csv")
            self.assertTrue((output / "curated_orders.parquet").exists())
            self.assertIn("Data Quality &amp; Lineage Report", (output / "quality_report.html").read_text())
            con = duckdb.connect(str(output / "quality_pipeline.duckdb"), read_only=True)
            try:
                self.assertEqual(con.execute("SELECT count(*) FROM curated_orders").fetchone()[0], 2)
                self.assertEqual(con.execute("SELECT sum(amount_eur) FROM curated_orders").fetchone()[0], Decimal("335.50"))
            finally:
                con.close()

    def test_duplicate_is_detected_after_invalid_first_row(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            source = Path(temporary_directory) / "source"
            source.mkdir()
            shutil.copy(SAMPLE_SOURCE / "customers.csv", source / "customers.csv")
            (source / "orders.csv").write_text(
                "\n".join(
                    [
                        "order_id,customer_id,order_timestamp,amount_eur,source_system",
                        "ORD-DUP,CUST-001,2026-01-01T10:00:00,not-a-number,web",
                        "ORD-DUP,CUST-001,2026-01-02T10:00:00,10.00,web",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            output = Path(temporary_directory) / "output"
            result = run_pipeline(source, output, run_timestamp=FIXED_RUN_TIMESTAMP)
            self.assertEqual(result.quarantined_rows, 2)
            self.assertEqual(result.rule_counts["amount_decimal"], 1)
            self.assertEqual(result.rule_counts["order_id_unique"], 1)

    def test_missing_trailing_column_is_quarantined_predictably(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            source = Path(temporary_directory) / "source"
            source.mkdir()
            shutil.copy(SAMPLE_SOURCE / "customers.csv", source / "customers.csv")
            (source / "orders.csv").write_text(
                "\n".join(
                    [
                        "order_id,customer_id,order_timestamp,amount_eur,source_system",
                        "ORD-900,CUST-001,2026-01-01T10:00:00,10.00",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            output = Path(temporary_directory) / "output"
            result = run_pipeline(source, output, run_timestamp=FIXED_RUN_TIMESTAMP)
            self.assertEqual(result.quarantined_rows, 1)
            self.assertIn("source_system_required", result.rule_counts)

    def test_header_only_orders_file_produces_empty_outputs(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            source = Path(temporary_directory) / "source"
            source.mkdir()
            shutil.copy(SAMPLE_SOURCE / "customers.csv", source / "customers.csv")
            (source / "orders.csv").write_text(
                "order_id,customer_id,order_timestamp,amount_eur,source_system\n",
                encoding="utf-8",
            )
            output = Path(temporary_directory) / "output"
            result = run_pipeline(source, output, run_timestamp=FIXED_RUN_TIMESTAMP)
            self.assertEqual(result.source_rows, 0)
            self.assertEqual(result.valid_rows, 0)
            self.assertEqual(result.quarantined_rows, 0)

    def test_pipeline_rejects_missing_required_column(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            source = Path(temporary_directory) / "source"
            source.mkdir()
            shutil.copy(SAMPLE_SOURCE / "customers.csv", source / "customers.csv")
            (source / "orders.csv").write_text("order_id,customer_id\nORD-1,CUST-001\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "missing required columns"):
                run_pipeline(source, Path(temporary_directory) / "output")

    def test_pipeline_rejects_missing_header(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            source = Path(temporary_directory) / "source"
            source.mkdir()
            shutil.copy(SAMPLE_SOURCE / "customers.csv", source / "customers.csv")
            (source / "orders.csv").write_text("ORD-1,CUST-001\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "missing required columns"):
                run_pipeline(source, Path(temporary_directory) / "output")

    def test_cli_policy_gate_returns_nonzero_when_quality_budget_is_breached(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            result = main(
                [
                    "run",
                    "--source",
                    str(SAMPLE_SOURCE),
                    "--output",
                    str(Path(temporary_directory) / "output"),
                    "--max-error-rate",
                    "0.5",
                ]
            )
            self.assertEqual(result, 2)
