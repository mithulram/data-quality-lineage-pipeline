import json
import shutil
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

import duckdb

from quality_lineage.cli import main
from quality_lineage.pipeline import run_pipeline


ROOT = Path(__file__).resolve().parents[1]
SAMPLE_SOURCE = ROOT / "examples" / "source"


class PipelineTests(unittest.TestCase):
    def test_pipeline_quarantines_invalid_rows_and_writes_curated_outputs(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            output = Path(temporary_directory) / "output"
            result = run_pipeline(SAMPLE_SOURCE, output)
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
            self.assertTrue((output / "curated_orders.parquet").exists())
            self.assertIn("Data Quality &amp; Lineage Report", (output / "quality_report.html").read_text())
            con = duckdb.connect(str(output / "quality_pipeline.duckdb"), read_only=True)
            try:
                self.assertEqual(con.execute("SELECT count(*) FROM curated_orders").fetchone()[0], 2)
                self.assertEqual(con.execute("SELECT sum(amount_eur) FROM curated_orders").fetchone()[0], Decimal("335.50"))
            finally:
                con.close()

    def test_pipeline_rejects_missing_required_column(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            source = Path(temporary_directory) / "source"
            source.mkdir()
            shutil.copy(SAMPLE_SOURCE / "customers.csv", source / "customers.csv")
            (source / "orders.csv").write_text("order_id,customer_id\nORD-1,CUST-001\n", encoding="utf-8")
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
