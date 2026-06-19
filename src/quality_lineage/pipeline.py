"""A small, auditable data-quality pipeline built around DuckDB."""

from __future__ import annotations

import csv
import hashlib
import json
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from html import escape
from pathlib import Path
from typing import Any

import duckdb


PIPELINE_VERSION = "0.1.0"
SCHEMA_VERSION = "1"
QUALITY_RULES = (
    "order_id_required",
    "order_id_unique",
    "customer_reference_exists",
    "amount_positive",
    "amount_decimal",
    "timestamp_iso8601",
)
CURATED_ORDER_COLUMNS = (
    "order_id",
    "order_timestamp",
    "amount_eur",
    "source_system",
    "customer_id",
    "customer_name",
    "region",
    "order_month",
)


@dataclass(frozen=True)
class QualityIssue:
    order_id: str
    customer_id: str
    rule: str
    detail: str


@dataclass(frozen=True)
class PipelineResult:
    source_rows: int
    valid_rows: int
    quarantined_rows: int
    error_rate: float
    output_dir: Path
    rule_counts: dict[str, int]


REQUIRED_ORDER_COLUMNS = ("order_id", "customer_id", "order_timestamp", "amount_eur", "source_system")
REQUIRED_CUSTOMER_COLUMNS = ("customer_id", "customer_name", "region")


def run_pipeline(
    source_dir: str | Path,
    output_dir: str | Path,
    *,
    run_timestamp: str | None = None,
) -> PipelineResult:
    """Validate order data, create curated DuckDB output, and write report artefacts."""

    source = Path(source_dir)
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    orders_path = source / "orders.csv"
    customers_path = source / "customers.csv"
    orders = _read_csv(orders_path, REQUIRED_ORDER_COLUMNS)
    customers = _read_csv(customers_path, REQUIRED_CUSTOMER_COLUMNS)
    customer_ids = {_normalize_cell(row.get("customer_id")) for row in customers if _normalize_cell(row.get("customer_id"))}

    valid_orders: list[dict[str, str]] = []
    quarantined_rows: list[dict[str, str]] = []
    issues: list[QualityIssue] = []
    seen_order_ids: set[str] = set()
    timestamp = run_timestamp or datetime.now(UTC).isoformat().replace("+00:00", "Z")

    for raw_row in orders:
        row = _normalize_row(raw_row)
        row_issues = _validate_order(row, customer_ids, seen_order_ids)
        if row_issues:
            issues.extend(row_issues)
            quarantined_rows.append({**row, "quality_errors": " | ".join(issue.rule for issue in row_issues)})
        else:
            valid_orders.append(row)

    _write_quarantine(output / "quarantined_orders.csv", quarantined_rows)
    _write_duckdb_outputs(output, valid_orders, customers)
    rule_counts = dict(sorted(Counter(issue.rule for issue in issues).items()))
    source_rows = len(orders)
    quarantined_count = len(quarantined_rows)
    error_rate = quarantined_count / source_rows if source_rows else 0.0
    result = PipelineResult(
        source_rows=source_rows,
        valid_rows=len(valid_orders),
        quarantined_rows=quarantined_count,
        error_rate=error_rate,
        output_dir=output,
        rule_counts=rule_counts,
    )
    _write_summary(output / "run_summary.json", result, issues, timestamp)
    _write_lineage(output / "lineage_manifest.json", source, output, orders_path, customers_path, timestamp)
    _write_html_report(output / "quality_report.html", result, issues)
    return result


def _normalize_cell(value: str | None) -> str:
    return value.strip() if value is not None else ""


def _normalize_row(row: dict[str, str | None]) -> dict[str, str]:
    return {key: _normalize_cell(value) for key, value in row.items()}


def _read_csv(path: Path, required_columns: tuple[str, ...]) -> list[dict[str, str | None]]:
    if not path.exists():
        raise ValueError(f"Required source file does not exist: {path}")
    with path.open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        if reader.fieldnames is None:
            raise ValueError(f"Source file has no header: {path}")
        missing = [column for column in required_columns if column not in reader.fieldnames]
        if missing:
            raise ValueError(f"Source file {path.name} is missing required columns: {', '.join(missing)}")
        rows = [dict(row) for row in reader]
    if not rows and reader.fieldnames:
        return []
    return rows


def _validate_order(
    order: dict[str, str],
    customer_ids: set[str],
    seen_order_ids: set[str],
) -> list[QualityIssue]:
    issues: list[QualityIssue] = []
    order_id = order.get("order_id", "")
    customer_id = order.get("customer_id", "")

    if not order_id:
        issues.append(QualityIssue(order_id, customer_id, "order_id_required", "Order ID is blank."))
    elif order_id in seen_order_ids:
        issues.append(
            QualityIssue(order_id, customer_id, "order_id_unique", "Order ID already appeared in the source batch.")
        )
    else:
        seen_order_ids.add(order_id)

    for column in REQUIRED_ORDER_COLUMNS:
        if column == "order_id":
            continue
        if not order.get(column, ""):
            issues.append(
                QualityIssue(
                    order_id,
                    customer_id,
                    f"{column}_required",
                    f"{column} is blank or missing in the source row.",
                )
            )

    if customer_id not in customer_ids:
        issues.append(
            QualityIssue(order_id, customer_id, "customer_reference_exists", "Customer ID is absent from customers.csv.")
        )
    try:
        amount = Decimal(order.get("amount_eur", ""))
        if amount <= 0:
            issues.append(QualityIssue(order_id, customer_id, "amount_positive", "Amount must be greater than zero."))
    except (InvalidOperation, ValueError):
        issues.append(QualityIssue(order_id, customer_id, "amount_decimal", "Amount is not a decimal number."))
    try:
        datetime.fromisoformat(order.get("order_timestamp", ""))
    except ValueError:
        issues.append(QualityIssue(order_id, customer_id, "timestamp_iso8601", "Timestamp is not ISO-8601."))
    return issues


def _write_quarantine(path: Path, rows: list[dict[str, str]]) -> None:
    fieldnames = [*REQUIRED_ORDER_COLUMNS, "quality_errors"]
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _write_duckdb_outputs(output: Path, orders: list[dict[str, str]], customers: list[dict[str, str]]) -> None:
    database_path = output / "quality_pipeline.duckdb"
    con = duckdb.connect(str(database_path))
    try:
        con.execute("CREATE OR REPLACE TABLE customers (customer_id VARCHAR, customer_name VARCHAR, region VARCHAR)")
        con.executemany(
            "INSERT INTO customers VALUES (?, ?, ?)",
            [(row["customer_id"], row["customer_name"], row["region"]) for row in customers],
        )
        con.execute("""
            CREATE OR REPLACE TABLE validated_orders (
                order_id VARCHAR,
                customer_id VARCHAR,
                order_timestamp TIMESTAMP,
                amount_eur DECIMAL(18, 2),
                source_system VARCHAR
            )
        """)
        if orders:
            con.executemany(
                "INSERT INTO validated_orders VALUES (?, ?, ?, ?, ?)",
                [
                    (
                        row["order_id"],
                        row["customer_id"],
                        datetime.fromisoformat(row["order_timestamp"]),
                        Decimal(row["amount_eur"]),
                        row["source_system"],
                    )
                    for row in orders
                ],
            )
        if orders:
            con.execute("""
                CREATE OR REPLACE TABLE curated_orders AS
                SELECT
                    o.order_id,
                    o.order_timestamp,
                    o.amount_eur,
                    o.source_system,
                    c.customer_id,
                    c.customer_name,
                    c.region,
                    date_trunc('month', o.order_timestamp) AS order_month
                FROM validated_orders o
                INNER JOIN customers c USING (customer_id)
            """)
            parquet_path = str(output / "curated_orders.parquet").replace("'", "''")
            csv_path = str(output / "curated_orders.csv").replace("'", "''")
            con.execute(f"COPY curated_orders TO '{parquet_path}' (FORMAT PARQUET)")
            con.execute(f"COPY curated_orders TO '{csv_path}' (HEADER, DELIMITER ',')")
        else:
            con.execute(
                """
                CREATE OR REPLACE TABLE curated_orders (
                    order_id VARCHAR,
                    order_timestamp TIMESTAMP,
                    amount_eur DECIMAL(18, 2),
                    source_system VARCHAR,
                    customer_id VARCHAR,
                    customer_name VARCHAR,
                    region VARCHAR,
                    order_month TIMESTAMP
                )
                """
            )
            parquet_path = str(output / "curated_orders.parquet").replace("'", "''")
            csv_path = str(output / "curated_orders.csv").replace("'", "''")
            con.execute(f"COPY curated_orders TO '{parquet_path}' (FORMAT PARQUET)")
            con.execute(f"COPY curated_orders TO '{csv_path}' (HEADER, DELIMITER ',')")
    finally:
        con.close()


def _file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_summary(path: Path, result: PipelineResult, issues: list[QualityIssue], run_timestamp: str) -> None:
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "pipeline_version": PIPELINE_VERSION,
        "pipeline": "data-quality-lineage-pipeline",
        "run_timestamp": run_timestamp,
        "source_rows": result.source_rows,
        "valid_rows": result.valid_rows,
        "quarantined_rows": result.quarantined_rows,
        "error_rate": round(result.error_rate, 4),
        "rule_counts": result.rule_counts,
        "quality_rules": list(QUALITY_RULES),
        "sample_issues": [issue.__dict__ for issue in issues[:10]],
        "outputs": {
            "database": "quality_pipeline.duckdb",
            "curated_parquet": "curated_orders.parquet",
            "curated_csv": "curated_orders.csv",
            "quarantine_csv": "quarantined_orders.csv",
            "lineage": "lineage_manifest.json",
            "report": "quality_report.html",
        },
        "output_schemas": {
            "curated_orders": list(CURATED_ORDER_COLUMNS),
        },
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_lineage(
    path: Path,
    source: Path,
    output: Path,
    orders_path: Path,
    customers_path: Path,
    run_timestamp: str,
) -> None:
    payload = {
        "schema_version": SCHEMA_VERSION,
        "pipeline_version": PIPELINE_VERSION,
        "run_timestamp": run_timestamp,
        "pipeline": "data-quality-lineage-pipeline",
        "datasets": [
            {
                "name": "raw_orders",
                "path": "orders.csv",
                "role": "source",
                "content_sha256": _file_sha256(orders_path),
                "columns": list(REQUIRED_ORDER_COLUMNS),
            },
            {
                "name": "raw_customers",
                "path": "customers.csv",
                "role": "source",
                "content_sha256": _file_sha256(customers_path),
                "columns": list(REQUIRED_CUSTOMER_COLUMNS),
            },
            {
                "name": "quarantined_orders",
                "path": "quarantined_orders.csv",
                "role": "quality-output",
            },
            {
                "name": "curated_orders",
                "path": "curated_orders.parquet",
                "role": "analytical-output",
                "columns": list(CURATED_ORDER_COLUMNS),
            },
        ],
        "transformations": [
            {
                "name": "validate_orders",
                "inputs": ["raw_orders", "raw_customers"],
                "outputs": ["quarantined_orders", "validated_orders"],
                "rules": list(QUALITY_RULES),
            },
            {
                "name": "enrich_orders",
                "inputs": ["validated_orders", "raw_customers"],
                "outputs": ["curated_orders"],
                "sql": "validated_orders INNER JOIN customers USING (customer_id)",
            },
        ],
        "source_directory": source.name,
        "output_directory": output.name,
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _write_html_report(path: Path, result: PipelineResult, issues: list[QualityIssue]) -> None:
    cards = "".join(
        f'<article class="card"><span>{label}</span><strong>{value}</strong></article>'
        for label, value in (
            ("Source rows", result.source_rows),
            ("Validated rows", result.valid_rows),
            ("Quarantined rows", result.quarantined_rows),
            ("Error rate", f"{result.error_rate:.1%}"),
        )
    )
    rule_rows = "".join(
        f"<tr><td>{escape(rule)}</td><td>{count}</td></tr>" for rule, count in result.rule_counts.items()
    ) or "<tr><td colspan=\"2\">No quality issues detected.</td></tr>"
    issue_rows = "".join(
        f"<tr><td>{escape(issue.order_id or '(blank)')}</td><td>{escape(issue.customer_id)}</td><td>{escape(issue.rule)}</td><td>{escape(issue.detail)}</td></tr>"
        for issue in issues[:8]
    ) or "<tr><td colspan=\"4\">No quarantined records.</td></tr>"
    path.write_text(
        f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1"><title>Data Quality Report</title>
<style>
  :root {{ font-family: Inter, ui-sans-serif, system-ui, sans-serif; color: #172033; background: #f4f7fb; }}
  body {{ max-width: 1120px; margin: 0 auto; padding: 48px 24px; }} header {{ border-bottom: 4px solid #155e75; padding-bottom: 18px; }}
  h1 {{ margin: 0; font-size: clamp(2rem, 5vw, 3.2rem); letter-spacing: -.04em; }} p {{ color: #52606d; line-height: 1.55; }}
  .cards {{ display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 14px; margin: 28px 0; }} .card, table {{ background: white; border: 1px solid #d9e2ec; border-radius: 12px; }}
  .card {{ padding: 18px; }} .card span {{ color: #52606d; display: block; }} .card strong {{ display: block; font-size: 2rem; margin-top: 6px; }}
  table {{ width: 100%; border-collapse: collapse; overflow: hidden; margin-top: 12px; }} th, td {{ padding: 13px 15px; text-align: left; border-bottom: 1px solid #e5edf5; }} th {{ background: #e7f3f5; color: #155e75; font-size: .82rem; letter-spacing: .05em; text-transform: uppercase; }} tr:last-child td {{ border-bottom: 0; }}
  code {{ background: #e9eef5; border-radius: 4px; padding: 2px 4px; }} footer {{ color: #6b7c93; font-size: .9rem; margin-top: 28px; }} @media(max-width:720px){{.cards{{grid-template-columns:repeat(2,minmax(0,1fr));}}}}
</style></head><body>
<header><h1>Data Quality &amp; Lineage Report</h1><p>Raw order records are checked against transparent quality rules, invalid data is quarantined, and valid records are enriched with customer dimensions before export to DuckDB, CSV, and Parquet.</p></header>
<section class="cards" aria-label="Quality summary">{cards}</section>
<section><h2>Rule failures</h2><table><thead><tr><th>Rule</th><th>Records</th></tr></thead><tbody>{rule_rows}</tbody></table></section>
<section><h2>Quarantine sample</h2><table><thead><tr><th>Order</th><th>Customer</th><th>Rule</th><th>Detail</th></tr></thead><tbody>{issue_rows}</tbody></table></section>
<footer>Generated by the Data Quality &amp; Lineage Pipeline. Inputs are synthetic and intended for portfolio demonstration only.</footer></body></html>""",
        encoding="utf-8",
    )
