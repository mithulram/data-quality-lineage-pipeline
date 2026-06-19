"""Command-line interface for the data quality pipeline."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from .pipeline import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="quality-lineage", description="Validate raw CSV order data and produce curated DuckDB outputs."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run", help="run validation, quarantine, enrichment, and reporting")
    run.add_argument("--source", type=Path, required=True, help="directory containing orders.csv and customers.csv")
    run.add_argument("--output", type=Path, required=True, help="directory for generated artefacts")
    run.add_argument(
        "--max-error-rate",
        type=float,
        default=0.0,
        help="return exit code 2 when quarantined-row rate exceeds this value (0.0-1.0)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not 0 <= args.max_error_rate <= 1:
        parser.error("--max-error-rate must be between 0.0 and 1.0")
    try:
        result = run_pipeline(args.source, args.output)
    except ValueError as error:
        parser.error(str(error))
    print(
        f"Pipeline complete: source_rows={result.source_rows} | valid_rows={result.valid_rows} | "
        f"quarantined_rows={result.quarantined_rows} | error_rate={result.error_rate:.1%}"
    )
    if result.error_rate > args.max_error_rate:
        print(f"Quality gate: error rate exceeds configured maximum of {args.max_error_rate:.1%}.")
        return 2
    return 0
