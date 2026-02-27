"""
pipeline/output_builder.py

OutputBuilder – Step 8 of the 8-step pipeline.

Assembles the final structured JSON-serialisable response from all
previous pipeline step outputs.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


class OutputBuilder:
    """Assembles the final structured output for Step 8."""

    def build(
        self,
        query: str,
        parsed_intent: dict[str, Any],
        result_df: pd.DataFrame | None,
        validation: dict[str, Any],
        step_status: dict[str, str],
        errors: list[str] | None = None,
    ) -> dict[str, Any]:
        """Build the final response dict.

        Parameters
        ----------
        query:
            The original natural-language query string.
        parsed_intent:
            Structured intent from QueryParser.
        result_df:
            DataFrame produced by TruthEngine.aggregate() (may be None on error).
        validation:
            Result of TruthEngine.validate_results().
        step_status:
            Per-step status dict from TruthEngine.step_status.
        errors:
            Optional list of error strings encountered during the run.

        Returns
        -------
        JSON-serialisable dict.
        """
        logger.info("Step 8 – Building structured output")

        records: list[dict[str, Any]] = []
        columns: list[str] = []

        if result_df is not None and not result_df.empty:
            # Use JSON round-trip so NaN/Inf become None (null in JSON)
            records = json.loads(result_df.to_json(orient="records"))
            columns = list(result_df.columns)

        status = "success"
        if errors:
            status = "error"
        elif not validation.get("valid", True):
            status = "warning"

        output: dict[str, Any] = {
            "query": query,
            "status": status,
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "metadata": {
                "metric": parsed_intent.get("metric", ""),
                "metric_column": parsed_intent.get("metric_column", ""),
                "group_by": parsed_intent.get("group_by", []),
                "filters": parsed_intent.get("filters", []),
                "result_columns": columns,
                "result_row_count": validation.get("row_count", 0),
            },
            "results": records,
            "validation": {
                "valid": validation.get("valid", False),
                "issues": validation.get("issues", []),
            },
            "pipeline_steps": _build_step_tracker(step_status),
            "errors": errors or [],
        }

        logger.info("Step 8 complete – status=%s, rows=%d", status, len(records))
        return output


def _build_step_tracker(step_status: dict[str, str]) -> list[dict[str, str]]:
    """Convert the step_status dict to an ordered list for the UI tracker."""
    ordered_steps = [
        ("ingest", "1 – Ingest Data"),
        ("validate_schema", "2 – Validate Schema"),
        ("clean", "3 – Clean Data"),
        ("parse_query", "4 – Parse Query Intent"),
        ("filter", "5 – Filter Data"),
        ("aggregate", "6 – Aggregate & Compute"),
        ("validate_results", "7 – Validate Results"),
        ("output", "8 – Structured Output"),
    ]

    tracker: list[dict[str, str]] = []
    for key, label in ordered_steps:
        raw_status = step_status.get(key, "pending")
        if raw_status == "ok":
            icon = "✅"
        elif raw_status == "warning":
            icon = "⚠️"
        elif raw_status == "error":
            icon = "❌"
        else:
            icon = "⏳"
        tracker.append({"step": label, "status": raw_status, "icon": icon})

    return tracker
