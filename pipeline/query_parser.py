"""
pipeline/query_parser.py

QueryParser – Step 4 of the 8-step pipeline.

Converts a natural-language query string into a structured intent dict
using rule-based keyword matching (no LLM required).

Returned intent structure::

    {
        "original_query": str,
        "metric": "count" | "churn_rate" | "average" | "sum",
        "metric_column": str,       # relevant for average/sum
        "group_by": [str, ...],     # list of column names to group by
        "filters": [                # optional row-level filters
            {"column": str, "operator": str, "value": str},
            ...
        ],
    }
"""

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


class QueryParser:
    """Rule-based natural-language → structured query intent parser."""

    # Keywords that indicate churn-rate metric
    _CHURN_KEYWORDS = {"churn rate", "churn_rate", "churned", "churn"}

    # Keywords that indicate average metric
    _AVG_KEYWORDS = {
        "average", "avg", "mean", "average of", "avg of",
        "show average", "what is the average",
    }

    # Keywords that indicate sum metric
    _SUM_KEYWORDS = {"total", "sum", "sum of", "total of"}

    # Keywords that indicate count metric
    _COUNT_KEYWORDS = {
        "count", "how many", "number of", "counts", "frequency",
    }

    # Prepositions / connectors used before a grouping column
    _GROUP_BY_PREPS = {"by", "per", "for each", "grouped by", "broken down by"}

    def __init__(self, available_columns: list[str]) -> None:
        """
        Parameters
        ----------
        available_columns:
            Column names present in the loaded dataset (case-sensitive).
        """
        self.available_columns: list[str] = available_columns
        # Build lower-case → original mapping for fuzzy matching
        self._col_lower: dict[str, str] = {c.lower(): c for c in available_columns}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def parse(self, query: str) -> dict[str, Any]:
        """Parse a natural-language query into structured intent.

        Parameters
        ----------
        query:
            Raw user query string.

        Returns
        -------
        Structured intent dict.
        """
        logger.info("Step 4 – Parsing query: %r", query)
        query_lower = query.lower().strip()

        metric, metric_column = self._detect_metric(query_lower)
        group_by = self._detect_group_by(query_lower)
        filters = self._detect_filters(query_lower)

        intent: dict[str, Any] = {
            "original_query": query,
            "metric": metric,
            "metric_column": metric_column,
            "group_by": group_by,
            "filters": filters,
        }
        logger.info("Step 4 complete – intent: %s", intent)
        return intent

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _detect_metric(self, q: str) -> tuple[str, str]:
        """Return (metric_name, metric_column)."""

        # --- churn rate ---
        for kw in self._CHURN_KEYWORDS:
            if kw in q:
                churn_col = self._find_column("churn")
                return "churn_rate", churn_col

        # --- average ---
        for kw in sorted(self._AVG_KEYWORDS, key=len, reverse=True):
            if kw in q:
                metric_col = self._extract_metric_column(q, kw)
                return "average", metric_col

        # --- sum ---
        for kw in sorted(self._SUM_KEYWORDS, key=len, reverse=True):
            if kw in q:
                metric_col = self._extract_metric_column(q, kw)
                return "sum", metric_col

        # --- count (default) ---
        return "count", ""

    def _extract_metric_column(self, q: str, trigger_kw: str) -> str:
        """Try to find which column the metric applies to, appearing after trigger_kw."""
        # Remove the trigger keyword and search for a known column in the remainder
        remainder = q[q.find(trigger_kw) + len(trigger_kw):]
        return self._find_column_in_text(remainder) or ""

    def _detect_group_by(self, q: str) -> list[str]:
        """Find GROUP BY columns from preposition-based heuristics."""
        group_cols: list[str] = []
        for prep in sorted(self._GROUP_BY_PREPS, key=len, reverse=True):
            pattern = rf"\b{re.escape(prep)}\b\s+(\w[\w\s]*)"
            for m in re.finditer(pattern, q):
                candidate = m.group(1).strip()
                col = self._find_column_in_text(candidate)
                if col and col not in group_cols:
                    group_cols.append(col)

        # Fallback: if the query mentions a column that looks like a category
        if not group_cols:
            col = self._find_column_in_text(q)
            if col:
                group_cols.append(col)

        return group_cols

    def _detect_filters(self, q: str) -> list[dict[str, Any]]:
        """Extract simple equality filters like 'where gender = female'."""
        filters: list[dict[str, Any]] = []

        # Pattern: <col> is/= <value> or where <col> <op> <value>
        for col_lower, col_orig in self._col_lower.items():
            # Look for patterns like "gender is female" or "gender = female"
            # Handle both quoted ("female") and unquoted (female) values
            pattern = (
                rf"\b{re.escape(col_lower)}\b\s*(?:is|=|equals?)\s+"
                rf"(?:\"([^\"]+)\"|'([^']+)'|(\w[\w\s-]*))"
            )
            m = re.search(pattern, q)
            if m:
                value = (m.group(1) or m.group(2) or m.group(3) or "").strip()
                if value:
                    filters.append({"column": col_orig, "operator": "=", "value": value})

        return filters

    def _find_column_in_text(self, text: str) -> str:
        """Return the first known column name found inside *text*."""
        text_lower = text.lower()
        # Prefer longer column names to avoid partial matches
        for col_lower, col_orig in sorted(
            self._col_lower.items(), key=lambda x: len(x[0]), reverse=True
        ):
            # Match whole word (allow underscores, digits, and letters as word chars)
            pattern = r"(?<![a-zA-Z0-9_])" + re.escape(col_lower) + r"(?![a-zA-Z0-9_])"
            if re.search(pattern, text_lower):
                return col_orig
        return ""

    def _find_column(self, keyword: str) -> str:
        """Return the original column name whose lower-case form contains *keyword*."""
        for col_lower, col_orig in self._col_lower.items():
            if keyword in col_lower:
                return col_orig
        return ""
