"""
pipeline/engine.py

TruthEngine – DuckDB-powered data engine covering Steps 1-3 and 5-7 of the
8-step data analysis pipeline.

Step 1 – Ingest Data
Step 2 – Validate Schema
Step 3 – Clean Data
Step 5 – Filter Data
Step 6 – Aggregate & Compute
Step 7 – Validate Results
"""

import logging
import re
from typing import Any

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


class TruthEngine:
    """DuckDB-backed engine that drives the data analysis pipeline."""

    # Canonical binary categorical values that can be standardised to No/Yes
    _BINARY_MAPS: dict[str, str] = {"0": "No", "1": "Yes"}

    def __init__(self) -> None:
        self.conn: duckdb.DuckDBPyConnection = duckdb.connect(":memory:")
        self.table_name: str = "pipeline_data"
        self.schema_info: list[dict[str, Any]] = []
        self.row_count: int = 0
        self.column_names: list[str] = []
        self.step_status: dict[str, str] = {}

    # ------------------------------------------------------------------
    # Step 1 – Ingest Data
    # ------------------------------------------------------------------

    def ingest(self, csv_path: str) -> dict[str, Any]:
        """Load a CSV file into DuckDB and detect schema.

        Parameters
        ----------
        csv_path:
            Absolute path to the CSV file on disk.

        Returns
        -------
        dict with keys: columns, dtypes, row_count, sample
        """
        logger.info("Step 1 – Ingesting data from %s", csv_path)
        try:
            # Drop stale table if a previous file was loaded
            self.conn.execute(f"DROP TABLE IF EXISTS {self.table_name}")
            self.conn.execute(
                f"CREATE TABLE {self.table_name} AS "
                f"SELECT * FROM read_csv_auto('{csv_path}', header=true)"
            )

            describe_df: pd.DataFrame = self.conn.execute(
                f"DESCRIBE {self.table_name}"
            ).df()

            self.schema_info = describe_df.to_dict(orient="records")
            self.column_names = [r["column_name"] for r in self.schema_info]

            count_result = self.conn.execute(
                f"SELECT COUNT(*) FROM {self.table_name}"
            ).fetchone()
            self.row_count = int(count_result[0]) if count_result else 0

            sample_df: pd.DataFrame = self.conn.execute(
                f"SELECT * FROM {self.table_name} LIMIT 5"
            ).df()

            self.step_status["ingest"] = "ok"
            logger.info(
                "Step 1 complete – %d rows, %d columns",
                self.row_count,
                len(self.column_names),
            )
            return {
                "columns": self.column_names,
                "dtypes": {r["column_name"]: r["column_type"] for r in self.schema_info},
                "row_count": self.row_count,
                "sample": sample_df.to_dict(orient="records"),
            }
        except Exception as exc:
            self.step_status["ingest"] = "error"
            logger.exception("Step 1 failed")
            raise RuntimeError(f"Ingest failed: {exc}") from exc

    # ------------------------------------------------------------------
    # Step 2 – Validate Schema
    # ------------------------------------------------------------------

    def validate_schema(self) -> dict[str, Any]:
        """Inspect the loaded table for missing values, nulls, and type issues.

        Returns
        -------
        dict with keys: missing_counts, blank_counts, type_issues, warnings
        """
        logger.info("Step 2 – Validating schema")
        self._require_loaded()

        missing_counts: dict[str, int] = {}
        blank_counts: dict[str, int] = {}
        type_issues: list[str] = []
        warnings: list[str] = []

        for col_info in self.schema_info:
            col = col_info["column_name"]
            col_type = col_info["column_type"].upper()

            # NULL counts
            null_row = self.conn.execute(
                f'SELECT COUNT(*) FROM {self.table_name} WHERE "{col}" IS NULL'
            ).fetchone()
            null_count = int(null_row[0]) if null_row else 0
            if null_count:
                missing_counts[col] = null_count
                warnings.append(f"Column '{col}' has {null_count} NULL value(s).")

            # Blank/whitespace string counts for VARCHAR columns
            if "VARCHAR" in col_type or "TEXT" in col_type or "CHAR" in col_type:
                blank_row = self.conn.execute(
                    f"SELECT COUNT(*) FROM {self.table_name} "
                    f"WHERE trim(CAST(\"{col}\" AS VARCHAR)) = ''"
                ).fetchone()
                blank_count = int(blank_row[0]) if blank_row else 0
                if blank_count:
                    blank_counts[col] = blank_count
                    warnings.append(
                        f"Column '{col}' has {blank_count} blank/whitespace-only value(s)."
                    )

            # Detect numeric-looking strings (potential type mismatches)
            if "VARCHAR" in col_type or "TEXT" in col_type or "CHAR" in col_type:
                try:
                    mismatch_row = self.conn.execute(
                        f"SELECT COUNT(*) FROM {self.table_name} "
                        f"WHERE \"{col}\" ~ '^[0-9]+(\\.[0-9]+)?$'"
                    ).fetchone()
                    mismatch_count = int(mismatch_row[0]) if mismatch_row else 0
                    if mismatch_count == self.row_count and self.row_count > 0:
                        type_issues.append(
                            f"Column '{col}' appears numeric but stored as {col_type}."
                        )
                except Exception:
                    pass  # regex not supported for this column; skip

        self.step_status["validate_schema"] = "ok"
        logger.info("Step 2 complete – %d warnings", len(warnings))
        return {
            "missing_counts": missing_counts,
            "blank_counts": blank_counts,
            "type_issues": type_issues,
            "warnings": warnings,
        }

    # ------------------------------------------------------------------
    # Step 3 – Clean Data
    # ------------------------------------------------------------------

    def clean_data(self) -> dict[str, Any]:
        """Apply cleaning transformations on the in-memory table.

        Transformations:
        - Blank strings in numeric-looking columns → '0'
        - Whitespace stripped from all VARCHAR columns
        - Binary 0/1 VARCHAR columns standardised to No/Yes

        Returns
        -------
        dict with keys: transformations_applied (list of descriptions)
        """
        logger.info("Step 3 – Cleaning data")
        self._require_loaded()

        transformations: list[str] = []

        for col_info in self.schema_info:
            col = col_info["column_name"]
            col_type = col_info["column_type"].upper()
            quoted = f'"{col}"'

            if "VARCHAR" in col_type or "TEXT" in col_type or "CHAR" in col_type:
                # Strip leading/trailing whitespace
                self.conn.execute(
                    f"UPDATE {self.table_name} "
                    f"SET {quoted} = trim(CAST({quoted} AS VARCHAR))"
                )
                transformations.append(f"Stripped whitespace from '{col}'.")

                # Replace blank strings with '0' if column looks numeric after stripping
                try:
                    numeric_check = self.conn.execute(
                        f"SELECT COUNT(*) FROM {self.table_name} "
                        f"WHERE {quoted} != '' "
                        f"AND NOT ({quoted} ~ '^-?[0-9]+(\\.[0-9]+)?$')"
                    ).fetchone()
                    non_numeric = int(numeric_check[0]) if numeric_check else 1
                    if non_numeric == 0:
                        self.conn.execute(
                            f"UPDATE {self.table_name} "
                            f"SET {quoted} = '0' WHERE {quoted} = ''"
                        )
                        transformations.append(
                            f"Replaced blank strings with '0' in numeric-string column '{col}'."
                        )
                except Exception:
                    pass

                # Standardise binary 0/1 → No/Yes for VARCHAR columns
                try:
                    distinct = self.conn.execute(
                        f"SELECT DISTINCT {quoted} FROM {self.table_name} "
                        f"WHERE {quoted} IS NOT NULL"
                    ).fetchall()
                    values = {str(r[0]).strip() for r in distinct}
                    if values <= {"0", "1"}:
                        self.conn.execute(
                            f"UPDATE {self.table_name} "
                            f"SET {quoted} = CASE {quoted} "
                            f"WHEN '1' THEN 'Yes' WHEN '0' THEN 'No' "
                            f"ELSE {quoted} END"
                        )
                        transformations.append(
                            f"Standardised binary encoding in '{col}' (0→No, 1→Yes)."
                        )
                except Exception:
                    pass

        # Standardise integer binary 0/1 → No/Yes (converts column to VARCHAR)
        for col_info in self.schema_info:
            col = col_info["column_name"]
            col_type = col_info["column_type"].upper()
            quoted = f'"{col}"'
            if any(t in col_type for t in ("INTEGER", "BIGINT", "SMALLINT", "HUGEINT", "TINYINT")):
                try:
                    distinct = self.conn.execute(
                        f"SELECT DISTINCT {quoted} FROM {self.table_name} "
                        f"WHERE {quoted} IS NOT NULL"
                    ).fetchall()
                    values = {r[0] for r in distinct}
                    if values <= {0, 1}:
                        self.conn.execute(
                            f"ALTER TABLE {self.table_name} ALTER COLUMN {quoted} TYPE VARCHAR"
                        )
                        self.conn.execute(
                            f"UPDATE {self.table_name} "
                            f"SET {quoted} = CASE {quoted} "
                            f"WHEN '1' THEN 'Yes' WHEN '0' THEN 'No' "
                            f"ELSE {quoted} END"
                        )
                        transformations.append(
                            f"Standardised binary encoding in '{col}' (0→No, 1→Yes)."
                        )
                except Exception:
                    pass

        self.step_status["clean"] = "ok"
        logger.info("Step 3 complete – %d transformation(s)", len(transformations))
        return {"transformations_applied": transformations}

    # ------------------------------------------------------------------
    # Step 5 – Filter Data
    # ------------------------------------------------------------------

    def filter_data(self, parsed_intent: dict[str, Any]) -> str:
        """Build a WHERE clause from the parsed intent's filters.

        Parameters
        ----------
        parsed_intent:
            The structured intent produced by QueryParser.parse().

        Returns
        -------
        SQL WHERE clause string (may be empty string if no filters).
        """
        logger.info("Step 5 – Building filter clause")
        filters: list[dict[str, Any]] = parsed_intent.get("filters", [])
        clauses: list[str] = []

        for f in filters:
            col = f.get("column", "")
            op = f.get("operator", "=")
            val = f.get("value", "")
            if not col:
                continue
            quoted_col = f'"{col}"'
            safe_val = str(val).replace("'", "''")
            if op in ("=", "!=", ">", "<", ">=", "<="):
                clauses.append(f"{quoted_col} {op} '{safe_val}'")
            elif op == "LIKE":
                clauses.append(f"{quoted_col} LIKE '%{safe_val}%'")

        where_clause = " AND ".join(clauses)
        self.step_status["filter"] = "ok"
        logger.info("Step 5 complete – WHERE clause: %s", where_clause or "(none)")
        return where_clause

    # ------------------------------------------------------------------
    # Step 6 – Aggregate & Compute
    # ------------------------------------------------------------------

    def aggregate(
        self, parsed_intent: dict[str, Any], where_clause: str = ""
    ) -> pd.DataFrame:
        """Execute GROUP BY + metric computation via DuckDB SQL.

        Parameters
        ----------
        parsed_intent:
            Structured intent from QueryParser.
        where_clause:
            Optional WHERE clause from Step 5.

        Returns
        -------
        pandas DataFrame with query results.
        """
        logger.info("Step 6 – Aggregating data")
        self._require_loaded()

        metric: str = parsed_intent.get("metric", "count")
        group_by: list[str] = parsed_intent.get("group_by", [])
        metric_col: str = parsed_intent.get("metric_column", "")

        # Build SELECT expressions
        select_parts: list[str] = []
        for col in group_by:
            select_parts.append(f'"{col}"')

        if metric == "count":
            select_parts.append("COUNT(*) AS count")
        elif metric == "churn_rate":
            # Support BOOLEAN (true), VARCHAR ('Yes','1'), and INTEGER (1) churn columns
            churn_col = metric_col or self._find_column("churn")
            if churn_col:
                select_parts.append(
                    f"ROUND(100.0 * SUM(CASE WHEN CAST(\"{churn_col}\" AS VARCHAR) "
                    f"IN ('Yes', '1', 'true') "
                    f"THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0), 2) AS churn_rate"
                )
            else:
                select_parts.append("COUNT(*) AS count")
        elif metric == "average":
            if metric_col:
                select_parts.append(
                    f'ROUND(AVG(TRY_CAST("{metric_col}" AS DOUBLE)), 4) AS avg_{metric_col}'
                )
            else:
                select_parts.append("COUNT(*) AS count")
        elif metric == "sum":
            if metric_col:
                select_parts.append(
                    f'ROUND(SUM(TRY_CAST("{metric_col}" AS DOUBLE)), 4) AS sum_{metric_col}'
                )
            else:
                select_parts.append("COUNT(*) AS count")
        else:
            select_parts.append("COUNT(*) AS count")

        sql = f"SELECT {', '.join(select_parts)} FROM {self.table_name}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        if group_by:
            group_exprs = ", ".join(f'"{c}"' for c in group_by)
            sql += f" GROUP BY {group_exprs} ORDER BY {group_exprs}"

        logger.info("Step 6 SQL: %s", sql)
        try:
            df: pd.DataFrame = self.conn.execute(sql).df()
            self.step_status["aggregate"] = "ok"
            logger.info("Step 6 complete – %d result row(s)", len(df))
            return df
        except Exception as exc:
            self.step_status["aggregate"] = "error"
            logger.exception("Step 6 failed")
            raise RuntimeError(f"Aggregation failed: {exc}") from exc

    # ------------------------------------------------------------------
    # Step 7 – Validate Results
    # ------------------------------------------------------------------

    def validate_results(self, df: pd.DataFrame) -> dict[str, Any]:
        """Check result DataFrame for common issues.

        Parameters
        ----------
        df:
            The DataFrame returned by aggregate().

        Returns
        -------
        dict with keys: valid (bool), issues (list of str), row_count (int)
        """
        logger.info("Step 7 – Validating results")
        issues: list[str] = []

        if df is None or df.empty:
            issues.append("Query returned no rows.")
            self.step_status["validate_results"] = "warning"
            return {"valid": False, "issues": issues, "row_count": 0}

        # Check for NaN values in numeric columns
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                nan_count = int(df[col].isna().sum())
                if nan_count:
                    issues.append(f"Column '{col}' has {nan_count} NaN value(s).")

        # Division-by-zero guard: if a rate column is 0 everywhere, warn
        rate_cols = [c for c in df.columns if "rate" in c.lower()]
        for col in rate_cols:
            if df[col].dropna().eq(0).all():
                issues.append(f"Column '{col}' is all zeros — possible division-by-zero.")

        row_count = len(df)
        valid = len(issues) == 0
        self.step_status["validate_results"] = "ok" if valid else "warning"
        logger.info("Step 7 complete – valid=%s, issues=%s", valid, issues)
        return {"valid": valid, "issues": issues, "row_count": row_count}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _require_loaded(self) -> None:
        if not self.column_names:
            raise RuntimeError("No data loaded. Call ingest() first.")

    def _find_column(self, keyword: str) -> str:
        """Return the first column whose name contains *keyword* (case-insensitive)."""
        for col in self.column_names:
            if keyword.lower() in col.lower():
                return col
        return ""

    def get_column_names(self) -> list[str]:
        return list(self.column_names)

    def get_schema_info(self) -> list[dict[str, Any]]:
        return list(self.schema_info)
