"""
tests/test_pipeline.py – Unit tests for the 8-step data analysis pipeline.
"""

import csv
import os
import tempfile
from typing import Any

import pandas as pd
import pytest

from pipeline import OutputBuilder, QueryParser, TruthEngine


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_csv(tmp_path) -> str:
    """Write a small CSV file and return its path."""
    csv_path = tmp_path / "sample.csv"
    rows = [
        {"CustomerID": "1", "Gender": "Male", "Tenure": "12", "MonthlyCharges": "50.0", "Churn": "No"},
        {"CustomerID": "2", "Gender": "Female", "Tenure": "24", "MonthlyCharges": "70.5", "Churn": "Yes"},
        {"CustomerID": "3", "Gender": "Male", "Tenure": "6", "MonthlyCharges": "30.0", "Churn": "No"},
        {"CustomerID": "4", "Gender": "Female", "Tenure": "36", "MonthlyCharges": "90.0", "Churn": "Yes"},
        {"CustomerID": "5", "Gender": "Male", "Tenure": "18", "MonthlyCharges": "60.0", "Churn": "No"},
    ]
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return str(csv_path)


@pytest.fixture()
def binary_csv(tmp_path) -> str:
    """CSV with 0/1 binary encoded column."""
    csv_path = tmp_path / "binary.csv"
    rows = [
        {"ID": "1", "Category": "A", "Flag": "0"},
        {"ID": "2", "Category": "B", "Flag": "1"},
        {"ID": "3", "Category": "A", "Flag": "1"},
    ]
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return str(csv_path)


@pytest.fixture()
def loaded_engine(sample_csv) -> TruthEngine:
    """Return a TruthEngine with sample data already ingested."""
    engine = TruthEngine()
    engine.ingest(sample_csv)
    return engine


# ---------------------------------------------------------------------------
# Step 1 – Ingest
# ---------------------------------------------------------------------------


class TestIngest:
    def test_ingest_returns_metadata(self, sample_csv):
        engine = TruthEngine()
        result = engine.ingest(sample_csv)
        assert result["row_count"] == 5
        assert "CustomerID" in result["columns"]
        assert len(result["sample"]) <= 5

    def test_ingest_sets_column_names(self, sample_csv):
        engine = TruthEngine()
        engine.ingest(sample_csv)
        assert "Gender" in engine.get_column_names()
        assert "Churn" in engine.get_column_names()

    def test_ingest_invalid_path_raises(self):
        engine = TruthEngine()
        with pytest.raises(RuntimeError):
            engine.ingest("/nonexistent/path/file.csv")


# ---------------------------------------------------------------------------
# Step 2 – Validate Schema
# ---------------------------------------------------------------------------


class TestValidateSchema:
    def test_validate_returns_dict_keys(self, loaded_engine):
        result = loaded_engine.validate_schema()
        assert "missing_counts" in result
        assert "blank_counts" in result
        assert "type_issues" in result
        assert "warnings" in result

    def test_validate_requires_loaded(self):
        engine = TruthEngine()
        with pytest.raises(RuntimeError):
            engine.validate_schema()

    def test_validate_clean_data_has_no_nulls(self, loaded_engine):
        result = loaded_engine.validate_schema()
        assert result["missing_counts"] == {}


# ---------------------------------------------------------------------------
# Step 3 – Clean Data
# ---------------------------------------------------------------------------


class TestCleanData:
    def test_clean_returns_transformations(self, loaded_engine):
        result = loaded_engine.clean_data()
        assert "transformations_applied" in result
        assert isinstance(result["transformations_applied"], list)

    def test_clean_requires_loaded(self):
        engine = TruthEngine()
        with pytest.raises(RuntimeError):
            engine.clean_data()

    def test_clean_standardises_binary(self, binary_csv):
        engine = TruthEngine()
        engine.ingest(binary_csv)
        result = engine.clean_data()
        txs = result["transformations_applied"]
        assert any("Flag" in t and "No" in t for t in txs)


# ---------------------------------------------------------------------------
# Step 4 – Parse Query Intent
# ---------------------------------------------------------------------------


class TestQueryParser:
    COLUMNS = ["CustomerID", "Gender", "Tenure", "MonthlyCharges", "Churn"]

    def test_parse_churn_rate(self):
        parser = QueryParser(self.COLUMNS)
        intent = parser.parse("What is the churn rate by Gender?")
        assert intent["metric"] == "churn_rate"
        assert "Gender" in intent["group_by"]

    def test_parse_average(self):
        parser = QueryParser(self.COLUMNS)
        intent = parser.parse("Show average MonthlyCharges by Gender")
        assert intent["metric"] == "average"
        assert "MonthlyCharges" in intent["metric_column"]
        assert "Gender" in intent["group_by"]

    def test_parse_count(self):
        parser = QueryParser(self.COLUMNS)
        intent = parser.parse("Count customers by Gender")
        assert intent["metric"] == "count"
        assert "Gender" in intent["group_by"]

    def test_parse_preserves_original_query(self):
        parser = QueryParser(self.COLUMNS)
        q = "How many by Tenure?"
        intent = parser.parse(q)
        assert intent["original_query"] == q


# ---------------------------------------------------------------------------
# Step 5 & 6 – Filter & Aggregate
# ---------------------------------------------------------------------------


class TestFilterAndAggregate:
    def test_filter_no_filters(self, loaded_engine):
        intent: dict[str, Any] = {
            "metric": "count", "metric_column": "", "group_by": [], "filters": []
        }
        where = loaded_engine.filter_data(intent)
        assert where == ""

    def test_aggregate_count_by_gender(self, loaded_engine):
        intent: dict[str, Any] = {
            "metric": "count", "metric_column": "", "group_by": ["Gender"], "filters": []
        }
        where = loaded_engine.filter_data(intent)
        df = loaded_engine.aggregate(intent, where)
        assert not df.empty
        assert "Gender" in df.columns
        assert "count" in df.columns

    def test_aggregate_churn_rate(self, loaded_engine):
        intent: dict[str, Any] = {
            "metric": "churn_rate",
            "metric_column": "Churn",
            "group_by": ["Gender"],
            "filters": [],
        }
        df = loaded_engine.aggregate(intent, "")
        assert not df.empty
        assert any("churn_rate" in c.lower() for c in df.columns)

    def test_aggregate_average(self, loaded_engine):
        intent: dict[str, Any] = {
            "metric": "average",
            "metric_column": "MonthlyCharges",
            "group_by": ["Gender"],
            "filters": [],
        }
        df = loaded_engine.aggregate(intent, "")
        assert not df.empty
        assert any("avg" in c.lower() for c in df.columns)

    def test_aggregate_requires_loaded(self):
        engine = TruthEngine()
        intent: dict[str, Any] = {
            "metric": "count", "metric_column": "", "group_by": [], "filters": []
        }
        with pytest.raises(RuntimeError):
            engine.aggregate(intent)


# ---------------------------------------------------------------------------
# Step 7 – Validate Results
# ---------------------------------------------------------------------------


class TestValidateResults:
    def test_empty_df_is_invalid(self, loaded_engine):
        result = loaded_engine.validate_results(pd.DataFrame())
        assert not result["valid"]
        assert result["row_count"] == 0

    def test_valid_df(self, loaded_engine):
        df = pd.DataFrame({"Gender": ["Male", "Female"], "count": [3, 2]})
        result = loaded_engine.validate_results(df)
        assert result["valid"]
        assert result["row_count"] == 2


# ---------------------------------------------------------------------------
# Step 8 – Output Builder
# ---------------------------------------------------------------------------


class TestOutputBuilder:
    def _make_intent(self) -> dict[str, Any]:
        return {
            "original_query": "Count by Gender",
            "metric": "count",
            "metric_column": "",
            "group_by": ["Gender"],
            "filters": [],
        }

    def test_output_structure(self, loaded_engine):
        intent = self._make_intent()
        df = pd.DataFrame({"Gender": ["Male", "Female"], "count": [3, 2]})
        validation = loaded_engine.validate_results(df)
        builder = OutputBuilder()
        output = builder.build(
            query="Count by Gender",
            parsed_intent=intent,
            result_df=df,
            validation=validation,
            step_status=loaded_engine.step_status,
        )
        assert output["status"] == "success"
        assert len(output["results"]) == 2
        assert output["metadata"]["metric"] == "count"
        assert "pipeline_steps" in output
        assert len(output["pipeline_steps"]) == 8

    def test_output_with_errors(self, loaded_engine):
        intent = self._make_intent()
        df = pd.DataFrame()
        validation = loaded_engine.validate_results(df)
        builder = OutputBuilder()
        output = builder.build(
            query="fail query",
            parsed_intent=intent,
            result_df=None,
            validation=validation,
            step_status=loaded_engine.step_status,
            errors=["Something went wrong"],
        )
        assert output["status"] == "error"
        assert output["errors"] == ["Something went wrong"]

    def test_output_nan_replaced_with_none(self, loaded_engine):
        import math

        intent = self._make_intent()
        df = pd.DataFrame({"Gender": ["Male", "Female"], "churn_rate": [float("nan"), 50.0]})
        validation = {"valid": True, "issues": [], "row_count": 2}
        builder = OutputBuilder()
        output = builder.build(
            query="churn by gender",
            parsed_intent=intent,
            result_df=df,
            validation=validation,
            step_status={},
        )
        churn_values = [r["churn_rate"] for r in output["results"]]
        assert churn_values[0] is None
        assert churn_values[1] == 50.0
