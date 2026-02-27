"""
app.py – Streamlit web UI for the 8-step data analysis pipeline.

Run with:
    streamlit run app.py
"""

import json
import logging
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from pipeline import OutputBuilder, QueryParser, TruthEngine

logging.basicConfig(level=logging.INFO)

# ---------------------------------------------------------------------------
# Page configuration
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="DataWeb Pipeline",
    page_icon="🔍",
    layout="wide",
)

st.title("🔍 8-Step Data Analysis Pipeline")
st.caption("Upload a CSV, inspect its schema, ask natural-language questions, and get structured results.")

# ---------------------------------------------------------------------------
# Session-state initialisation
# ---------------------------------------------------------------------------
if "engine" not in st.session_state:
    st.session_state.engine: TruthEngine | None = None
if "ingest_result" not in st.session_state:
    st.session_state.ingest_result: dict[str, Any] | None = None
if "validation_result" not in st.session_state:
    st.session_state.validation_result: dict[str, Any] | None = None
if "clean_result" not in st.session_state:
    st.session_state.clean_result: dict[str, Any] | None = None
if "last_output" not in st.session_state:
    st.session_state.last_output: dict[str, Any] | None = None

# ---------------------------------------------------------------------------
# Sidebar – CSV upload and pipeline steps 1-3
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("📁 Upload CSV")
    uploaded_file = st.file_uploader("Drag & drop or browse", type=["csv"])

    if uploaded_file is not None:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmp:
            tmp.write(uploaded_file.read())
            tmp_path = tmp.name

        engine = TruthEngine()
        st.session_state.engine = engine

        # Step 1 – Ingest
        with st.spinner("Step 1 – Ingesting data…"):
            try:
                ingest_result = engine.ingest(tmp_path)
                st.session_state.ingest_result = ingest_result
                st.success(
                    f"✅ Loaded **{ingest_result['row_count']:,}** rows × "
                    f"**{len(ingest_result['columns'])}** columns"
                )
            except Exception as exc:
                st.error(f"❌ Ingest failed: {exc}")
                st.stop()

        # Step 2 – Validate Schema
        with st.spinner("Step 2 – Validating schema…"):
            try:
                validation_result = engine.validate_schema()
                st.session_state.validation_result = validation_result
                n_warn = len(validation_result["warnings"])
                if n_warn:
                    st.warning(f"⚠️ {n_warn} schema warning(s) found")
                else:
                    st.success("✅ Schema looks clean")
            except Exception as exc:
                st.error(f"❌ Schema validation failed: {exc}")

        # Step 3 – Clean Data
        with st.spinner("Step 3 – Cleaning data…"):
            try:
                clean_result = engine.clean_data()
                st.session_state.clean_result = clean_result
                n_tx = len(clean_result["transformations_applied"])
                if n_tx:
                    st.info(f"🔧 {n_tx} transformation(s) applied")
                else:
                    st.success("✅ No cleaning needed")
            except Exception as exc:
                st.error(f"❌ Data cleaning failed: {exc}")

        Path(tmp_path).unlink(missing_ok=True)

# ---------------------------------------------------------------------------
# Main area
# ---------------------------------------------------------------------------
engine: TruthEngine | None = st.session_state.engine
ingest_result: dict[str, Any] | None = st.session_state.ingest_result

if engine is None or ingest_result is None:
    st.info("⬅️ Upload a CSV file in the sidebar to get started.")
    st.stop()

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab_schema, tab_quality, tab_query, tab_output, tab_steps = st.tabs(
    ["📋 Schema", "🔎 Data Quality", "💬 Query", "📊 Results", "🗂️ Pipeline Steps"]
)

# ---- Schema tab ------------------------------------------------------------
with tab_schema:
    st.subheader("Detected Schema")
    schema_rows = [
        {"Column": info["column_name"], "Type": info["column_type"]}
        for info in engine.get_schema_info()
    ]
    st.dataframe(pd.DataFrame(schema_rows), use_container_width=True, hide_index=True)

    st.subheader(f"Sample Data (first 5 rows of {ingest_result['row_count']:,} total)")
    if ingest_result["sample"]:
        st.dataframe(
            pd.DataFrame(ingest_result["sample"]),
            use_container_width=True,
            hide_index=True,
        )

# ---- Data Quality tab ------------------------------------------------------
with tab_quality:
    vr: dict[str, Any] | None = st.session_state.validation_result
    if vr is None:
        st.info("Validation results will appear here after upload.")
    else:
        col1, col2, col3 = st.columns(3)
        col1.metric("Columns with NULLs", len(vr["missing_counts"]))
        col2.metric("Columns with Blank Strings", len(vr["blank_counts"]))
        col3.metric("Potential Type Issues", len(vr["type_issues"]))

        if vr["warnings"]:
            st.subheader("⚠️ Warnings")
            for w in vr["warnings"]:
                st.warning(w)
        else:
            st.success("No data quality issues detected.")

        if vr["type_issues"]:
            st.subheader("🔤 Possible Type Mismatches")
            for t in vr["type_issues"]:
                st.info(t)

        cr: dict[str, Any] | None = st.session_state.clean_result
        if cr and cr["transformations_applied"]:
            st.subheader("🔧 Cleaning Transformations Applied")
            for tx in cr["transformations_applied"]:
                st.success(tx)

# ---- Query tab -------------------------------------------------------------
with tab_query:
    st.subheader("💬 Natural-Language Query (Step 4)")
    st.caption(
        "Examples: *What is the churn rate by gender?* · "
        "*Show average monthly charges by contract type* · "
        "*Count customers by internet service*"
    )

    query_text = st.text_input(
        "Enter your query:",
        placeholder="e.g. What is the churn rate by gender?",
    )

    run_query = st.button("▶️ Run Query", type="primary", disabled=(not query_text))

    if run_query and query_text:
        errors: list[str] = []
        output_builder = OutputBuilder()

        # Step 4 – Parse query intent
        parser = QueryParser(engine.get_column_names())
        parsed_intent = parser.parse(query_text)
        engine.step_status["parse_query"] = "ok"

        # Step 5 – Filter
        try:
            where_clause = engine.filter_data(parsed_intent)
        except Exception as exc:
            where_clause = ""
            errors.append(f"Filter step failed: {exc}")

        # Step 6 – Aggregate
        result_df: pd.DataFrame | None = None
        try:
            result_df = engine.aggregate(parsed_intent, where_clause)
        except Exception as exc:
            errors.append(f"Aggregation failed: {exc}")

        # Step 7 – Validate results
        validation = engine.validate_results(result_df if result_df is not None else pd.DataFrame())

        # Step 8 – Build output
        engine.step_status["output"] = "ok"
        output = output_builder.build(
            query=query_text,
            parsed_intent=parsed_intent,
            result_df=result_df,
            validation=validation,
            step_status=engine.step_status,
            errors=errors,
        )
        st.session_state.last_output = output

        if errors:
            for e in errors:
                st.error(e)
        elif not validation["valid"]:
            for issue in validation["issues"]:
                st.warning(issue)
        else:
            st.success(f"Query completed — {validation['row_count']} row(s) returned.")

# ---- Results tab -----------------------------------------------------------
with tab_output:
    output: dict[str, Any] | None = st.session_state.last_output
    if output is None:
        st.info("Run a query to see results here.")
    else:
        st.subheader("📊 Results Table")
        if output["results"]:
            st.dataframe(
                pd.DataFrame(output["results"]),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.warning("No results to display.")

        st.subheader("🗃️ Raw JSON Output")
        st.json(output)

# ---- Pipeline Steps tab ----------------------------------------------------
with tab_steps:
    st.subheader("🗂️ Pipeline Step Tracker")
    current_status = engine.step_status if engine else {}
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
    for key, label in ordered_steps:
        raw = current_status.get(key, "pending")
        if raw == "ok":
            icon, colour = "✅", "success"
        elif raw == "warning":
            icon, colour = "⚠️", "warning"
        elif raw == "error":
            icon, colour = "❌", "error"
        else:
            icon, colour = "⏳", "info"
        getattr(st, colour)(f"{icon} **{label}** — {raw}")
