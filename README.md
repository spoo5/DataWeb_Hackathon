# DataWeb Hackathon – 8-Step Data Analysis Pipeline

A fully automated, DuckDB-powered data analysis pipeline with a Streamlit web UI.  
Upload any CSV file, inspect its schema, clean the data automatically, and query it with plain English.

---

## Features

| Step | Name | What it does |
|------|------|-------------|
| 1 | **Ingest Data** | Loads uploaded CSV into DuckDB via `read_csv_auto` |
| 2 | **Validate Schema** | Detects nulls, blanks, and potential type mismatches |
| 3 | **Clean Data** | Strips whitespace, fills blank numeric strings, standardises 0/1 → No/Yes |
| 4 | **Parse Query Intent** | Converts natural-language into filters, group-by, and metric via rule-based matching |
| 5 | **Filter Data** | Applies a WHERE clause based on parsed intent |
| 6 | **Aggregate & Compute** | Runs GROUP BY + metric (count / average / sum / churn_rate) via DuckDB |
| 7 | **Validate Results** | Checks for empty results, NaN values, division-by-zero |
| 8 | **Structured Output** | Returns a clean JSON response with status, metadata, results, and pipeline tracker |

---

## Setup

```bash
pip install -r requirements.txt
```

Requirements:
- Python 3.9+
- `duckdb>=0.10.0`
- `pandas>=2.0.0`
- `streamlit>=1.32.0`

---

## Run

```bash
streamlit run app.py
```

Open the URL shown in your terminal (usually http://localhost:8501).

---

## Usage

1. **Upload** a CSV file using the sidebar uploader.
2. The pipeline automatically runs **Steps 1–3** (ingest, validate, clean).
3. Switch to the **Schema** tab to see column names, types, and a data preview.
4. Switch to the **Data Quality** tab to review any warnings.
5. Switch to the **Query** tab and type a natural-language question, e.g.:
   - *"What is the churn rate by gender?"*
   - *"Show average monthly charges by contract type"*
   - *"Count customers by internet service"*
6. Click **▶️ Run Query** to execute **Steps 4–8**.
7. View the formatted results table and raw JSON on the **Results** tab.
8. Track all 8 pipeline steps on the **Pipeline Steps** tab.

---

## Example Queries

```
What is the churn rate by gender?
Show average MonthlyCharges by Contract
Count customers by InternetService
How many customers by tenure?
Total charges by PaymentMethod
```

---

## Project Structure

```
├── app.py                  # Streamlit web UI (main entry point)
├── pipeline/
│   ├── __init__.py
│   ├── engine.py           # TruthEngine – DuckDB-powered engine (Steps 1-3, 5-7)
│   ├── query_parser.py     # QueryParser – NL query → structured intent (Step 4)
│   └── output_builder.py   # OutputBuilder – structured JSON response (Step 8)
├── requirements.txt        # Python dependencies
├── README.md               # This file
└── tests/
    ├── __init__.py
    └── test_pipeline.py    # Unit tests
```

---

## Running Tests

```bash
pip install pytest
pytest tests/
```
