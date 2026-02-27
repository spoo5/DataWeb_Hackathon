import duckdb

class TruthEngine:
    def __init__(self, db_path=":memory:"):
        self.conn = duckdb.connect(db_path)
        self.table_name = "data_store"

    def initial_load(self, csv_path):
        # 1. Ingest and infer
        self.conn.execute(f"CREATE TABLE {self.table_name} AS SELECT * FROM read_csv_auto('{csv_path}')")
        
        # 2. Get Schema for LLM
        schema = self.conn.execute(f"DESCRIBE {self.table_name}").df().to_json()
        return schema

    def run_analyst_step(self, generated_sql):
        """
        This is where the LLM's SQL is actually executed.
        """
        try:
            # We use a 'Relation' to get both data and metadata easily
            rel = self.conn.sql(generated_sql)
            df = rel.df()
            
            return {
                "success": True,
                "data": df.to_dict(orient="records"),
                "row_count": len(df),
                "columns": list(df.columns)
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

# --- LOGIC FLOW ---
# engine = TruthEngine()
# metadata = engine.initial_load("my_data.csv")
# # Send metadata to LLM... get back 'sql_query'
# result = engine.run_analyst_step(sql_query)