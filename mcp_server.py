import sqlite3
import duckdb
import sys
import os
import io
import contextlib
import pandas as pd
from fastmcp import FastMCP

# Initialize FastMCP Server for Data Engineering & DuckDB
mcp = FastMCP("DuckDB-Python-DataEngineering-Server")

# Global REPL execution context
_REPL_GLOBALS = {
    "duckdb": duckdb,
    "pd": pd,
    "os": os,
    "sys": sys
}

# ─────────────────────────────────────────────────────────────────────────────
# 🐍 Python Execution Tools
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def execute_python_code(code: str) -> str:
    """
    Executes Python data engineering code in a persistent session.
    `duckdb`, `pd` (pandas), `os`, and `sys` are pre-imported into the session context.
    Returns printed stdout/stderr or evaluation results.
    """
    global _REPL_GLOBALS
    stdout = io.StringIO()
    stderr = io.StringIO()
    
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        try:
            try:
                compiled = compile(code.strip(), "<mcp-repl>", "eval")
                result = eval(compiled, _REPL_GLOBALS)
                if result is not None:
                    print(result)
            except SyntaxError:
                compiled = compile(code, "<mcp-repl>", "exec")
                exec(compiled, _REPL_GLOBALS)
        except Exception as e:
            import traceback
            traceback.print_exc(file=sys.stderr)
            
    output = stdout.getvalue()
    errors = stderr.getvalue()
    
    response = []
    if output:
        response.append(output)
    if errors:
        response.append(f"Errors/Traceback:\n{errors}")
        
    return "\n".join(response) if response else "Code executed successfully (no output)."


# Default DuckDB database path
DEFAULT_DB_PATH = r"C:\DuckDB\my_db.duckdb"

# ─────────────────────────────────────────────────────────────────────────────
# 🦆 DuckDB & Data Engineering Tools
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def query_duckdb(sql: str, db_path: str = DEFAULT_DB_PATH) -> str:
    """
    Executes a SQL query on a local DuckDB database file (defaulting to 'C:\\DuckDB\\my_db.duckdb' or custom path / ':memory:').
    Returns tabular query results. Supports both read and write SQL statements.
    """
    if db_path != ":memory:":
        dir_name = os.path.dirname(db_path)
        if dir_name and not os.path.exists(dir_name):
            return f"Error: Database directory '{dir_name}' does not exist."
            
    try:
        conn = duckdb.connect(db_path)
        df = conn.execute(sql).df()
        conn.close()
        
        if df.empty:
            return "Query executed successfully. Empty result set."
            
        return df.to_string(index=False)
    except Exception as e:
        return f"Error executing DuckDB query: {str(e)}"


@mcp.tool()
def duckdb_list_tables(db_path: str = DEFAULT_DB_PATH) -> str:
    """
    Lists all tables and views in a DuckDB database file (default: 'C:\\DuckDB\\my_db.duckdb') along with their row counts.
    """
    if not os.path.exists(db_path) and db_path != ":memory:":
        return f"Error: DuckDB file not found at {db_path}"
        
    try:
        conn = duckdb.connect(db_path)
        tables = conn.execute("SHOW TABLES").fetchall()
        if not tables:
            conn.close()
            return f"Database at '{db_path}' contains no tables."
            
        result_lines = [f"Database: {db_path}", "-" * 40]
        for item in tables:
            table_name = item[0]
            count = conn.execute(f"SELECT COUNT(*) FROM \"{table_name}\"").fetchone()[0]
            result_lines.append(f"Table: {table_name:<35} | Rows: {count:,}")
            
        conn.close()
        return "\n".join(result_lines)
    except Exception as e:
        return f"Error listing DuckDB tables: {str(e)}"


@mcp.tool()
def duckdb_describe_table(table_name: str, db_path: str = DEFAULT_DB_PATH, sample_rows: int = 5) -> str:
    """
    Describes the schema (columns, data types) of a table in DuckDB and displays sample rows (default: 'C:\\DuckDB\\my_db.duckdb').
    """
    if not os.path.exists(db_path) and db_path != ":memory:":
        return f"Error: DuckDB file not found at {db_path}"
        
    try:
        conn = duckdb.connect(db_path)
        schema_df = conn.execute(f"DESCRIBE SELECT * FROM \"{table_name}\"").df()
        sample_df = conn.execute(f"SELECT * FROM \"{table_name}\" LIMIT {sample_rows}").df()
        conn.close()
        
        output = [
            f"=== Schema for '{table_name}' ({db_path}) ===",
            schema_df.to_string(index=False),
            f"\n=== Top {sample_rows} Rows ===",
            sample_df.to_string(index=False)
        ]
        return "\n".join(output)
    except Exception as e:
        return f"Error describing table '{table_name}': {str(e)}"


@mcp.tool()
def duckdb_export_parquet(sql_or_table: str, output_parquet_path: str, db_path: str = DEFAULT_DB_PATH) -> str:
    """
    Exports a DuckDB query or table directly to a compressed Parquet file using DuckDB native fast exporter.
    `sql_or_table`: Can be a table name (e.g. "my_table") or a full SELECT query (e.g. "SELECT * FROM my_table WHERE age > 20").
    """
    try:
        conn = duckdb.connect(db_path)
        query = sql_or_table if sql_or_table.strip().upper().startswith("SELECT") else f"SELECT * FROM \"{sql_or_table}\""
        export_sql = f"COPY ({query}) TO '{output_parquet_path}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        conn.execute(export_sql)
        conn.close()
        
        file_size = os.path.getsize(output_parquet_path) / (1024 * 1024) if os.path.exists(output_parquet_path) else 0
        return f"Successfully exported to '{output_parquet_path}' ({file_size:.2f} MB)."
    except Exception as e:
        return f"Error exporting to Parquet: {str(e)}"


@mcp.tool()
def duckdb_import_file(table_name: str, file_path: str, db_path: str = DEFAULT_DB_PATH) -> str:
    """
    Imports a CSV or Parquet file directly into a DuckDB table with auto-detected schema.
    """
    if not os.path.exists(file_path):
        return f"Error: File not found at '{file_path}'"
        
    try:
        conn = duckdb.connect(db_path)
        ext = os.path.splitext(file_path)[1].lower()
        
        if ext == ".parquet":
            read_fn = f"read_parquet('{file_path}')"
        elif ext in [".csv", ".tsv", ".txt"]:
            read_fn = f"read_csv_auto('{file_path}')"
        else:
            conn.close()
            return f"Unsupported file format '{ext}'. Supported: .csv, .tsv, .parquet"
            
        conn.execute(f"CREATE TABLE IF NOT EXISTS \"{table_name}\" AS SELECT * FROM {read_fn}")
        count = conn.execute(f"SELECT COUNT(*) FROM \"{table_name}\"").fetchone()[0]
        conn.close()
        
        return f"Successfully imported '{file_path}' into table '{table_name}'. Total rows: {count:,}"
    except Exception as e:
        return f"Error importing file into DuckDB: {str(e)}"



# ─────────────────────────────────────────────────────────────────────────────
# 💾 SQLite Tools (Legacy Support)
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def query_sqlite(db_path: str, sql: str) -> str:
    """
    Executes a SQL query on a local SQLite database and returns the rows.
    """
    if not os.path.exists(db_path):
        return f"Error: SQLite database file not found at {db_path}"
        
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(sql)
        
        if cursor.description is None:
            conn.commit()
            rows_affected = conn.changes()
            conn.close()
            return f"Query executed successfully. Rows affected: {rows_affected}"
            
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
        conn.close()
        
        if not rows:
            return "Query completed successfully. Empty result set."
            
        header = " | ".join(columns)
        divider = "-" * len(header)
        data_rows = [" | ".join(str(val) for val in row) for row in rows]
        return "\n".join([header, divider] + data_rows)
        
    except Exception as e:
        return f"Error executing SQLite query: {str(e)}"


if __name__ == "__main__":
    mcp.run()
