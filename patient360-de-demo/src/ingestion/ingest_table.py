"""Table ingestion: Azure SQL -> Bronze via JDBC (incremental by watermark).

Demo run (no live SQL): reads the CSV extracts in data/table/ and writes the
same Bronze layout the JDBC path produces, so the downstream notebooks run as-is.

Azure run (Databricks): uncomment the JDBC block and set the secrets in
Key Vault -- the query, watermark logic, and Bronze contract are identical.
"""
import shutil
from datetime import datetime, timezone
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[2]
TABLE_DIR = PROJECT / "data" / "table"
BRONZE = PROJECT / "data" / "bronze"

# Watermark: last admit_date loaded. Stored in a control table in production
# (Delta table `control.watermarks`); a local file here for the demo.
WATERMARK_FILE = PROJECT / "data" / "bronze" / "_watermark_admissions.txt"


def read_watermark() -> str:
    if WATERMARK_FILE.exists():
        return WATERMARK_FILE.read_text().strip()
    return "1900-01-01"


def write_watermark(value: str) -> None:
    WATERMARK_FILE.parent.mkdir(parents=True, exist_ok=True)
    WATERMARK_FILE.write_text(value)


# --- Azure JDBC version (run in Databricks; needs the SQL JDBC driver) ---
# def jdbc_read(spark, table: str, watermark: str):
#     jdbc_url = dbutils.secrets.get("patient360-kv", "sql-jdbc-url")
#     query = f"(SELECT * FROM dbo.{table} WHERE admit_date > '{watermark}')" \
#         if table == "admissions" else f"(SELECT * FROM dbo.{table})"
#     return (spark.read.format("jdbc")
#             .option("url", jdbc_url)
#             .option("dbtable", query)
#             .option("user", dbutils.secrets.get("patient360-kv", "sql-user"))
#             .option("password", dbutils.secrets.get("patient360-kv", "sql-password"))
#             .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
#             .load())


def ingest_table_local(table: str) -> Path:
    """Demo path: copy the table extract to Bronze with a batch stamp."""
    watermark = read_watermark()
    print(f"{table}: incremental load, watermark admit_date > {watermark}")
    batch_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    dest_dir = BRONZE / table / f"batch_id={batch_id}"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f"{table}.csv"
    shutil.copy2(TABLE_DIR / f"{table}.csv", dest)
    # In production the new watermark is MAX(admit_date) of the loaded batch.
    write_watermark(datetime.now(timezone.utc).date().isoformat())
    print(f"landed {table}.csv -> {dest}")
    return dest


if __name__ == "__main__":
    ingest_table_local("patients")
    ingest_table_local("admissions")
