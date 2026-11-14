# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze ingestion
# MAGIC Reads the landed sources (table extracts, files, API bundle) into Bronze
# MAGIC Delta tables. Raw copy only: no cleaning, no dedupe. Adds load metadata.
# MAGIC Run after the ingestion scripts / ADF pipeline.

# COMMAND ----------

from pyspark.sql import functions as F

BRONZE_PATH = "/mnt/patient360/bronze"   # ADLS Gen2 Bronze container (mounted)
LANDING = "/mnt/patient360/landing"      # local demo: use the repo data/ folder

# COMMAND ----------

def to_bronze(df, table: str, source_file: str):
    """Append raw rows + load metadata to a Bronze Delta table."""
    out = (df
           .withColumn("_load_timestamp", F.current_timestamp())
           .withColumn("_source_file", F.lit(source_file))
           .withColumn("_batch_id", F.date_format(F.current_timestamp(), "yyyyMMdd_HHmmss")))
    (out.write.format("delta").mode("append")
        .saveAsTable(f"bronze.{table}"))
    print(f"bronze.{table}: +{df.count()} rows from {source_file}")

# COMMAND ----------

# Table sources (JDBC in production; CSV extracts in the local demo)
to_bronze(spark.read.option("header", True).csv(f"{LANDING}/table/patients.csv"),
          "bronze_patients", "patients.csv")
to_bronze(spark.read.option("header", True).csv(f"{LANDING}/table/admissions.csv"),
          "bronze_admissions", "admissions.csv")

# File sources
to_bronze(spark.read.option("header", True).csv(f"{LANDING}/file/lab_results.csv"),
          "bronze_lab_results", "lab_results.csv")
to_bronze(spark.read.option("multiLine", True).json(f"{LANDING}/file/claims.json"),
          "bronze_claims", "claims.json")

# API source: raw FHIR bundle, kept as one JSON string per entry
bundle = spark.read.option("multiLine", True).json(
    f"{LANDING}/api/fhir_medication_requests.json")
meds_raw = bundle.select(F.explode("entry").alias("entry")).select("entry.resource.*")
to_bronze(meds_raw, "bronze_medications", "fhir_medication_requests.json")

# COMMAND ----------

# Row-count check: every Bronze load logs its counts for the next step.
for t in ["bronze_patients", "bronze_admissions", "bronze_lab_results",
          "bronze_claims", "bronze_medications"]:
    print(t, spark.table(f"bronze.{t}").count())
