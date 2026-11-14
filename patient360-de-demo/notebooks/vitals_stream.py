# Databricks notebook source
# MAGIC %md
# MAGIC # Streaming vitals — Event Hubs -> Silver -> Gold hourly
# MAGIC Structured Streaming with a 10-minute watermark on `event_time`.
# MAGIC Production source is Event Hubs; the local demo reads the JSONL file
# MAGIC as a file stream, so the same code runs without Azure.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (IntegerType, StringType, StructField,
                               StructType, TimestampType, DoubleType)

vitals_schema = StructType([
    StructField("event_id", StringType()),
    StructField("patient_id", StringType()),
    StructField("device_id", StringType()),
    StructField("event_time", TimestampType()),
    StructField("heart_rate", IntegerType()),
    StructField("spo2", DoubleType()),
    StructField("temperature_f", DoubleType()),
    StructField("bp_systolic", IntegerType()),
    StructField("bp_diastolic", IntegerType()),
    StructField("respiratory_rate", IntegerType()),
    StructField("alert", StringType()),
])

USE_EVENT_HUBS = False  # set True in Azure; keep False for the local file demo
CHECKPOINT = "/mnt/patient360/checkpoints/vitals"

# COMMAND ----------

if USE_EVENT_HUBS:
    eh_conf = {
        "eventhubs.connectionString":
            dbutils.secrets.get("patient360-kv", "eventhubs-connection-string"),
        "eventhubs.consumerGroup": "$Default",
    }
    raw = (spark.readStream.format("eventhubs").options(**eh_conf).load()
           .select(F.from_json(F.col("body").cast("string"), vitals_schema).alias("v"))
           .select("v.*"))
else:
    raw = (spark.readStream.schema(vitals_schema).json("/mnt/patient360/landing/stream"))

vitals = (raw
    .withWatermark("event_time", "10 minutes")
    .withColumn("alert_flag",
                F.when((F.col("heart_rate") > 100) | (F.col("heart_rate") < 60) |
                       (F.col("spo2") < 93), 1).otherwise(0)))

# Silver: every reading, Delta sink, exactly-once via checkpointing
silver_query = (vitals.writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT}/silver_vitals")
    .outputMode("append")
    .toTable("silver.silver_vitals"))

# Alerts: only abnormal readings
alerts_query = (vitals.filter("alert_flag = 1").writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT}/vitals_alerts")
    .outputMode("append")
    .toTable("silver.silver_vitals_alerts"))

# Gold: hourly per-patient aggregates
hourly = (vitals
    .withWatermark("event_time", "10 minutes")
    .groupBy(F.window("event_time", "1 hour"), "patient_id")
    .agg(F.avg("heart_rate").alias("avg_heart_rate"),
         F.min("spo2").alias("min_spo2"),
         F.max("temperature_f").alias("max_temp_f"),
         F.sum("alert_flag").alias("alert_count"),
         F.count("*").alias("reading_count")))

gold_query = (hourly.writeStream
    .format("delta")
    .option("checkpointLocation", f"{CHECKPOINT}/vitals_hourly")
    .outputMode("append")
    .toTable("gold.gold_fact_vitals_hourly"))

# In Databricks, the queries run until stopped; check progress with:
#   spark.streams.active  /  silver_query.status
