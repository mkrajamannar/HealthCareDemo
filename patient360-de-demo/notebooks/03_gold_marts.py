# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold marts
# MAGIC Business-ready star schema for Power BI, built from Silver.
# MAGIC One row per patient in `gold_patient_360`, with a simple rule-based
# MAGIC readmission-risk score (demo logic, not a clinical model).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# --- gold_dim_patient (SCD2-ready: one current row per patient) ---
dim_patient = (spark.table("silver.silver_patients")
    .withColumn("age", F.floor(F.months_between(F.current_date(), "date_of_birth") / 12))
    .withColumn("is_current", F.lit(True)))
dim_patient.write.format("delta").mode("overwrite").saveAsTable("gold.gold_dim_patient")

# --- gold_fact_admissions: LOS + 30-day readmission flag ---
admissions = spark.table("silver.silver_admissions")
w = Window.partitionBy("patient_id").orderBy("admit_date")
admissions = (admissions
    .withColumn("prev_discharge", F.lag("discharge_date").over(w))
    .withColumn("days_since_prev_discharge",
                F.datediff("admit_date", "prev_discharge"))
    .withColumn("readmission_30d",
                F.when(F.col("days_since_prev_discharge") <= 30, 1).otherwise(0)))
admissions.write.format("delta").mode("overwrite").saveAsTable("gold.gold_fact_admissions")

# --- gold_fact_lab_results / claims / medications: Silver as-is, conformed ---
for src, dest in [("silver_lab_results", "gold_fact_lab_results"),
                  ("silver_claims", "gold_fact_claims"),
                  ("silver_medications", "gold_fact_medications")]:
    spark.table(f"silver.{src}").write.format("delta").mode("overwrite") \
        .saveAsTable(f"gold.{dest}")

# --- gold_patient_360: one row per patient ---
latest_adm = (admissions
    .withColumn("_rn", F.row_number().over(
        Window.partitionBy("patient_id").orderBy(F.col("admit_date").desc())))
    .filter("_rn = 1")
    .select("patient_id", "admission_id", "admit_date", "department",
            "diagnosis", "readmission_30d"))

adm_counts = (admissions.groupBy("patient_id")
    .agg(F.count("*").alias("admission_count"),
         F.sum("readmission_30d").alias("readmissions_30d"),
         F.avg("length_of_stay_days").alias("avg_los_days")))

claims_agg = (spark.table("silver.silver_claims").groupBy("patient_id")
    .agg(F.sum("billed_amount").alias("total_billed"),
         F.sum("paid_amount").alias("total_paid"),
         F.sum("denied_flag").alias("denied_claims")))

meds_agg = (spark.table("silver.silver_medications")
    .filter("status = 'active'").groupBy("patient_id")
    .agg(F.count("*").alias("active_medications")))

abnormal_labs = (spark.table("silver.silver_lab_results")
    .filter("abnormal_flag = 'Y'").groupBy("patient_id")
    .agg(F.count("*").alias("abnormal_lab_count")))

patient_360 = (dim_patient
    .join(adm_counts, "patient_id", "left")
    .join(latest_adm, "patient_id", "left")
    .join(claims_agg, "patient_id", "left")
    .join(meds_agg, "patient_id", "left")
    .join(abnormal_labs, "patient_id", "left")
    .na.fill({"admission_count": 0, "readmissions_30d": 0, "active_medications": 0,
              "abnormal_lab_count": 0, "denied_claims": 0})
    # Rule-based risk score (demo): prior readmission + frequent admissions
    # + abnormal labs + many active meds. 0-100, higher = higher risk.
    .withColumn("readmission_risk_score",
                (F.when(F.col("readmissions_30d") > 0, 40).otherwise(0)
                 + F.least(F.col("admission_count") * 10, 30)
                 + F.least(F.col("abnormal_lab_count") * 2, 20)
                 + F.least(F.col("active_medications") * 2, 10)))
    .withColumn("risk_band",
                F.when(F.col("readmission_risk_score") >= 60, "High")
                 .when(F.col("readmission_risk_score") >= 30, "Medium")
                 .otherwise("Low")))

patient_360.write.format("delta").mode("overwrite").saveAsTable("gold.gold_patient_360")

# COMMAND ----------

for t in ["gold_dim_patient", "gold_fact_admissions", "gold_fact_lab_results",
          "gold_fact_claims", "gold_fact_medications", "gold_patient_360"]:
    print(t, spark.table(f"gold.{t}").count())
spark.table("gold.gold_patient_360").groupBy("risk_band").count().show()
