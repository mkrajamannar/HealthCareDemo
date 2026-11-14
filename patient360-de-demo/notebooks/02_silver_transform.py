# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Silver transforms
# MAGIC Clean + conform Bronze into Silver. Typed columns, trimmed strings,
# MAGIC deduped by business key. Invalid rows go to `silver_quarantine` with a
# MAGIC reason code; the pipeline continues with valid rows.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

def quarantine(df, table: str, rule, reason: str):
    """Split off rows failing `rule` into the quarantine table."""
    bad = df.filter(~rule).withColumn("quarantine_reason", F.lit(reason))
    if bad.count():
        (bad.write.format("delta").mode("append")
            .saveAsTable("silver.silver_quarantine"))
        print(f"{table}: quarantined {bad.count()} rows ({reason})")
    return df.filter(rule)

def dedupe(df, keys):
    w = Window.partitionBy(*keys).orderBy(F.col("_load_timestamp").desc())
    return (df.withColumn("_rn", F.row_number().over(w))
              .filter("_rn = 1").drop("_rn"))

# COMMAND ----------

# --- silver_patients: typed + trimmed + deduped by patient_id ---
patients = (spark.table("bronze.bronze_patients")
    .withColumn("date_of_birth", F.to_date("date_of_birth"))
    .withColumn("first_name", F.trim("first_name"))
    .withColumn("last_name", F.trim("last_name")))
patients = quarantine(patients, "patients",
                      F.col("patient_id").isNotNull() & F.col("date_of_birth").isNotNull(),
                      "null business key")
patients = dedupe(patients, ["patient_id"])
patients.write.format("delta").mode("overwrite").saveAsTable("silver.silver_patients")

# --- silver_admissions: date rules + length of stay ---
admissions = (spark.table("bronze.bronze_admissions")
    .withColumn("admit_date", F.to_date("admit_date"))
    .withColumn("discharge_date", F.to_date("discharge_date"))
    .withColumn("length_of_stay_days",
                F.datediff("discharge_date", "admit_date")))
admissions = quarantine(admissions, "admissions",
                        F.col("discharge_date") >= F.col("admit_date"),
                        "discharge before admit")
admissions = dedupe(admissions, ["admission_id"])
admissions.write.format("delta").mode("overwrite").saveAsTable("silver.silver_admissions")

# --- silver_lab_results: numeric results, abnormal flag recomputed ---
labs = (spark.table("bronze.bronze_lab_results")
    .withColumn("result_value", F.col("result_value").cast("double"))
    .withColumn("result_date", F.to_date("result_date"))
    .withColumn("abnormal_flag",
                F.when((F.col("result_value") < F.col("reference_low")) |
                       (F.col("result_value") > F.col("reference_high")), "Y").otherwise("N")))
labs = dedupe(labs, ["lab_id"])
labs.write.format("delta").mode("overwrite").saveAsTable("silver.silver_lab_results")

# --- silver_claims: amounts typed, denial flag ---
claims = (spark.table("bronze.bronze_claims")
    .withColumn("billed_amount", F.col("billed_amount").cast("decimal(10,2)"))
    .withColumn("paid_amount", F.col("paid_amount").cast("decimal(10,2)"))
    .withColumn("claim_date", F.to_date("claim_date"))
    .withColumn("denied_flag", F.when(F.col("claim_status") == "Denied", 1).otherwise(0)))
claims = quarantine(claims, "claims",
                    F.col("billed_amount") >= F.col("paid_amount"),
                    "paid exceeds billed")
claims = dedupe(claims, ["claim_id"])
claims.write.format("delta").mode("overwrite").saveAsTable("silver.silver_claims")

# --- silver_medications: flatten FHIR MedicationRequest ---
meds = (spark.table("bronze.bronze_medications")
    .withColumn("patient_id", F.regexp_replace("subject.reference", "Patient/", ""))
    .withColumn("medication", F.col("medicationCodeableConcept.text"))
    .withColumn("dosage", F.col("dosageInstruction")[0]["text"])
    .withColumn("authored_on", F.to_date("authoredOn"))
    .withColumn("prescriber", F.col("requester.display"))
    .select("id", "patient_id", "medication", "dosage", "status",
            "authored_on", "prescriber")
    .withColumnRenamed("id", "medication_request_id"))
meds = dedupe(meds, ["medication_request_id"])
meds.write.format("delta").mode("overwrite").saveAsTable("silver.silver_medications")

# COMMAND ----------

for t in ["silver_patients", "silver_admissions", "silver_lab_results",
          "silver_claims", "silver_medications"]:
    print(t, spark.table(f"silver.{t}").count())
print("silver_quarantine", spark.table("silver.silver_quarantine").count())
