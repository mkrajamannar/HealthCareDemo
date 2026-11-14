# Data dictionary — Patient 360

All data is synthetic. Every dataset joins on `patient_id` (P0001–P0200).
Dates cover 5 Jan 2026 – 13 Nov 2026 unless noted.

## patients (table — Azure SQL)

| Column | Type | Notes |
|---|---|---|
| patient_id | string | PK, `P0001`–`P0200` |
| first_name | string | Synthetic |
| last_name | string | Synthetic |
| date_of_birth | date | 1940–2005 |
| gender | string | F / M |
| blood_type | string | A+, A-, B+, B-, AB+, O+, O- |
| city | string | US city |
| state | string | US state code |
| primary_physician | string | e.g. Dr. A. Patel |

200 rows. Source: `data/table/patients.csv`, DDL + seed in `data/table/admissions.sql`.

## admissions (table — Azure SQL)

| Column | Type | Notes |
|---|---|---|
| admission_id | string | PK, `A0001`–`A0350` |
| patient_id | string | FK -> patients |
| admit_date | date | |
| discharge_date | date | admit + 1–9 days |
| department | string | Cardiology, Oncology, Orthopedics, Neurology, Emergency, Pediatrics, Surgery, Internal Medicine |
| diagnosis_code | string | ICD-10, e.g. I10, E11.9 |
| diagnosis | string | Description |
| attending_physician | string | |
| bed_type | string | General, ICU, Step-down, Observation |
| admission_type | string | Elective, Emergency, Urgent |

350 rows. Source: `data/table/admissions.csv`.

## lab_results (file — CSV)

| Column | Type | Notes |
|---|---|---|
| lab_id | string | PK, `L00001`–`L01500` |
| patient_id | string | FK -> patients |
| test_code | string | LOINC, e.g. 2345-7 |
| test_name | string | Glucose, Hemoglobin, Cholesterol, Creatinine, Potassium, Sodium, ALT, AST |
| result_value | decimal | |
| unit | string | mg/dL, g/dL, mmol/L, U/L |
| reference_low | decimal | |
| reference_high | decimal | |
| abnormal_flag | string | Y if outside reference range |
| result_date | date | |

1,500 rows. Source: `data/file/lab_results.csv`.

## claims (file — JSON)

| Column | Type | Notes |
|---|---|---|
| claim_id | string | PK, `C0001`–`C0400` |
| patient_id | string | FK -> patients |
| admission_id | string | FK -> admissions, null for outpatient claims |
| procedure_code | string | CPT, e.g. 99213 |
| procedure | string | Description |
| claim_date | date | |
| billed_amount | decimal | |
| paid_amount | decimal | 0.00 if denied |
| payer | string | Aetna, UnitedHealthcare, BCBS, Cigna, Medicare, Medicaid, Self-pay |
| claim_status | string | Paid, Pending, Denied |

400 rows. Source: `data/file/claims.json`.

## medications (API — FHIR R4 MedicationRequest bundle)

Flattened fields after ingestion:

| Column | Type | Notes |
|---|---|---|
| medication_request_id | string | `medreq-0001`–`medreq-0300` |
| patient_id | string | FK -> patients, from `subject.reference` |
| medication | string | e.g. Lisinopril, Metformin |
| dosage | string | e.g. `10 mg, once daily` |
| status | string | active, completed, on-hold |
| authored_on | date | Order date |
| prescriber | string | |

300 entries. Source: `data/api/fhir_medication_requests.json`
(FHIR Bundle, `type: searchset`).

## vitals (stream — JSON lines)

| Column | Type | Notes |
|---|---|---|
| event_id | string | `V00001`–`V00600` |
| patient_id | string | FK -> patients |
| device_id | string | `MON-01`–`MON-40` |
| event_time | timestamp | Last 7 days to 13 Nov 2026 |
| heart_rate | int | bpm; alert if > 100 or < 60 |
| spo2 | decimal | % |
| temperature_f | decimal | Fahrenheit |
| bp_systolic | int | mmHg |
| bp_diastolic | int | mmHg |
| respiratory_rate | int | breaths/min |
| alert | string | Y/N |

600 events. Source: `data/stream/vitals.jsonl`. In production this arrives
via Event Hubs; the file is the local-demo equivalent.
