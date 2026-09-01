# Hospital readmission risk — data guide

Inpatient admissions with the vitals, labs and clinical notes recorded during each stay, plus whether the patient was readmitted within 30 days of discharge. Used to analyse what drives avoidable readmissions.

Database `mistral_demo`, reached through the Cloudera Iceberg MCP connector (`get_schema`, `execute_query`). Impala SQL, read-only.

_Generated 2026-09-01 and validated against the live schema: every column and metric below was verified to exist and execute._

## Entities

### Patient — `patients`

A person who has had at least one inpatient admission. **Grain:** One row per patient.

| Column | Type | Meaning |
|---|---|---|
| `patient_id` | string |  |
| `full_name` | string |  |
| `date_of_birth` | date | DATE. |
| `age` | int | Age in years at the time the dataset was generated. |
| `sex` | string |  |
| `primary_condition` | string | The patient's main chronic condition. Patient-level, not per-admission. |

### Admission — `admissions`

A single inpatient stay, from admit through discharge. **Grain:** One row per hospital admission. A patient can appear many times.

| Column | Type | Meaning |
|---|---|---|
| `admission_id` | string |  |
| `patient_id` | string |  |
| `admit_date` | date |  |
| `discharge_date` | date |  |
| `length_of_stay_days` | int | Integer days. Equals DATEDIFF(discharge_date, admit_date). |
| `admitting_diagnosis` | string | Reason for THIS admission. Usually matches the patient's primary_condition but is the correct column for per-admission analysis. |
| `department` | string |  |
| `prior_admissions_90d` | int | Count of this patient's admissions in the 90 days before this one. |
| `days_since_discharge` | int | Days since this patient's previous discharge. NULL for a first admission. |
| `follow_up_scheduled` | boolean | BOOLEAN. Whether a follow-up appointment was booked before discharge. |
| `discharge_disposition` | string |  |
| `readmission_risk_flag` | boolean | BOOLEAN prediction made at discharge. NOT the outcome - it is deliberately imperfect. Never use it as a substitute for readmitted_within_30d. |
| `readmitted_within_30d` | boolean | BOOLEAN outcome. True if the patient was readmitted within 30 days of this discharge. This is ground truth. |

### Observation — `vitals_labs`

Vitals and lab results recorded during a stay. **Grain:** One row per measurement per admission.

| Column | Type | Meaning |
|---|---|---|
| `patient_id` | string |  |
| `admission_id` | string |  |
| `metric` | string | Name of the measurement. Units differ per metric. |
| `value` | double | DOUBLE. Only comparable within a single metric. |
| `unit` | string |  |
| `recorded_at` | date |  |
| `abnormal` | boolean | BOOLEAN. True if the value falls outside that metric's reference range. |

### ClinicalNote — `clinical_notes`

Free-text clinical documentation written during the stay. **Grain:** One row per note. Several notes per admission.

| Column | Type | Meaning |
|---|---|---|
| `note_id` | string |  |
| `patient_id` | string |  |
| `admission_id` | string |  |
| `note_type` | string | Admission Note or Discharge Summary. |
| `author_role` | string |  |
| `note_date` | date |  |
| `note_text` | string | Unstructured text. Search it with LIKE / ILIKE. |

## How the tables join

| From | To | Cardinality | Note |
|---|---|---|---|
| `admissions.patient_id` | `patients.patient_id` | many-to-one | The standard join for per-admission analysis broken down by patient attributes. |
| `vitals_labs.admission_id` | `admissions.admission_id` | many-to-one | Fans out. Aggregate the observations first, or count admissions with COUNT(DISTINCT admission_id). |
| `clinical_notes.admission_id` | `admissions.admission_id` | many-to-one | Fans out. Filter by note_type to get one row per admission. |

## Metrics

Use these definitions verbatim. The current value over the whole dataset is shown so you can sanity-check a result.

| Metric | SQL | Over | Current |
|---|---|---|---|
| **admissions_count** — Number of admissions. | `COUNT(*)` | `admissions` | 1147 |
| **patients_count** — Number of distinct patients. | `COUNT(DISTINCT patient_id)` | `admissions` | 500 |
| **readmission_rate_30d** — Share of admissions followed by a readmission within 30 days. The headline metric. | `ROUND(100 * AVG(CAST(readmitted_within_30d AS INT)), 1)` | `admissions` | 26.8 percent |
| **follow_up_rate** — Share of admissions where a follow-up appointment was scheduled before discharge. | `ROUND(100 * AVG(CAST(follow_up_scheduled AS INT)), 1)` | `admissions` | 74.2 percent |
| **avg_length_of_stay** — Mean length of stay in days. | `ROUND(AVG(length_of_stay_days), 1)` | `admissions` | 3.8 days |
| **risk_flag_precision** — Of admissions flagged as high risk at discharge, the share actually readmitted. | `ROUND(100 * SUM(CASE WHEN readmission_risk_flag AND readmitted_within_30d THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN readmission_risk_flag THEN 1 ELSE 0 END), 0), 1)` | `admissions` | 36.7 percent |
| **risk_flag_recall** — Of admissions that were readmitted, the share the discharge flag caught. | `ROUND(100 * SUM(CASE WHEN readmission_risk_flag AND readmitted_within_30d THEN 1 ELSE 0 END) / NULLIF(SUM(CASE WHEN readmitted_within_30d THEN 1 ELSE 0 END), 0), 1)` | `admissions` | 61.9 percent |
| **abnormal_result_rate** — Share of recorded observations outside the reference range. | `ROUND(100 * AVG(CAST(abnormal AS INT)), 1)` | `vitals_labs` | 29.4 percent |

## Dimensions

- **`admitting_diagnosis`** (`admissions`): `Asthma`, `Atrial fibrillation`, `COPD exacerbation`, `Cellulitis`, `Chronic kidney disease`, `Congestive heart failure`, `Hypertension`, `Pneumonia`, `Post-surgical recovery - appendectomy`, `Type 2 diabetes with complications`
- **`department`** (`admissions`): `Cardiology`, `Endocrinology`, `General Medicine`, `Nephrology`, `Pulmonology`, `Surgery`
- **`discharge_disposition`** (`admissions`): `Home`, `Home with home health`, `Inpatient rehabilitation`, `Left against medical advice`, `Skilled nursing facility`
- **`follow_up_scheduled`** (`admissions`)
- **`prior_admissions_90d`** (`admissions`)
- **`metric`** (`vitals_labs`): `BNP`, `Creatinine`, `Diastolic BP`, `HbA1c`, `Heart Rate`, `O2 Saturation`, `Peak Flow`, `Systolic BP`, `Temperature`, `WBC`
- **`sex`** (`patients`): `F`, `M`

## Rules and pitfalls

- The grain of `admissions` is one row per ADMISSION, not per patient. Counting rows counts stays. Use COUNT(DISTINCT patient_id) for people.
- `readmission_risk_flag` is a prediction; `readmitted_within_30d` is what actually happened. Answer whether a patient was readmitted with the latter, always.
- Impala will not average a BOOLEAN. Cast it: AVG(CAST(col AS INT)).
- `vitals_labs.value` mixes units across metrics. Always filter to a single `metric` before averaging.
- Dates are real DATE columns. Use DATEDIFF(a, b) and plain literals like '2026-01-01'. No string casting needed.
- Joining `vitals_labs` or `clinical_notes` to `admissions` multiplies admission rows. Aggregate first, or use COUNT(DISTINCT admission_id).
- `days_since_discharge` is NULL for a patient's first admission; that is expected, not missing data.
