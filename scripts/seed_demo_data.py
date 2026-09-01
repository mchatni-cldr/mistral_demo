#!/usr/bin/env python3
"""Generate and load a synthetic readmission-risk dataset into CDW Iceberg.

The stock demo tables hold five rows each, so every GROUP BY returns 1 and the
interesting demo turns fall flat. This builds a dataset with enough volume for
real distributions, and with deliberately planted signal so the model can find
and quantify a genuine pattern:

    missed follow-up, prior admissions, abnormal labs, age and a handful of
    chronic conditions all raise the 30-day readmission rate.

`readmission_risk_flag` is a *predicted* flag recorded at discharge and is
deliberately imperfect, so "how well does the risk flag actually predict
readmission?" is a real question with a real answer.

All data is synthetic. Names are generated from fixed word lists.

    python scripts/seed_demo_data.py --yes
    python scripts/seed_demo_data.py --dry-run      # print counts, touch nothing
"""

import argparse
import os
import random
import sys
from datetime import date, timedelta

from dotenv import load_dotenv
from impala.dbapi import connect

SEED = 20260901
N_PATIENTS = 500
WINDOW_START = date(2025, 9, 1)
WINDOW_END = date(2026, 8, 20)

FIRST = """Aisha Liam Noor Mateo Sofia Omar Elena Kai Priya Idris Hana Tomas Zara Nils
Amara Yusuf Clara Dmitri Leila Ravi Ingrid Marco Fatima Jonas Mei Alvaro Nadia Sven
Rosa Hugo Anika Pablo Yara Lars Selin Andres Freya Karim Lucia Otto Ines Malik
Greta Rafael Sana Emil Bianca Tariq Maya Viktor""".split()
LAST = """Okafor Nguyen Silva Kowalski Haddad Lindqvist Moreau Rossi Dlamini Farrell
Vargas Novak Bergman Costa Ivanov Mensah Fischer Duarte Petrov Salazar Andersen
Bhatt Romero Jansen Kaur Molnar Ferreira Larsen Aziz Sorensen Pereira Kovac
Nakamura Oduya Rivera Halvorsen Bakker Tanaka Mwangi Escobar Lindgren Abadi
Schneider Marino Osei Vidal Karlsson Ahmed Blanco Reyes""".split()

# condition -> (base 30-day readmission rate, department, typical LOS mean)
CONDITIONS = {
    "Congestive heart failure":            (0.30, "Cardiology",       6.0),
    "COPD exacerbation":                   (0.26, "Pulmonology",      5.0),
    "Type 2 diabetes with complications":  (0.22, "Endocrinology",    4.5),
    "Chronic kidney disease":              (0.20, "Nephrology",       5.5),
    "Atrial fibrillation":                 (0.16, "Cardiology",       3.5),
    "Pneumonia":                           (0.15, "General Medicine", 4.5),
    "Cellulitis":                          (0.10, "General Medicine", 3.0),
    "Asthma":                              (0.09, "Pulmonology",      2.5),
    "Hypertension":                        (0.08, "General Medicine", 2.0),
    "Post-surgical recovery - appendectomy": (0.05, "Surgery",        2.0),
}

DISPOSITIONS = ["Home", "Home with home health", "Skilled nursing facility",
                "Inpatient rehabilitation", "Left against medical advice"]

# metric -> (unit, healthy mean, sd, (low, high) normal range)
METRICS = {
    "Systolic BP":    ("mmHg",   128, 18, (90, 140)),
    "Diastolic BP":   ("mmHg",    78, 11, (60, 90)),
    "Heart Rate":     ("bpm",     78, 14, (60, 100)),
    "Temperature":    ("C",      36.8, 0.6, (36.1, 37.5)),
    "O2 Saturation":  ("%",      96.5, 2.5, (94, 100)),
}
CONDITION_METRICS = {
    "Congestive heart failure":           ("BNP", "pg/mL", 620, 340, (0, 100)),
    "Type 2 diabetes with complications": ("HbA1c", "%", 8.6, 1.6, (4.0, 5.7)),
    "Chronic kidney disease":             ("Creatinine", "mg/dL", 2.4, 0.9, (0.6, 1.3)),
    "COPD exacerbation":                  ("Peak Flow", "L/min", 240, 70, (400, 700)),
    "Asthma":                             ("Peak Flow", "L/min", 330, 80, (400, 700)),
    "Pneumonia":                          ("WBC", "10^9/L", 14.5, 4.0, (4.0, 11.0)),
    "Cellulitis":                         ("WBC", "10^9/L", 13.0, 3.5, (4.0, 11.0)),
}


def q(value) -> str:
    """Render a Python value as an Impala SQL literal."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def build(rng):
    patients, admissions, labs, notes = [], [], [], []

    for i in range(1, N_PATIENTS + 1):
        pid = f"P{i:05d}"
        age = max(19, min(96, int(rng.gauss(64, 17))))
        dob = date(2026 - age, rng.randint(1, 12), rng.randint(1, 28))
        condition = rng.choices(list(CONDITIONS), weights=[
            3, 3, 3, 2, 2, 3, 2, 2, 3, 2])[0]
        patients.append({
            "patient_id": pid,
            "full_name": f"{rng.choice(FIRST)} {rng.choice(LAST)}",
            "date_of_birth": dob,
            "age": age,
            "sex": rng.choice(["F", "M"]),
            "primary_condition": condition,
        })

    span = (WINDOW_END - WINDOW_START).days
    admission_no = 0

    for p in patients:
        condition = p["primary_condition"]
        base, dept, los_mean = CONDITIONS[condition]
        # Sicker cohorts come back more often.
        n_adm = (1 + (rng.random() < 0.55 + base)
                   + (rng.random() < base * 2.2)
                   + (rng.random() < base * 1.2))
        prev_discharge = None

        for _ in range(n_adm):
            admission_no += 1
            aid = f"A{admission_no:06d}"
            if prev_discharge is None:
                admit = WINDOW_START + timedelta(days=rng.randint(0, max(1, span - 40)))
            else:
                admit = prev_discharge + timedelta(days=rng.randint(5, 120))
                if admit > WINDOW_END:
                    break
            los = max(1, min(21, int(rng.gauss(los_mean, 2.2))))
            discharge = admit + timedelta(days=los)
            prior_90 = sum(
                1 for a in admissions
                if a["patient_id"] == p["patient_id"]
                and 0 < (admit - a["discharge_date"]).days <= 90
            )
            days_since = ((admit - prev_discharge).days if prev_discharge else None)
            follow_up = rng.random() > (0.22 + 0.10 * (prior_90 > 0))

            # Condition-specific measurement, used both as a lab row and as a
            # driver of the outcome -- this is the signal the model can find.
            n_abnormal = 0
            rows = []
            for metric, (unit, mean, sd, (lo, hi)) in METRICS.items():
                val = round(rng.gauss(mean, sd), 1)
                abnormal = not (lo <= val <= hi)
                n_abnormal += abnormal
                rows.append((metric, val, unit, abnormal))
            if condition in CONDITION_METRICS:
                metric, unit, mean, sd, (lo, hi) = CONDITION_METRICS[condition]
                val = round(rng.gauss(mean, sd), 1)
                abnormal = not (lo <= val <= hi)
                n_abnormal += abnormal
                rows.append((metric, val, unit, abnormal))

            risk = base
            risk += 0.15 if not follow_up else 0.0
            risk += 0.06 * min(prior_90, 3)
            risk += 0.06 if p["age"] >= 75 else 0.0
            risk += 0.08 if n_abnormal >= 3 else 0.0
            risk += 0.04 if los > 7 else 0.0
            readmitted = rng.random() < min(risk, 0.85)

            # The discharge-time prediction: correlated with risk but wrong
            # often enough that measuring it is a real question.
            flag = (risk + rng.gauss(0, 0.09)) > 0.28

            disposition = rng.choices(
                DISPOSITIONS, weights=[55, 18, 14, 9, 4])[0]

            admissions.append({
                "admission_id": aid, "patient_id": p["patient_id"],
                "admit_date": admit, "discharge_date": discharge,
                "length_of_stay_days": los, "admitting_diagnosis": condition,
                "department": dept, "prior_admissions_90d": prior_90,
                "days_since_discharge": days_since,
                "follow_up_scheduled": follow_up,
                "discharge_disposition": disposition,
                "readmission_risk_flag": flag,
                "readmitted_within_30d": readmitted,
            })

            for k, (metric, val, unit, abnormal) in enumerate(rows):
                labs.append({
                    "patient_id": p["patient_id"], "admission_id": aid,
                    "metric": metric, "value": val, "unit": unit,
                    "recorded_at": admit + timedelta(days=min(k // 3, los)),
                    "abnormal": abnormal,
                })

            fu = ("Follow-up appointment scheduled within 7 days of discharge."
                  if follow_up else
                  "No follow-up appointment was scheduled prior to discharge.")
            notes.append({
                "note_id": f"N{len(notes)+1:06d}", "patient_id": p["patient_id"],
                "admission_id": aid, "note_type": "Admission Note",
                "author_role": rng.choice(["Attending Physician", "Resident"]),
                "note_date": admit,
                "note_text": (
                    f"{p['age']}-year-old patient admitted to {dept} with {condition.lower()}. "
                    f"{prior_90} prior admission(s) in the preceding 90 days. "
                    f"{'Multiple abnormal results on admission panel.' if n_abnormal >= 3 else 'Admission panel largely within reference range.'}"
                ),
            })
            notes.append({
                "note_id": f"N{len(notes)+1:06d}", "patient_id": p["patient_id"],
                "admission_id": aid, "note_type": "Discharge Summary",
                "author_role": rng.choice(["Attending Physician", "Case Manager"]),
                "note_date": discharge,
                "note_text": (
                    f"Discharged after {los} day(s) to {disposition.lower()}. "
                    f"{fu} "
                    f"{'Flagged as elevated readmission risk at discharge.' if flag else 'Not flagged as elevated readmission risk.'}"
                ),
            })

    return patients, admissions, labs, notes


TABLES = {
    "patients": ("""patient_id STRING, full_name STRING, date_of_birth DATE,
                    age INT, sex STRING, primary_condition STRING""",
                 ["patient_id", "full_name", "date_of_birth", "age", "sex",
                  "primary_condition"]),
    "admissions": ("""admission_id STRING, patient_id STRING, admit_date DATE,
                      discharge_date DATE, length_of_stay_days INT,
                      admitting_diagnosis STRING, department STRING,
                      prior_admissions_90d INT, days_since_discharge INT,
                      follow_up_scheduled BOOLEAN, discharge_disposition STRING,
                      readmission_risk_flag BOOLEAN,
                      readmitted_within_30d BOOLEAN""",
                   ["admission_id", "patient_id", "admit_date", "discharge_date",
                    "length_of_stay_days", "admitting_diagnosis", "department",
                    "prior_admissions_90d", "days_since_discharge",
                    "follow_up_scheduled", "discharge_disposition",
                    "readmission_risk_flag", "readmitted_within_30d"]),
    "vitals_labs": ("""patient_id STRING, admission_id STRING, metric STRING,
                       value DOUBLE, unit STRING, recorded_at DATE,
                       abnormal BOOLEAN""",
                    ["patient_id", "admission_id", "metric", "value", "unit",
                     "recorded_at", "abnormal"]),
    "clinical_notes": ("""note_id STRING, patient_id STRING, admission_id STRING,
                          note_type STRING, author_role STRING, note_date DATE,
                          note_text STRING""",
                       ["note_id", "patient_id", "admission_id", "note_type",
                        "author_role", "note_date", "note_text"]),
}
BATCH = 250


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--yes", action="store_true",
                    help="required: confirms DROPping and recreating the tables")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--env", default=os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))
    args = ap.parse_args()

    rng = random.Random(SEED)
    patients, admissions, labs, notes = build(rng)
    data = {"patients": patients, "admissions": admissions,
            "vitals_labs": labs, "clinical_notes": notes}

    for name, rows in data.items():
        print(f"  {name:<16} {len(rows):>6,} rows")
    def rate(rows):
        return sum(r["readmitted_within_30d"] for r in rows) / max(len(rows), 1)

    print(f"\n  overall 30-day readmission rate: {rate(admissions):.1%}")
    print("\n  planted signal (the model should be able to find these):")
    yes = [a for a in admissions if a["follow_up_scheduled"]]
    no = [a for a in admissions if not a["follow_up_scheduled"]]
    print(f"    follow-up scheduled      {rate(yes):>6.1%}  (n={len(yes)})")
    print(f"    no follow-up             {rate(no):>6.1%}  (n={len(no)})")
    for k in (0, 1, 2):
        sub = [a for a in admissions if a["prior_admissions_90d"] == k]
        if sub:
            print(f"    {k} prior admissions (90d) {rate(sub):>6.1%}  (n={len(sub)})")
    print("\n  by condition:")
    for cond in CONDITIONS:
        sub = [a for a in admissions if a["admitting_diagnosis"] == cond]
        if sub:
            print(f"    {cond[:38]:<40}{rate(sub):>6.1%}  (n={len(sub)})")
    flagged = [a for a in admissions if a["readmission_risk_flag"]]
    tp = sum(a["readmitted_within_30d"] for a in flagged)
    actual = sum(a["readmitted_within_30d"] for a in admissions)
    print(f"\n  risk flag: precision {tp/max(len(flagged),1):.1%}, "
          f"recall {tp/max(actual,1):.1%}  (deliberately imperfect)")

    if args.dry_run:
        print("\n--dry-run: nothing written.")
        return 0
    if not args.yes:
        print("\nRefusing to run without --yes (this DROPs the existing tables).")
        return 1

    load_dotenv(args.env)
    db = os.getenv("IMPALA_DATABASE")
    conn = connect(
        host=os.getenv("IMPALA_HOST"), port=int(os.getenv("IMPALA_PORT", "443")),
        user=os.getenv("IMPALA_USER"), password=os.getenv("IMPALA_PASSWORD"),
        database=db, auth_mechanism=os.getenv("IMPALA_AUTH_MECHANISM", "LDAP"),
        use_http_transport=True, http_path=os.getenv("IMPALA_HTTP_PATH", "cliservice"),
        use_ssl=True,
    )
    cur = conn.cursor()
    try:
        for name, (schema, cols) in TABLES.items():
            rows = data[name]
            print(f"\n== {name}")
            cur.execute(f"DROP TABLE IF EXISTS {name}")
            cur.execute(f"CREATE TABLE {name} ({schema}) STORED AS ICEBERG")
            for start in range(0, len(rows), BATCH):
                chunk = rows[start:start + BATCH]
                values = ",".join(
                    "(" + ",".join(q(r[c]) for c in cols) + ")" for r in chunk
                )
                cur.execute(f"INSERT INTO {name} ({','.join(cols)}) VALUES {values}")
                print(f"   inserted {min(start+BATCH, len(rows)):>6,}/{len(rows):,}")
            cur.execute(f"COMPUTE STATS {name}")
            cur.execute(f"SELECT COUNT(*) FROM {name}")
            print(f"   verified {cur.fetchall()[0][0]:,} rows in {db}.{name}")
    finally:
        cur.close()
        conn.close()

    print("\nDone. Re-run scripts/warmup.py before demoing.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
