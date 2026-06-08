"""
Generate a synthetic EMR seed as a .sql file (Phase 1, Java-free alternative to Synthea).

Produces departments, providers, patients, encounters, and transactions for two hospitals,
using the same ENC-###### encounter-id space as generate_claims.py so claims join to encounters
in the silver/gold layers. Run the output with sqlcmd against the Azure SQL emr_source DB.

Usage:
    python data_generation/generate_emr_seed_sql.py --out sql/03_seed_emr_sample.sql
"""
from __future__ import annotations

import argparse
import random
from datetime import date, datetime, timedelta

HOSPITALS = ["hospital_a", "hospital_b"]
DEPTS = ["Cardiology", "Oncology", "Emergency", "Orthopedics", "Pediatrics", "Radiology"]
SPECIALTIES = ["Cardiologist", "Oncologist", "ER Physician", "Orthopedic Surgeon", "Pediatrician"]
ENC_TYPES = ["inpatient", "outpatient", "emergency", "ambulatory"]
ICD = ["E11.9", "I10", "A41.9", "S72.001A", "J18.9", "E78.5", "N39.0", "M54.5"]
PROC = ["99213", "99285", "93000", "71046", "80053", "85025", "36415"]
PAYERS = ["Aetna", "BlueCross", "Cigna", "UnitedHealth", "Medicare", "Medicaid"]
STATUSES = ["billed", "paid", "denied", "partially_paid"]
FIRST = ["James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda", "David", "Susan"]
LAST = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Wilson", "Lee"]


def esc(s: str) -> str:
    return s.replace("'", "''")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="sql/03_seed_emr_sample.sql")
    ap.add_argument("--patients", type=int, default=60, help="per hospital")
    ap.add_argument("--encounters", type=int, default=250, help="per hospital")
    ap.add_argument("--seed", type=int, default=7)
    args = ap.parse_args()
    random.seed(args.seed)

    lines: list[str] = ["SET NOCOUNT ON;", "BEGIN TRANSACTION;"]
    enc_counter = 0

    for h in HOSPITALS:
        # Departments
        dept_ids = []
        for i, d in enumerate(DEPTS):
            did = f"{h[-1].upper()}-DEP-{i:02d}"
            dept_ids.append(did)
            lines.append(f"INSERT INTO dbo.Department (dept_id,hospital_id,name) "
                         f"VALUES ('{did}','{h}','{esc(d)}');")
        # Providers
        prov_ids = []
        for i in range(12):
            pid = f"{h[-1].upper()}-PRV-{i:03d}"
            prov_ids.append(pid)
            npi = str(random.randint(1000000000, 1999999999))
            nm = f"{random.choice(FIRST)} {random.choice(LAST)}"
            lines.append(
                f"INSERT INTO dbo.Providers (provider_id,hospital_id,npi,name,specialty,dept_id) "
                f"VALUES ('{pid}','{h}','{npi}','Dr. {esc(nm)}','{random.choice(SPECIALTIES)}',"
                f"'{random.choice(dept_ids)}');")
        # Patients
        pat_ids = []
        for i in range(args.patients):
            pid = f"{h[-1].upper()}-PAT-{i:05d}"
            pat_ids.append(pid)
            dob = date(random.randint(1940, 2015), random.randint(1, 12), random.randint(1, 28))
            ssn = f"{random.randint(100,899)}-{random.randint(10,99)}-{random.randint(1000,9999)}"
            lines.append(
                f"INSERT INTO dbo.Patients (patient_id,hospital_id,firstname,lastname,dob,gender,"
                f"address,city,state,zip,ssn) VALUES ('{pid}','{h}','{random.choice(FIRST)}',"
                f"'{random.choice(LAST)}','{dob.isoformat()}','{random.choice(['M','F'])}',"
                f"'{random.randint(1,9999)} Main St','Springfield','MA','0{random.randint(1000,1999)}','{ssn}');")
        # Encounters + Transactions (shared encounter id)
        for _ in range(args.encounters):
            enc_counter += 1
            eid = f"ENC-{enc_counter:06d}"
            start = datetime.now() - timedelta(days=random.randint(0, 365), minutes=random.randint(0, 1440))
            end = start + timedelta(hours=random.randint(1, 72))
            lines.append(
                f"INSERT INTO dbo.Encounter (encounter_id,hospital_id,patient_id,provider_id,dept_id,"
                f"encounter_type,procedure_code,icd_code,start_time,end_time) "
                f"VALUES ('{eid}','{h}','{random.choice(pat_ids)}','{random.choice(prov_ids)}',"
                f"'{random.choice(dept_ids)}','{random.choice(ENC_TYPES)}','{random.choice(PROC)}',"
                f"'{random.choice(ICD)}','{start.strftime('%Y-%m-%d %H:%M:%S')}',"
                f"'{end.strftime('%Y-%m-%d %H:%M:%S')}');")
            charge = round(random.uniform(200, 12000), 2)
            status = random.choice(STATUSES)
            adj = round(charge * random.uniform(0.05, 0.3), 2)
            paid = 0.0 if status == "denied" else round((charge - adj) * random.uniform(0.5, 1.0), 2)
            lines.append(
                f"INSERT INTO dbo.Transactions (txn_id,hospital_id,encounter_id,amount,paid_amount,"
                f"adjustment,payer,status,txn_date) VALUES ('TXN-{enc_counter:06d}','{h}','{eid}',"
                f"{charge},{paid},{adj},'{random.choice(PAYERS)}','{status}','{start.date().isoformat()}');")

    lines.append("COMMIT;")
    with open(args.out, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print(f"wrote {args.out}  ({len(lines)} statements, {enc_counter} encounters)")


if __name__ == "__main__":
    main()
