"""
Load Synthea CSV output into the Azure SQL EMR source tables (Phase 1).

Reads Synthea CSVs from data_generation/synthea_out/<hospital_id>/ and maps them into our
EMR schema (see sql/01_create_emr_tables.sql), tagging each row with hospital_id.

Connection settings come from environment variables (do NOT hard-code secrets):
    SQL_SERVER   e.g. sql-rcm-demo.database.windows.net
    SQL_DB       e.g. emr_source
    SQL_USER     e.g. rcmadmin
    SQL_PASSWORD (pull from Key Vault:  az keyvault secret show ...)

Usage:
    pip install pyodbc pandas
    python data_generation/load_synthea_to_azuresql.py
"""
from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pyodbc

HOSPITALS = ["hospital_a", "hospital_b"]
SYNTHEA_ROOT = Path(__file__).parent / "synthea_out"


def conn():
    server = os.environ["SQL_SERVER"]
    db = os.environ["SQL_DB"]
    user = os.environ["SQL_USER"]
    pwd = os.environ["SQL_PASSWORD"]
    cs = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{server},1433;Database={db};Uid={user};Pwd={pwd};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
    )
    return pyodbc.connect(cs)


def _read(hospital: str, name: str) -> pd.DataFrame:
    p = SYNTHEA_ROOT / hospital / f"{name}.csv"
    if not p.exists():
        print(f"  [skip] {p} not found")
        return pd.DataFrame()
    return pd.read_csv(p, dtype=str)


def map_patients(df, hospital):
    if df.empty:
        return df
    out = pd.DataFrame({
        "patient_id": df["Id"],
        "hospital_id": hospital,
        "firstname": df.get("FIRST"),
        "lastname": df.get("LAST"),
        "dob": df.get("BIRTHDATE"),
        "gender": df.get("GENDER"),
        "address": df.get("ADDRESS"),
        "city": df.get("CITY"),
        "state": df.get("STATE"),
        "zip": df.get("ZIP"),
        "ssn": df.get("SSN"),
    })
    return out


def map_providers(df, hospital):
    if df.empty:
        return df
    return pd.DataFrame({
        "provider_id": df["Id"],
        "hospital_id": hospital,
        "npi": df.get("Id").str.replace("-", "").str[:10],  # placeholder; real NPI joined in silver
        "name": df.get("NAME"),
        "specialty": df.get("SPECIALITY"),
        "dept_id": df.get("ORGANIZATION"),
    })


def map_department(df, hospital):
    if df.empty:
        return df
    return pd.DataFrame({
        "dept_id": df["Id"],
        "hospital_id": hospital,
        "name": df.get("NAME"),
    })


def map_encounter(enc, proc, hospital):
    if enc.empty:
        return enc
    icd = None
    if not proc.empty and "CODE" in proc and "ENCOUNTER" in proc:
        icd = proc.groupby("ENCOUNTER")["CODE"].first()
    out = pd.DataFrame({
        "encounter_id": enc["Id"],
        "hospital_id": hospital,
        "patient_id": enc.get("PATIENT"),
        "provider_id": enc.get("PROVIDER"),
        "dept_id": enc.get("ORGANIZATION"),
        "encounter_type": enc.get("ENCOUNTERCLASS"),
        "procedure_code": enc.get("CODE"),
        "start_time": enc.get("START"),
        "end_time": enc.get("STOP"),
    })
    out["icd_code"] = out["encounter_id"].map(icd) if icd is not None else None
    return out


def map_transactions(claims, hospital):
    if claims.empty:
        return claims
    svc = claims.get("SERVICEDATE")
    return pd.DataFrame({
        "txn_id": claims["Id"],
        "hospital_id": hospital,
        "encounter_id": claims.get("APPOINTMENTID"),
        "patient_id": claims.get("PATIENTID"),          # transaction's patient (guarantor)
        "amount": pd.to_numeric(claims.get("TOTAL_CLAIM_COST"), errors="coerce"),
        "paid_amount": pd.to_numeric(claims.get("PAYER_COVERAGE"), errors="coerce"),
        "adjustment": 0,
        "amount_type": "Insurance",                     # default for Synthea-derived rows
        "payer": claims.get("PAYER"),
        "status": "billed",
        "visit_date": svc,
        "service_date": svc,
        "txn_date": svc,
    })


INSERTS = {
    "Patients": ("patient_id,hospital_id,firstname,lastname,dob,gender,address,city,state,zip,ssn", 11),
    "Department": ("dept_id,hospital_id,name", 3),
    "Providers": ("provider_id,hospital_id,npi,name,specialty,dept_id", 6),
    "Encounter": ("encounter_id,hospital_id,patient_id,provider_id,dept_id,encounter_type,procedure_code,start_time,end_time,icd_code", 10),
    "Transactions": ("txn_id,hospital_id,encounter_id,patient_id,amount,paid_amount,adjustment,amount_type,payer,status,visit_date,service_date,txn_date", 13),
}


def bulk_insert(cur, table: str, df: pd.DataFrame):
    if df.empty:
        return
    cols, n = INSERTS[table]
    placeholders = ",".join(["?"] * n)
    col_list = cols.split(",")
    df = df.reindex(columns=col_list)
    sql = f"INSERT INTO dbo.{table} ({cols}) VALUES ({placeholders})"
    cur.fast_executemany = True
    rows = [tuple(None if pd.isna(v) else v for v in row) for row in df.itertuples(index=False)]
    cur.executemany(sql, rows)
    print(f"  inserted {len(rows)} into {table}")


def main():
    with conn() as cn:
        cur = cn.cursor()
        for h in HOSPITALS:
            print(f"== {h} ==")
            bulk_insert(cur, "Patients", map_patients(_read(h, "patients"), h))
            bulk_insert(cur, "Department", map_department(_read(h, "organizations"), h))
            bulk_insert(cur, "Providers", map_providers(_read(h, "providers"), h))
            bulk_insert(cur, "Encounter",
                        map_encounter(_read(h, "encounters"), _read(h, "procedures"), h))
            bulk_insert(cur, "Transactions", map_transactions(_read(h, "claims"), h))
            cn.commit()
    print("Done loading EMR source tables.")


if __name__ == "__main__":
    main()
