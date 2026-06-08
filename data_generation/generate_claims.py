"""
Generate synthetic insurance CLAIMS flat files (Phase 1).

Claims arrive from an insurance company as monthly flat files. These are uploaded to the ADLS
`landing/claims/` folder and ingested incrementally by Databricks Auto Loader (Phase 2b).

Each run writes one file per month so you can demonstrate Auto Loader incrementality
(drop a new month -> only that file gets processed).

Usage:
    python data_generation/generate_claims.py --months 3 --per-month 400 --out ./out/claims
    # then upload ./out/claims/*.csv to ADLS landing/claims/  (az storage blob upload-batch)
"""
from __future__ import annotations

import argparse
import csv
import random
import uuid
from datetime import date, timedelta
from pathlib import Path

PAYERS = ["Aetna", "BlueCross", "Cigna", "UnitedHealth", "Medicare", "Medicaid"]
DENIAL_CODES = ["", "", "", "", "CO-16", "CO-97", "PR-1", "CO-45", "CO-29"]  # weighted toward no denial
CLAIM_STATUSES = ["paid", "paid", "paid", "partially_paid", "denied", "pending"]
HOSPITALS = ["hospital_a", "hospital_b"]


def month_starts(n: int) -> list[date]:
    today = date.today().replace(day=1)
    out = []
    for i in range(n):
        # walk backwards n months
        m = today.month - i
        y = today.year
        while m <= 0:
            m += 12
            y -= 1
        out.append(date(y, m, 1))
    return sorted(out)


def gen_claim(submission: date) -> dict:
    billed = round(random.uniform(150, 9000), 2)
    status = random.choice(CLAIM_STATUSES)
    denial = random.choice(DENIAL_CODES) if status == "denied" else ""
    if status == "denied":
        allowed = 0.0
        paid = 0.0
    elif status == "pending":
        allowed = round(billed * random.uniform(0.6, 0.9), 2)
        paid = 0.0
    elif status == "partially_paid":
        allowed = round(billed * random.uniform(0.5, 0.85), 2)
        paid = round(allowed * random.uniform(0.4, 0.8), 2)
    else:  # paid
        allowed = round(billed * random.uniform(0.6, 0.95), 2)
        paid = allowed
    return {
        "claim_id": f"CLM-{uuid.uuid4().hex[:12]}",
        "encounter_id": f"ENC-{random.randint(1, 5000):06d}",
        "hospital_id": random.choice(HOSPITALS),
        "payer": random.choice(PAYERS),
        "billed_amount": billed,
        "allowed_amount": allowed,
        "paid_amount": paid,
        "denial_code": denial,
        "claim_status": status,
        "submission_date": (submission + timedelta(days=random.randint(0, 27))).isoformat(),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--months", type=int, default=3)
    ap.add_argument("--per-month", type=int, default=400)
    ap.add_argument("--out", type=str, default="./out/claims")
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    random.seed(args.seed)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    fields = [
        "claim_id", "encounter_id", "hospital_id", "payer", "billed_amount",
        "allowed_amount", "paid_amount", "denial_code", "claim_status", "submission_date",
    ]

    for ms in month_starts(args.months):
        fname = out_dir / f"claims_{ms.strftime('%Y_%m')}.csv"
        with fname.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for _ in range(args.per_month):
                w.writerow(gen_claim(ms))
        print(f"wrote {fname}  ({args.per_month} claims)")

    print(f"\nDone. Upload to ADLS landing/claims/ e.g.:")
    print(f"  az storage blob upload-batch -d landing/claims -s {out_dir} "
          f"--account-name <storage> --auth-mode login")


if __name__ == "__main__":
    main()
