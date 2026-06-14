"""
Patient monitoring TELEMETRY simulator (Phase 5 input).

Emits per-patient vitals events as JSON files into a local folder (or directly to ADLS
`landing/telemetry/` if you upload them). The Spark Structured Streaming job
(databricks/streaming/telemetry_stream.py) consumes these with event-time watermarking.

To demonstrate late/out-of-order handling, a small fraction of events are emitted with an
intentionally older event_time.

Usage:
    python data_generation/telemetry_simulator.py --patients 20 --batches 30 --interval 2 --out ./out/telemetry
"""
from __future__ import annotations

import argparse
import json
import random
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path


def gen_event(patient_id: str, now: datetime, late: bool) -> dict:
    event_time = now - timedelta(minutes=random.randint(6, 20)) if late else now
    # occasional anomaly: low SpO2
    spo2 = random.choice([random.randint(95, 100)] * 9 + [random.randint(82, 90)])
    return {
        "event_id": uuid.uuid4().hex,
        "patient_id": patient_id,
        "heart_rate": random.randint(55, 120),
        "spo2": spo2,
        "systolic_bp": random.randint(90, 160),
        "diastolic_bp": random.randint(55, 100),
        "temperature_c": round(random.uniform(36.0, 39.5), 1),
        "event_time": event_time.astimezone(UTC).isoformat(),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--patients", type=int, default=20)
    ap.add_argument("--batches", type=int, default=30, help="number of file batches to emit")
    ap.add_argument("--interval", type=float, default=2.0, help="seconds between batches")
    ap.add_argument("--late-fraction", type=float, default=0.05)
    ap.add_argument("--out", type=str, default="./out/telemetry")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    patient_ids = [f"PAT-{i:05d}" for i in range(1, args.patients + 1)]

    print(f"Emitting telemetry for {len(patient_ids)} patients -> {out_dir}")
    for b in range(args.batches):
        now = datetime.now(UTC)
        events = []
        for pid in patient_ids:
            late = random.random() < args.late_fraction
            events.append(gen_event(pid, now, late))
        fname = out_dir / f"telemetry_{now.strftime('%Y%m%dT%H%M%S')}_{b:04d}.json"
        with fname.open("w", encoding="utf-8") as f:
            for e in events:
                f.write(json.dumps(e) + "\n")  # newline-delimited JSON for Spark
        print(f"batch {b + 1}/{args.batches}: {len(events)} events -> {fname.name}")
        if b < args.batches - 1:
            time.sleep(args.interval)

    print("\nDone. Upload to ADLS landing/telemetry/ (or point the stream at this folder).")


if __name__ == "__main__":
    main()
