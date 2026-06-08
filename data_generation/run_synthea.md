# Generating synthetic EMR data with Synthea

[Synthea](https://github.com/synthetichealth/synthea) generates realistic — but entirely **synthetic** —
patient records. We use its CSV export to populate the Azure SQL EMR source tables.

## 1. Prerequisites
- Java 11+ (`java -version`)
- Git

## 2. Clone & configure
```bash
git clone https://github.com/synthetichealth/synthea.git
cd synthea
```
Enable CSV export in `src/main/resources/synthea.properties`:
```
exporter.csv.export = true
exporter.fhir.export = false
```

## 3. Generate (small, cheap dataset)
Generate ~500 patients for one "hospital" (state acts as a partition we map to a hospital):
```bash
# hospital_a
./run_synthea -p 500 Massachusetts
# hospital_b
./run_synthea -p 500 California
```
Output CSVs land in `synthea/output/csv/`: `patients.csv`, `providers.csv`, `organizations.csv`,
`encounters.csv`, `procedures.csv`, `claims.csv`, etc.

## 4. Map Synthea → our EMR schema
`load_synthea_to_azuresql.py` reads the Synthea CSVs and maps them into our tables:

| Our table     | Synthea source(s)                          |
|---------------|--------------------------------------------|
| `Patients`    | `patients.csv`                             |
| `Providers`   | `providers.csv`                            |
| `Department`  | `organizations.csv` (as departments)       |
| `Encounter`   | `encounters.csv` + `procedures.csv` (codes)|
| `Transactions`| `claims.csv` (amounts/payer/status)        |

The loader assigns `hospital_id` (`hospital_a` / `hospital_b`) based on which run the CSV came from —
copy each run's `output/csv` into `data_generation/synthea_out/hospital_a/` and
`.../hospital_b/` respectively.

## 5. Load into Azure SQL
```powershell
python data_generation/load_synthea_to_azuresql.py
```

## Notes
- Keep `-p` small (a few hundred) to stay within trial credits and keep Spark jobs fast.
- Synthea data contains no real PII; the `ssn`-style fields are fabricated and still get masked in silver
  to demonstrate the governance pattern.
