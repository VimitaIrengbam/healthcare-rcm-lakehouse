# Who Feeds the Landing Zone, and How Often?

In production, **nobody manually uploads files** to the landing zone — automated producers and
pipelines write to it. This project's demo uses manual uploads (`az storage blob upload`) and a
simulator only as stand-ins. This doc explains how each source really arrives, who/what injects it,
and how frequently.

---

## The principle: landing-zone ingestion is automated, not human

A "landing zone" is just the raw entry point in storage. In real systems it is fed by:
- **Streaming services** — Azure **Event Hubs / IoT Hub / Kafka** (continuous device data)
- **Scheduled pipelines** — **ADF / Synapse** Copy activities (time- or event-triggered)
- **Source systems pushing files** — payer/vendor **SFTP** drops, AzCopy, SDK uploads
- **Event-driven glue** — **Azure Functions / Logic Apps / Event Grid** reacting to a file arrival,
  an email attachment, or an API webhook

Humans only touch it for rare ad-hoc/manual loads (e.g., an analyst dropping a one-off vendor file).

---

## Who/what injects each source, and how often

| Source | Who/what produces it (real world) | How it lands | Frequency |
|---|---|---|---|
| **Telemetry (vitals JSON)** | Bedside/ICU **medical devices** -> a gateway (HL7/MQTT) -> **Azure IoT Hub / Event Hubs**. No human, no manual file. | Usually **streamed directly** (Structured Streaming reads Event Hubs/IoT Hub). If landed as files, via **Event Hubs Capture / Stream Analytics**. | **Continuous** — events every few seconds per patient/device. If captured to files, micro-batched (e.g., every ~5 min or ~300 MB). |
| **Claims (flat files)** | The **insurance payer / clearinghouse** (e.g., EDI 837/835 files). | Payer **SFTP** drop -> a Function/Logic App/ADF moves it to `landing/claims/`; or pulled via payer API on a schedule. | **Batch** — typically **monthly or weekly** (we modeled monthly/twice-monthly). |
| **EMR (Patients/Encounters/...)** | Hospital's **operational database** (system of record). | **Not a landing file** — **ADF pulls** it directly from Azure SQL into bronze. | **Scheduled** (e.g., daily) and/or CDC-incremental via watermark. |
| **Reference (NPI/ICD)** | Government/public **APIs** (CMS NPI Registry, NLM ICD). | A Databricks/Function job **calls the API** and writes to bronze. | **Periodic** — change slowly, so weekly/monthly or on demand. |

---

## How "how often" is actually controlled (the trigger)

Frequency is set by the **trigger** attached to each ingestion process:
- **Schedule trigger** (cron / tumbling-window in ADF): "every day at 2 AM," "every hour," "1st of
  the month." Used for batch sources like claims and EMR.
- **Event/storage trigger** (Event Grid -> ADF/Function/Auto Loader): fires **the moment a file
  lands**, so processing starts immediately instead of waiting for the next schedule. Great for
  unpredictable payer drops.
- **Always-on** (streaming): telemetry has no "frequency" — the Structured Streaming job runs
  continuously and processes events as they arrive.

**Auto Loader** makes file-based ingestion robust regardless of who dropped a file or when: it
incrementally detects and processes only new files (with checkpointing), and at scale it can use
**file-notification mode** (Event Grid + a queue) instead of directory listing — so it scales to high
file volumes and reacts quickly.

---

## Telemetry specifically — demo vs production

- **Demo:** `telemetry_simulator.py` writes NDJSON files into `landing/telemetry/`, and Structured
  Streaming reads that **file source** — chosen purely to avoid messaging-service costs on the trial.
- **Production:** delete the file step entirely. Devices -> IoT Hub/Event Hubs -> the **same**
  Structured Streaming code pointed at the **Event Hubs/Kafka source**. Only the source connector
  changes, not the engine or logic. (Files in a landing folder are not how you normally feed a
  real-time stream; that is a demo convenience.)

---

## Mapping the demo to reality

| Demo (what we do now) | Real-world replacement |
|---|---|
| `az storage blob upload` of claim CSVs | Payer SFTP drop + Event Grid trigger / scheduled ADF copy |
| `telemetry_simulator.py` writing JSON files | Medical devices -> IoT Hub/Event Hubs (streamed directly) |
| Manual notebook runs | ADF schedule triggers / Databricks Workflows / Auto Loader file-notifications |

---

## Bottom line

The landing zone is fed by **automated producers** — streaming services for continuous device data,
and scheduled or event-triggered pipelines (often kicked off by an SFTP drop or storage event) for
batch files. Frequency ranges from **continuous** (telemetry) to **monthly** (claims), controlled by
the trigger attached to each source. A person manually uploading is only a demo/ad-hoc pattern.
