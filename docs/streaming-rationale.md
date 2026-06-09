# Why Telemetry + Why Structured Streaming

A detailed rationale for the real-time patient-monitoring (telemetry) component of the
Healthcare RCM Lakehouse: why it exists, why it is a separate file/component, and why it
uses Spark Structured Streaming.

Related code:
- Producer: [`data_generation/telemetry_simulator.py`](../data_generation/telemetry_simulator.py)
- Consumer: [`databricks/streaming/telemetry_stream.py`](../databricks/streaming/telemetry_stream.py)

---

## 1. What the telemetry represents

The telemetry feed simulates **real-time patient monitoring data** — the continuous stream of
vital signs produced by bedside/ICU monitors: heart rate, SpO₂ (blood-oxygen), blood pressure,
and temperature — each tagged with an `event_time` (when the reading was taken) and a `patient_id`.

It is implemented as two files:
- **`telemetry_simulator.py`** (producer): emits per-patient vitals as newline-delimited JSON into
  `landing/telemetry/`, a few events per second, including a small fraction of intentionally
  **late / out-of-order** events.
- **`telemetry_stream.py`** (consumer): a Spark Structured Streaming job that continuously reads
  those events, processes them by event time, and writes results to Delta.

This fulfills the project capability: *"Spark Structured Streaming for continuous patient
monitoring, optimizing event-time processing for real-time healthcare analytics."*

---

## 2. Why we need it — a fundamentally different kind of data

The rest of the project (EMR, claims, KPIs) is **financial / Revenue Cycle** data. Telemetry is
**clinical / operational** data, and it differs on every axis a data engineer cares about:

| Dimension | RCM batch data (EMR, claims) | Telemetry stream |
|---|---|---|
| Shape | Bounded — finite tables/files | **Unbounded** — never "ends" |
| Arrival | Scheduled batches (daily/monthly, DB pulls) | **Continuous**, high-frequency events |
| Latency need | Hours/days is fine (finance reports) | **Seconds** — a low-oxygen reading must be flagged now |
| Business use | Days in A/R, NCR, denial trends | Real-time alerts, early-warning scores, ICU dashboards |
| Value of old data | High (historical trends) | The latest reading dominates |

A credible healthcare platform must handle **both** — slow, high-volume financial batch **and**
fast, continuous clinical streams. Telemetry demonstrates the platform (and the engineer) can do
**real-time** as well as batch, on the same lakehouse, with the same Spark/Delta toolset.

---

## 3. Why it is a separate file / separate component (separation of concerns)

A deliberate architectural choice, not an accident:

**a) Different processing model.** Batch notebooks run and finish (read → transform → write → done).
A streaming job is a **long-running, always-on query** that never naturally completes. Embedding a
never-ending job inside the batch bronze→silver→gold sequence would block the whole pipeline — which
is exactly why the master ADF orchestration runs the batch flow end-to-end but leaves streaming as
its **own independent job**.

**b) Independent failure & scaling.** If the streaming job crashes or restarts, it must not take down
the nightly KPI run — and a heavy gold rebuild must not stall real-time vitals. Separation lets each
fail, restart, and scale on its own.

**c) Different reliability machinery.** Streaming needs **checkpoints** (resume exactly where it left
off), **watermarks** (handle late data), and idempotent sinks. Batch does not. Isolating the streaming
code keeps that complexity contained.

**d) Different source feed.** Telemetry has its own landing path (`landing/telemetry/`) and its own
format (high-frequency NDJSON events), distinct from the EMR DB pulls and monthly claim CSVs. A
separate **generator file** exists because there is no real bedside-monitor feed in a demo — we
synthesize the continuous stream (including late events) so the engine has realistic input.

In short: batch and streaming are two different runtime contracts; forcing them into one module would
couple things that must evolve, scale, and fail independently.

---

## 4. Why Structured Streaming specifically

You could micro-batch vitals with plain batch jobs, but you would be hand-building everything
Structured Streaming provides natively — and adding latency. Why it is the right tool:

**a) Built for unbounded data.** Structured Streaming models a stream as an **unbounded table**
queried with the *same* DataFrame/SQL API as batch — one mental model across the whole project.

**b) Exactly-once + fault tolerance via checkpointing.** It records progress; on restart it **resumes
from the last committed offset** with no data loss and no duplicates.

**c) Event-time processing + watermarking (the headline feature).** Vital-sign events arrive **late
and out of order** (device buffering, network lag). Structured Streaming aggregates by **event time**
(when the vital was measured), not **processing time** (when it landed), and a **watermark** says how
long to wait for stragglers before finalizing.

  Concrete example from our job:
  - A reading taken at **10:00:30** arrives at **10:01:10** (40s late).
  - Processing-time windowing would wrongly count it in the *10:01* minute.
  - With `withWatermark("event_time", "5 minutes")` + `window("event_time", "1 minute")`, it is
    correctly placed in the **10:00** window; the engine keeps that window open for up to 5 minutes of
    lateness, then **finalizes it and drops anything later** — which also **bounds memory** so state
    does not grow forever.

**d) Stateful, incremental aggregations.** It maintains running state across micro-batches
efficiently, enabling **per-patient, per-minute rolling averages** (avg HR, avg/min SpO₂, max BP) and
a real-time **anomaly flag** (e.g., `SpO₂ < 90` = hypoxia), writing both the raw cleansed events and
the 1-minute aggregates to Delta.

**e) Same Delta lakehouse.** The streaming sink is a Delta table, so the live vitals carry the same
governance (Unity Catalog) and time-travel as everything else.

> Cost-aware detail: in this demo the stream source is a **file source** (NDJSON in
> `landing/telemetry/`) rather than Event Hubs/Kafka, to avoid messaging-service costs on the trial.
> In production you would point the *same* Structured Streaming code at **Azure Event Hubs / IoT Hub /
> Kafka** — only the source connector changes, not the engine or logic.

---

## TL;DR

We need telemetry because a real healthcare platform must handle **continuous clinical data**, not
just batch financial data. It is a **separate component** because streaming is an always-on,
independently-scaling, independently-failing workload with its own checkpoint/watermark machinery that
must not be coupled to the batch RCM pipeline. And we use **Structured Streaming** because it is
purpose-built for unbounded data — event-time correctness, late-data handling via watermarks,
exactly-once fault tolerance, and stateful real-time aggregations, all with the same PySpark/Delta API
as the rest of the project.
