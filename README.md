# Operating Under Pressure

**Scaling a Stream-Processing Pipeline for Real-Time Intraoperative Hypotension
Prediction**

Gabriel Brown — M.Sc. Data Science, Freie Universität Berlin
TRANSFER project, Charité – Universitätsmedizin Berlin

## Summary

An instrumented replay-based testbed that measures the end-to-end latency and
scaling behaviour of a real-time intraoperative hypotension (IOH) prediction
pipeline. Recorded perioperative data is replayed through a Kafka/Flink path to
a configurable inference stub and a clinician-facing interface, while a
benchmark wrapped around the pipeline records where delay accumulates. The goal
is to locate the architecture's operating boundary across two dimensions —
concurrent surgical cases and inference demand — and to interpret the measured
latency against the clinical warning-time budget that a prediction must fit
inside to be actionable. See `proposal.txt` for the research questions and the
full evaluation plan.

## Architecture (target)

```
replay → Kafka → Flink → inference stub → interface
     └─────── end-to-end latency benchmark ───────┘
```

Cases are keyed by surgical case throughout, so multiple recorded operations
replay concurrently while their state and progress stay separately observable.
**None of this exists yet** — it is built out sprint by sprint.

## Repo layout

- `src/` — the single evolving codebase. Git history is the source of truth; the
  pipeline is never forked into per-week copies.
- `sprints/NN-MM_topic/` — per-sprint deliverables (report, configs, results).
  `NN-MM` is a thesis **week range**: `01-02` DWC access → `01-06` pipeline
  build → `07-08` calibration → `09-12` experiments → `11-14` analysis →
  `14-16` writeup. Append-only, because `src/` is rewritten continuously: a
  week-9 result is unreproducible from week-14 `src/` unless the config that
  produced it is frozen alongside it.
- `proposal.txt` — the approved thesis proposal, verbatim text.

## Status

**setup / pre-sprint-1.** Repository skeleton only; no pipeline code yet.
