# Part 4 — KPI & Pipeline Monitoring

## Purpose

This monitoring layer is designed to provide **early warning signals** when either business performance or data pipeline health deviates from expected behavior. The objective is to **detect when something is wrong quickly**, so corrective action can be taken before reporting or decision-making is impacted.

## What is monitored

### 1. Business Performance Metrics

#### Daily Revenue
**What is monitored**
- Total revenue generated on the current run

**Why**
Revenue is the most critical business KPI. Sudden changes often indicate:
- Tracking or ingestion failures
- Broken conversion logic
- Genuine business disruptions (campaign issues, outages)

Rather than comparing against a single prior day, revenue is benchmarked against a **rolling baseline** to reduce noise.

---

#### Direct Attribution Share
**What is monitored**
- Share of revenue attributed to `direct`, segmented by attribution model

**Why**
A spike in direct attribution is often an early signal of:
- Broken UTM tracking
- Missing referrer data
- Changes in upstream instrumentation

Monitoring this protects against silently misattributing marketing performance.

---

### 2. Pipeline Health Metrics

#### Event Volume
**What is monitored**
- Whether any events were ingested

**Why**
A zero-event day almost always indicates a **pipeline or ingestion failure**, not a real-world absence of user activity.

---

#### Session Generation
**What is monitored**
- Whether sessions were successfully created

**Why**
Sessionization is a core dependency for funnels, attribution, and KPIs. Missing sessions indicate upstream failures in event processing or timestamp handling.

---

#### Conversion Detection
**What is monitored**
- Whether any conversions were produced

**Why**
A sudden drop to zero conversions can signal:
- Broken event naming
- Changes to conversion definitions
- Join failures between events and sessions

---

## How We Detect When Something Is Wrong

### Baseline Deviation Detection

For revenue, the framework computes a **rolling baseline** (default: 7-day average) and compares the current value against it.

An alert is triggered when:
- Revenue deviates by more than **±30%** from baseline

### Threshold-Based Anomaly Detection

Some conditions are binary and should never occur under normal operation:
- Zero events
- Zero sessions
- Zero conversions

These conditions trigger **critical alerts immediately**, as they almost always indicate data or pipeline failures rather than legitimate business behavior.


The daily monitoring run produces a structured report with:
- Overall pass/fail status
- Alert count
- Detailed alert messages