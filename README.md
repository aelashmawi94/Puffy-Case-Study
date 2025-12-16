# Puffy Case Study

# Event Data Pipeline Project

This repository contains an end-to-end event data pipeline covering **data quality validation**, **data transformation**, and **production monitoring**.  
Each stage builds on the outputs of the previous one, so the project must be run **sequentially**.


## Prerequisites

- Python 3.9+
- `pip`
- A folder containing **only event CSV files** (no other CSVs)

---

## Setup

1. Install dependencies:
pip install -r requirements.txt


2. Before running the pipeline, update the **data path** in the following files:

- `part1-data-quality/data_validation_framework.py`
- `part2-transformation/transformation_pipeline.py`

In **both files**, edit the **first line** to point to the folder where your event CSV files are stored.

**Important constraints:**
- The folder must contain **only event CSV files**
- Do not place any other CSVs in this directory

## Execution Order

The pipeline must be run in the exact order below due to layered dependencies.

### 1. Data Quality Validation

From the `part1-data-quality` folder:
This step:
- Validates schema and column-level constraints
- Detects schema drift
- Produces validated outputs used by downstream steps

### 2. Data Transformation

From the `part2-transformation` folder:
This step:
- Builds transformed datasets (sessions, users, conversions, etc.)
- Outputs multiple CSV files
- These CSVs are **required** for production monitoring

### 3. Data Analysis

From the `part3-analysis` folder:
This step:
- Contains analysis done in a pdf file.

### 4. Production Monitoring

From the `part4-production-monitoring` folder:
This step:
- Consumes transformed CSV outputs
- Monitors pipeline health, data freshness, and anomalies
- Surfaces issues suitable for operational alerting

## Troubleshooting

- Missing files in later steps usually indicate a skipped or failed earlier step
- Schema-related failures typically originate in the data quality stage
- Ensure paths are correct and absolute if relative paths cause issues