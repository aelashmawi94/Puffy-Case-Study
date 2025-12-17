# Puffy Case Study

# Event Data Pipeline Project

This repository contains an end-to-end event data pipeline covering **data quality validation**, **data transformation**, and **production monitoring**.  
Each stage builds on the outputs of the previous one, so the project must be run **sequentially**.


## Prerequisites

- Python 3.9+
- `pip`

---

## Setup

Install dependencies:
pip install -r requirements.txt

## Execution Order

The pipeline must be run in the exact order below due to layered dependencies.

## Adding Data

Create a folder named "event-file-input" in the "PUFFY-CASE_STUDY" folder containing **only event CSV files** (no other CSVs)

**Important constraints:**
- The folder must contain **only event CSV files**
- Do not place any other CSVs in this directory

### 1. Data Quality Validation

Run the following command:
python part1-data-quality/data_validation_framework.py
This step:
- Validates schema and column-level constraints
- Detects schema drift
- Produces validated outputs used by downstream steps

### 2. Data Transformation

Run the following command:
python part2-transformation/transformation_pipeline.py
This step:
- Builds transformed datasets (sessions, users, conversions, etc.)
- Outputs multiple CSV files
- These CSVs are **required** for production monitoring

### 3. Data Analysis

Review the pdf file.
This step:
- Contains analysis done in a pdf file.

### 4. Production Monitoring

Run the following command:
python part4-monitoring/production_monitoring.py 
This step:
- Consumes transformed CSV outputs
- Monitors pipeline health, data freshness, and anomalies
- Surfaces issues suitable for operational alerting

## Troubleshooting

- Missing files in later steps usually indicate a skipped or failed earlier step
- Schema-related failures typically originate in the data quality stage
- Ensure paths are correct and absolute if relative paths cause issues