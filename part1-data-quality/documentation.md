# Part 1 — Data Validation Framework

## Overview

The data validation framework was built to ensure that all incoming event data conforms to a clearly defined and reliable structure before any downstream processing or analysis occurs. Its primary goal is to prevent silent data corruption, detect schema drift early, and provide confidence that analytical outputs are based on consistent and trustworthy inputs.

## Validation Approach

The framework enforces a strict schema contract that defines the expected structure of event data. It verifies that all required columns are present, identifies missing fields, and flags any unexpected additional columns when schema flexibility is not allowed. This ensures that all datasets adhere to the same structural assumptions and prevents downstream logic from relying on unstable or inferred schemas.

In addition to structural checks, the framework applies column-level validation rules. These rules confirm whether columns are allowed to contain null values, validate that data types match expectations, and enforce format requirements such as URL prefixes and timestamp parseability. Critical fields, including event names, are explicitly validated to ensure they are populated and usable for sessionization, funnel analysis, and attribution logic.

## Validation Outputs

Each input CSV file is evaluated independently. The framework produces a per-file validation result indicating whether the file passed all checks or failed one or more rules. For failed files, the output clearly identifies which checks failed and which columns were affected. This design provides transparency into data quality issues while preserving full diagnostic context.

## Key Findings

Applying the framework revealed evidence of schema drift across the input files. Notably, the `referrer` column was removed without warning starting with events_20250227.csv, which could have broken logic relying on positional column mapping and is the most likely cause of the suspicious revenue numbers. Additionally, the `client_id` column was renamed, likely causing downstream joins and identity-based logic to fail. These issues underscore the risk of relying on implicit assumptions about upstream data structure.