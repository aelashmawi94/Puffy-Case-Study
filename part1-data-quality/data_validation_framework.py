FOLDER_PATH = "event-file-input"  # Edit path to the folder containing the event csvs. Make sure there are no other csvs there.

SCHEMA_CONTRACT = {
    "required_columns": [
        "client_id",
        "page_url",
        "referrer" ,
        "timestamp",
        "event_name",
        "user_agent"

    ],

    "columns": {
        "client_id": {
            "dtype": "object",
            "nullable": True
        },
        "page_url": {
            "dtype": "object",
            "nullable": False,
            "must_start_with": "http"
        },
        "referrer": {
            "dtype": "object",
            "nullable": True,
            "must_start_with": "http"
        },
        "timestamp": {
            "dtype": "object",
            "nullable": False,
            "parseable_datetime": True
        },
        "event_name": {
            "dtype": "object",
            "nullable": False
        },
        "event_data": {
            "dtype": "object",
            "nullable": True
        },
        "user_agent": {
            "dtype": "object",
            "nullable": False
        }
    },

    "allow_extra_columns": False
}

def check_schema(df, schema):
    expected = set(schema["columns"].keys())
    actual = set(df.columns)

    missing = list(set(schema["required_columns"]) - actual)
    unexpected = list(actual - expected) if not schema["allow_extra_columns"] else []

    dtype_mismatches = []
    for col, rules in schema["columns"].items():
        if col in df.columns:
            if str(df[col].dtype) != rules["dtype"]:
                dtype_mismatches.append({
                    "column": col,
                    "expected": rules["dtype"],
                    "actual": str(df[col].dtype)
                })

    success = not (missing or unexpected or dtype_mismatches)

    return {
        "check": "schema",
        "success": success,
        "details": {
            "missing_columns": missing,
            "unexpected_columns": unexpected,
            "dtype_mismatches": dtype_mismatches
        }
    }

def check_column_rules(df, schema):
    failures = []

    for col, rules in schema["columns"].items():
        if col not in df.columns:
            continue

        series = df[col]

        # Nullability
        if not rules.get("nullable", True):
            nulls = series.isna().sum()
            if nulls > 0:
                failures.append({
                    "check": "not_null",
                    "column": col,
                    "null_count": int(nulls)
                })

        # Datetime parsing
        if rules.get("parseable_datetime"):
            parsed = pd.to_datetime(series, errors="coerce", utc=True)
            invalid = parsed.isna().sum()
            if invalid > 0:
                failures.append({
                    "check": "parseable_datetime",
                    "column": col,
                    "invalid_count": int(invalid)
                })

    return failures

def check_event_semantics(df):
    failures = []

    # Event name should not be empty
    empty_events = (df["event_name"].str.strip() == "").sum()
    if empty_events > 0:
        failures.append({
            "check": "event_name_not_empty",
            "invalid_count": int(empty_events)
        })

    return failures

from datetime import datetime

def validate_events_csv(df, file_name, schema):
    results = []

    schema_result = check_schema(df, schema)
    if not schema_result["success"]:
        results.append(schema_result)

    results.extend(check_column_rules(df, schema))
    results.extend(check_event_semantics(df))

    return {
        "file": file_name,
        "validated_at": datetime.utcnow().isoformat(),
        "row_count": len(df),
        "success": len(results) == 0,
        "failed_checks": results
    }

import pandas as pd
import glob
import os

all_results = []

for file_path in glob.glob(os.path.join(FOLDER_PATH, "*.csv")):
    file_name = os.path.basename(file_path)

    df = pd.read_csv(file_path)

    validation_result = validate_events_csv(
        df=df,
        file_name=file_name,
        schema=SCHEMA_CONTRACT
    )

    all_results.append(validation_result)

print(all_results)