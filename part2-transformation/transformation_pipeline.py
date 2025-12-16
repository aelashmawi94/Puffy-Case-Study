FOLDER_PATH = "C:\\Users\\aelas\\Desktop\\HoDIA-STdataset-20251118T051305Z-1-001\\HoDIA-STdataset"  # Edit path to the folder containing the event csvs. Make sure there are no other csvs there.

# Building Enriched Events

import pandas as pd
from urllib.parse import urlparse, parse_qs
from typing import Dict, List


def extract_utm_params(url: str) -> Dict[str, str]:
    """
    Extracts standard UTM parameters from a URL.
    Returns None for missing params.
    """
    if pd.isna(url):
        return {
            "utm_source": None,
            "utm_medium": None,
            "utm_campaign": None,
            "utm_content": None,
        }

    query = urlparse(url).query
    params = parse_qs(query)

    return {
        "utm_source": params.get("utm_source", [None])[0],
        "utm_medium": params.get("utm_medium", [None])[0],
        "utm_campaign": params.get("utm_campaign", [None])[0],
        "utm_content": params.get("utm_content", [None])[0],
    }


def parse_user_agent(ua_string: str) -> Dict[str, str]:
    """
    Lightweight, dependency-free user agent parser
    suitable for marketing analytics.
    """
    if pd.isna(ua_string):
        return {
            "device_type": None,
            "operating_system": None,
            "browser": None,
            "is_mobile": None,
        }

    ua = ua_string.lower()


    # Device

    if "mobile" in ua or "iphone" in ua or "android" in ua:
        device_type = "mobile"
        is_mobile = True
    elif "tablet" in ua or "ipad" in ua:
        device_type = "tablet"
        is_mobile = True
    else:
        device_type = "desktop"
        is_mobile = False


    # Operating System

    if "android" in ua:
        operating_system = "Android"
    elif "iphone" in ua or "ipad" in ua or "ios" in ua:
        operating_system = "iOS"
    elif "windows" in ua:
        operating_system = "Windows"
    elif "mac os" in ua or "macintosh" in ua:
        operating_system = "MacOS"
    elif "linux" in ua:
        operating_system = "Linux"
    else:
        operating_system = "Other"


    # Browser

    if "chrome" in ua and "safari" in ua:
        browser = "Chrome"
    elif "safari" in ua and "chrome" not in ua:
        browser = "Safari"
    elif "firefox" in ua:
        browser = "Firefox"
    elif "edge" in ua:
        browser = "Edge"
    elif "msie" in ua or "trident" in ua:
        browser = "Internet Explorer"
    else:
        browser = "Other"

    return {
        "device_type": device_type,
        "operating_system": operating_system,
        "browser": browser,
        "is_mobile": is_mobile,
    }

#Fix for the client ID schema drift

def resolve_client_id(df: pd.DataFrame) -> pd.Series:
    """
    Resolves client identity across schema versions.
    Priority:
      1. client_id
      2. clientId
    """

    has_client_id = "client_id" in df.columns
    has_clientID = "clientId" in df.columns

    if has_client_id:
        return df["client_id"]

    if has_clientID:
        return df["clientId"]

    raise ValueError(
        "No client id column found. Expected one of: client_id, clientID"
    )


def build_enriched_events(dfs: List[pd.DataFrame]) -> pd.DataFrame:

    df = pd.concat(dfs, ignore_index=True)

    df["client_id"] = resolve_client_id(df)


    df["event_ts"] = pd.to_datetime(
        df["timestamp"],
        errors="coerce",
        utc=True
    )

    # Extract UTM parameters
    utm_df = df["page_url"].apply(extract_utm_params).apply(pd.Series)

    # Parse user agent
    ua_df = df["user_agent"].apply(parse_user_agent).apply(pd.Series)

    df = pd.concat([df, utm_df, ua_df], axis=1)

    enriched_events = df[
        [
            "client_id",
            "event_name",
            "event_ts",
            "page_url",
            "utm_source",
            "utm_medium",
            "utm_campaign",
            "utm_content",
            "device_type",
            "operating_system",
            "browser",
            "is_mobile",
            "user_agent",
            "event_data",
        ]
    ]

    return enriched_events

import glob
import os

dfs = []

for file_path in glob.glob(os.path.join(FOLDER_PATH, "*.csv")):
    df = pd.read_csv(file_path)
    dfs.append(df)

enriched_events = build_enriched_events(dfs=dfs)
# saving incase of later need
enriched_events.to_csv('enriched_events.csv')

# Sanity Checks
assert enriched_events["event_ts"].isna().sum() == 0
assert set(enriched_events.columns) == {
    "client_id",
    "event_name",
    "event_ts",
    "page_url",
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_content",
    "device_type",
    "operating_system",
    "browser",
    "is_mobile",
    "user_agent",
    "event_data"
}

assert enriched_events["device_type"].isna().sum() == 0
assert set(enriched_events["device_type"].unique()).issubset(
    {"mobile", "tablet", "desktop"}
)

# Sessionization

SESSION_TIMEOUT_MINUTES = 30

import numpy as np
import pandas as pd


def assign_sessions(enriched_events: pd.DataFrame) -> pd.DataFrame:

    df = enriched_events.copy()

    # Session identity:
    # - Identified users → client_id
    # - Anonymous users → per-event anonymous key (no cross-event linking)

    df["session_identity"] = np.where(
        df["client_id"].notna(),
        df["client_id"].astype(str),
        "anon_event_" + df.index.astype(str)
    )

    df = df.sort_values(["session_identity", "event_ts"])

    df["prev_event_ts"] = (
        df.groupby("session_identity")["event_ts"].shift(1)
    )

    df["minutes_since_prev"] = (
        (df["event_ts"] - df["prev_event_ts"])
        .dt.total_seconds()
        .div(60)
    )

    df["is_new_session"] = (
        df["minutes_since_prev"].isna()
        | (df["minutes_since_prev"] > SESSION_TIMEOUT_MINUTES)
    ).astype(int)

    df["session_index"] = (
        df.groupby("session_identity")["is_new_session"]
        .cumsum()
    )

    df["session_id"] = (
        df["session_identity"]
        + "_"
        + df["session_index"].astype(str)
    )

    return df.drop(
        columns=[
            "session_identity",
            "prev_event_ts",
            "minutes_since_prev",
            "is_new_session",
        ]
    )



def build_sessions(events_with_sessions: pd.DataFrame) -> pd.DataFrame:

    # Identify first event in each session
    first_events = (
        events_with_sessions
        .sort_values("event_ts")
        .groupby("session_id")
        .first()
        .reset_index()
    )

    # Session aggregates
    session_agg = (
        events_with_sessions
        .groupby("session_id")
        .agg(
            client_id=("client_id", "first"),
            session_start_ts=("event_ts", "min"),
            session_end_ts=("event_ts", "max"),
            event_count=("event_name", "count"),

            # Conversion flag

            has_conversion=(
                "event_name",
                lambda x: (x == "checkout_completed").any()
            ),
        )
        .reset_index()
    )

    # Duration
    session_agg["session_duration_seconds"] = (
        session_agg["session_end_ts"]
        - session_agg["session_start_ts"]
    ).dt.total_seconds()

    # Landing attributes from first event
    sessions = session_agg.merge(
        first_events[
            [
                "session_id",
                "page_url",
                "utm_source",
                "utm_medium",
                "utm_campaign",
                "device_type",
                "operating_system",
                "browser",
                "is_mobile",
            ]
        ],
        on="session_id",
        how="left"
    )

    sessions = sessions.rename(columns={
        "page_url": "landing_page",
        "utm_source": "landing_utm_source",
        "utm_medium": "landing_utm_medium",
        "utm_campaign": "landing_utm_campaign",
        "device_type": "landing_device_type",
        "operating_system": "landing_operating_system",
        "browser": "landing_browser",
        "is_mobile": "landing_is_mobile",
    })

    return sessions

events_with_sessions = assign_sessions(enriched_events)
sessions = build_sessions(events_with_sessions)
events_with_sessions.to_csv("events_with_sessions.csv")
sessions.to_csv("sessions.csv")

# Sanity Checks

# Each event has exactly one session
assert events_with_sessions["session_id"].isna().sum() == 0

# No overlapping sessions per user
assert (
    sessions
    .sort_values(["client_id", "session_start_ts"])
    .groupby("client_id")["session_start_ts"]
    .is_monotonic_increasing
).all()

# Session duration is non-negative
assert (sessions["session_duration_seconds"] >= 0).all()

# Conversions

import json
from typing import Tuple

def extract_transaction_fields(event_data: str) -> Tuple[str, float]:

    payload = json.loads(event_data)

    transaction_id = payload["transaction_id"]
    revenue = float(payload["revenue"])

    return transaction_id, revenue

def build_fact_conversions(events_with_sessions: pd.DataFrame) -> pd.DataFrame:

    # Filter conversion events
    conversions = events_with_sessions[
        events_with_sessions["event_name"] == "checkout_completed"
    ].copy()

    # Extract transaction_id and revenue
    conversions[["conversion_id", "revenue"]] = (
        conversions["event_data"]
        .apply(lambda x: pd.Series(extract_transaction_fields(x)))
    )

    conversions = conversions.sort_values("event_ts")

    # one row per transaction_id
    conversions = (
        conversions
        .groupby("conversion_id", as_index=False)
        .first()
    )

    fact_conversions = conversions[
        [
            "conversion_id",
            "client_id",
            "session_id",
            "event_ts",
            "revenue",
            "event_data",
        ]
    ].rename(columns={
        "event_ts": "conversion_ts"
    })

    return fact_conversions

fact_conversions = build_fact_conversions(events_with_sessions)

# Every conversion has a session
assert fact_conversions["session_id"].isna().sum() == 0

# conversion_id is present and unique
assert fact_conversions["conversion_id"].notna().all()
assert fact_conversions["conversion_id"].is_unique

# Revenue is always present and non-negative
assert fact_conversions["revenue"].notna().all()
assert (fact_conversions["revenue"] >= 0).all()

# All events are checkout_completed
assert set(
    events_with_sessions.loc[
        events_with_sessions["event_name"] == "checkout_completed",
        "event_name"
    ]
) == {"checkout_completed"}

fact_conversions.to_csv("fact_conversions.csv")

# Attribution

from datetime import timedelta

ATTRIBUTION_LOOKBACK_DAYS = 7

def build_conversion_touchpoints(
    events_with_sessions: pd.DataFrame,
    fact_conversions: pd.DataFrame
) -> pd.DataFrame:

    # Keep only events with marketing context
    marketing_events = events_with_sessions[
        events_with_sessions[["utm_source", "utm_medium", "utm_campaign", "utm_content"]]
        .notna()
        .any(axis=1)
    ].copy()

    # Join conversions to events by client_id
    touchpoints = fact_conversions.merge(
        marketing_events,
        on="client_id",
        suffixes=("_conversion", "_event")
    )

    # Apply temporal constraints
    touchpoints = touchpoints[
        (touchpoints["event_ts"] < touchpoints["conversion_ts"]) &
        (touchpoints["event_ts"] >=
         touchpoints["conversion_ts"] - timedelta(days=ATTRIBUTION_LOOKBACK_DAYS))
    ]

    return touchpoints[
        [
            "conversion_id",
            "conversion_ts",
            "event_ts",
            "utm_source",
            "utm_medium",
            "utm_campaign",
            "utm_content",
            "session_id_event",
        ]
    ].rename(columns={
        "event_ts": "touchpoint_ts",
        "session_id_event": "touchpoint_session_id"
    })

def build_direct_attribution(
    fact_conversions: pd.DataFrame,
    attributed_conversions: pd.Series,
    model: str
) -> pd.DataFrame:

    # Builds direct attribution rows for conversions with no touchpoints.


    missing = fact_conversions[
        ~fact_conversions["conversion_id"].isin(attributed_conversions)
    ].copy()

    missing["attribution_model"] = model
    missing["utm_source"] = "direct"
    missing["utm_medium"] = "none"
    missing["utm_campaign"] = "direct"
    missing["utm_content"] = None

    return missing[
        [
            "conversion_id",
            "attribution_model",
            "utm_source",
            "utm_medium",
            "utm_campaign",
            "utm_content",
            "revenue",
        ]
    ]


def select_attribution_touchpoints(
    conversion_touchpoints: pd.DataFrame,
    model: str
) -> pd.DataFrame:

    ascending = model == "first_click"

    selected = (
        conversion_touchpoints
        .sort_values("touchpoint_ts", ascending=ascending)
        .groupby("conversion_id", as_index=False)
        .first()
    )

    selected["attribution_model"] = model
    return selected

def compute_sessions_to_conversion(conversion_touchpoints: pd.DataFrame) -> pd.DataFrame:

    return (
        conversion_touchpoints
        .groupby("conversion_id")
        .agg(
            sessions_to_conversion_7d=("touchpoint_session_id", "nunique")
        )
        .reset_index()
    )


def build_fact_attribution(
    conversion_touchpoints: pd.DataFrame,
    fact_conversions: pd.DataFrame
) -> pd.DataFrame:

    first_click = select_attribution_touchpoints(
        conversion_touchpoints,
        model="first_click"
    )

    last_click = select_attribution_touchpoints(
        conversion_touchpoints,
        model="last_click"
    )

    first_click = first_click.merge(
        fact_conversions[["conversion_id", "revenue"]],
        on="conversion_id",
        how="left"
    )

    last_click = last_click.merge(
        fact_conversions[["conversion_id", "revenue"]],
        on="conversion_id",
        how="left"
    )

    sessions_to_conversion = compute_sessions_to_conversion(conversion_touchpoints)

    first_click = first_click.merge(
        sessions_to_conversion,
        on="conversion_id",
        how="left"
    )

    last_click = last_click.merge(
        sessions_to_conversion,
        on="conversion_id",
        how="left"
    )

    direct_first = build_direct_attribution(
        fact_conversions,
        first_click["conversion_id"],
        model="first_click"
    )

    direct_last = build_direct_attribution(
        fact_conversions,
        last_click["conversion_id"],
        model="last_click"
    )

    direct_first["sessions_to_conversion_7d"] = 0
    direct_last["sessions_to_conversion_7d"] = 0

    attribution = pd.concat(
        [first_click, last_click, direct_first, direct_last],
        ignore_index=True
    )

    return attribution[
        [
            "conversion_id",
            "attribution_model",
            "utm_source",
            "utm_medium",
            "utm_campaign",
            "utm_content",
            "sessions_to_conversion_7d",
            "revenue",
        ]
    ]

conversion_touchpoints = build_conversion_touchpoints(
    events_with_sessions=events_with_sessions,
    fact_conversions=fact_conversions
)

fact_attribution = build_fact_attribution(
    conversion_touchpoints=conversion_touchpoints,
    fact_conversions=fact_conversions
)

# Each conversion appears at most once per model
assert (
    fact_attribution
    .groupby(["conversion_id", "attribution_model"])
    .size()
    .max() == 1
)

# Only valid models
assert set(fact_attribution["attribution_model"]) == {"first_click", "last_click"}

# Revenue reconciliation
for model in ["first_click", "last_click"]:
    attributed = fact_attribution.loc[
        fact_attribution["attribution_model"] == model,
        "revenue"
    ].sum()

    original = fact_conversions["revenue"].sum()

    assert abs(attributed - original) < 1e-6

fact_attribution.to_csv('fact_attribution.csv')

