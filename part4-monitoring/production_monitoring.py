# Part 4: KPI Monitoring
# Business Metrics
import pandas as pd

fact_conversions = pd.read_csv("fact_conversions.csv")
fact_attribution = pd.read_csv("fact_attribution.csv")
sessions = pd.read_csv("sessions.csv")
events_with_sessions = pd.read_csv("events_with_sessions.csv")

def compute_baseline(df, date_col, value_col, lookback_days=7):

    return (
        df
        .sort_values(date_col)
        .tail(lookback_days)[value_col]
        .mean()
    )

def monitor_business_metrics(
    fact_conversions: pd.DataFrame,
    fact_attribution: pd.DataFrame,
    run_date
):
    alerts = []

    # Daily revenue
    daily_rev = fact_conversions["revenue"].sum()
    baseline_rev = compute_baseline(
        fact_conversions,
        "conversion_ts",
        "revenue"
    )

    if baseline_rev > 0:
        delta = (daily_rev - baseline_rev) / baseline_rev
        if abs(delta) > 0.3:
            alerts.append({
                "metric": "daily_revenue",
                "severity": "critical",
                "message": f"Revenue changed {delta:.1%} vs baseline"
            })

    # Direct attribution share
    direct_share = (
        fact_attribution
        .query("utm_source == 'direct'")
        .groupby("attribution_model")["revenue"]
        .sum()
        / fact_attribution.groupby("attribution_model")["revenue"].sum()
    )

    if (direct_share > 0.7).any():
        alerts.append({
            "metric": "direct_attribution_share",
            "severity": "warning",
            "message": "High direct attribution share detected"
        })

    return alerts

#Pipeline health
def monitor_pipeline_health(
    events_with_sessions,
    sessions,
    fact_conversions
):
    alerts = []

    if len(events_with_sessions) == 0:
        alerts.append({
            "metric": "events_volume",
            "severity": "critical",
            "message": "No events ingested"
        })

    if len(fact_conversions) == 0:
        alerts.append({
            "metric": "conversions",
            "severity": "critical",
            "message": "No conversions detected"
        })

    if len(sessions) == 0:
        alerts.append({
            "metric": "sessions",
            "severity": "critical",
            "message": "No sessions generated"
        })

    return alerts

from datetime import date

def run_daily_monitoring(
    fact_conversions,
    fact_attribution,
    sessions,
    events_with_sessions
):
    alerts = []

    alerts.extend(
        monitor_business_metrics(
            fact_conversions,
            fact_attribution,
            run_date=date.today()
        )
    )

    alerts.extend(
        monitor_pipeline_health(
            events_with_sessions,
            sessions,
            fact_conversions
        )
    )

    status = "PASS" if len(alerts) == 0 else "FAIL"

    return {
        "run_date": str(date.today()),
        "status": status,
        "alert_count": len(alerts),
        "alerts": alerts
    }

def main():

    monitoring_report = run_daily_monitoring(
        fact_conversions=fact_conversions,
        fact_attribution=fact_attribution,
        sessions=sessions,
        events_with_sessions=events_with_sessions
    )

    print(monitoring_report)

    if monitoring_report["status"] == "FAIL":
        raise RuntimeError(
            f"Data monitoring failed with {monitoring_report['alert_count']} alerts"
        )
    if __name__ == "__main__":
      main()