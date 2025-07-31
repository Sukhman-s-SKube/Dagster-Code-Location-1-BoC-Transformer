import numpy as np
import pandas as pd
import requests
from dagster import (
    asset,
    multi_asset,
    AssetOut,
    DailyPartitionsDefinition,
    WeeklyPartitionsDefinition,
)
from fredapi import Fred

DAILY = DailyPartitionsDefinition(start_date="2015-01-01")
WEEKLY = WeeklyPartitionsDefinition(start_date="2020-01-06")

@asset(
    partitions_def=DAILY,
    required_resource_keys={"boc_api"},
    group_name="raw_daily",
    tags={"layer": "raw", "source": "BoC"},
    metadata={
        "frequency":    "daily",
        "columns":      ["date", "rate"],
        "series_id": "B114039",
        "source_url":   "https://www.bankofcanada.ca/valet",
        "unit":         "percent",
    },
    description="Daily 10-yr policy rate from BoC Valet.",
)
def daily_policy_rate(context) -> pd.DataFrame:
    date_str = context.partition_key
    base = context.resources.boc_api.base_url.rstrip("/")
    resp = requests.get(
        f"{base}/observations/B114039/json",
        params={"start_date": date_str,
                "end_date":   date_str,
                }
    )
    resp.raise_for_status()
    data = resp.json()

    obs = data.get("observations", [])
    if not obs:
        context.log.warning(f"No policy-rate change on {date_str}")
        return pd.DataFrame(columns=["date", "rate"])

    rate = float(obs[0]["B114039"]["v"])
    df   = pd.DataFrame({"date": [pd.to_datetime(date_str)], "rate": [rate]})

    context.add_output_metadata(
        {
            "date":   date_str,
            "rate":   rate,
            "preview": f"{date_str}:{rate:.2f}%",
        }
    )
    return df

@asset(
    partitions_def=DAILY,
    required_resource_keys={"boc_api"},
    group_name="raw_daily",
    tags={"layer": "raw", "source": "BoC"},
    metadata={
        "frequency":   "monthly, sparse",
        "columns":     ["date", "cpi"],
        "series_id":   "V41690973",
        "unit":        "index, 2015 = 100",
        "source_url":  "https://www.bankofcanada.ca/valet/observations/V41690973",
    },
    description="Canadian CPI (all-items, 2015=100) pulled directly from BoC Valet.",
)
def daily_cpi(context) -> pd.DataFrame:
    date_str = context.partition_key
    part_dt  = pd.to_datetime(date_str)

    base = context.resources.boc_api.base_url.rstrip("/")
    series = "V41690973"

    resp = requests.get(
        f"{base}/observations/{series}/json",
        params={
            "start_date": date_str,
            "end_date":   date_str,
        },
    )
    resp.raise_for_status()
    obs = resp.json().get("observations", [])

    if not obs:
        context.log.warning(f"No CPI release on {date_str}")
        return pd.DataFrame(columns=["date", "cpi"])

    cpi_value = float(obs[0][series]["v"])
    df = pd.DataFrame({"date": [part_dt], "cpi": [cpi_value]})

    context.add_output_metadata(
        {"date": date_str,
         "cpi":  cpi_value,
         "preview": f"{date_str}: {cpi_value:.2f}"}
    )
    return df