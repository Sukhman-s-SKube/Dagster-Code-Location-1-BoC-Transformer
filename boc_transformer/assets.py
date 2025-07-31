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

@asset(
    partitions_def=DAILY,
    required_resource_keys={"boc_api", "fred_api"},
    group_name="raw_daily",
    tags={"layer": "raw", "source": "mixed"},
    metadata={
        "frequency": "daily (sparse)",
        "columns": [
            "date","y2","y5","y10","spread_2_10","oil","unemploy"
        ],
        "series_ids": {
            "y2":"BD.CDN.2YR.DQ.YLD",
            "y5":"BD.CDN.5YR.DQ.YLD",
            "y10":"BD.CDN.10YR.DQ.YLD",
            "oil":"DCOILWTICO",
            "unemploy":"LRUNTTTTCAQ156S"
        },
        "unit": "percent (yields & unemploy), USD/barrel (oil)",
    },
    description=(
        "Daily 2, 5, 10 yr GoC benchmark yields from BoC Valet"
        "and their 2-10 spread. WTI spot price and Canadian unemployment "
        "rate from FRED."
    ),
)
def daily_yield_spread_and_macros(context) -> pd.DataFrame:
    date_str = context.partition_key
    part_dt  = pd.to_datetime(date_str)
    base     = context.resources.boc_api.base_url.rstrip("/")

    yield_ids = {
        "y2":  "BD.CDN.2YR.DQ.YLD",
        "y5":  "BD.CDN.5YR.DQ.YLD",
        "y10": "BD.CDN.10YR.DQ.YLD",
    }
    yields = {}
    for k, sid in yield_ids.items():
        r = requests.get(f"{base}/observations/{sid}/json",
                         params={"start_date":date_str, "end_date":date_str})
        r.raise_for_status()
        obs = r.json().get("observations", [])
        if not obs:
            context.log.warning(f"{sid} not published on {date_str} skipping row.")
            return pd.DataFrame(columns=[
                "date", *yield_ids.keys(), "spread_2_10", "oil", "unemploy"])
        yields[k] = float(obs[0][sid]["v"])

    spread = yields["y2"] - yields["y10"]

    fred = Fred(api_key=context.resources.fred_api)
    oil_series  = fred.get_series("DCOILWTICO",
                                  observation_start=date_str,
                                  observation_end=date_str)
    oil_val = float(oil_series.iloc[0]) if not oil_series.empty else None

    unemp_series = fred.get_series("LRUNTTTTCAQ156S",
                                   observation_start=date_str,
                                   observation_end=date_str)
    unemploy = float(unemp_series.iloc[0]) if not unemp_series.empty else None

    df = pd.DataFrame({
        "date":[part_dt],
        "y2":[yields["y2"]],
        "y5":[yields["y5"]],
        "y10":[yields["y10"]],
        "spread_2_10":[spread],
        "oil":[oil_val],
        "unemploy":[unemploy],
    })

    context.add_output_metadata({
        "preview": (f"{date_str} | 2y={yields['y2']:.2f}% "
                    f"10y={yields['y10']:.2f}% spread={spread:.2f} "
                    f"oil={oil_val} unemploy={unemploy:.2f}")
    })
    return df