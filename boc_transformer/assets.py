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
        "columns": ["date", "y2", "y5", "y10", "spread_2_10", "oil", "unemploy"],
        "series_ids": {
            "y2": "YC_2",
            "y5": "YC_5",
            "y10": "YC_10",
            "oil": "DCOILWTICO",
            "unemploy": "LRUNTTTTCAQ156S",
        },
        "unit": "percent (yields & unemploy), USD/barrel (oil)",
    },
    description="Daily 2, 5, and 10 year yields from BoC Valet, the 2–10 spread, oil price, and Canadian unemployment rate.",
)
def daily_yield_spread_and_macros(context) -> pd.DataFrame:
    date_str = context.partition_key
    part_dt = pd.to_datetime(date_str)
    base = context.resources.boc_api.base_url.rstrip("/")

    series_map = {"y2": "YC_2", "y5": "YC_5", "y10": "YC_10"}
    yields = {}
    for key, sid in series_map.items():
        resp = requests.get(
            f"{base}/observations/{sid}/json",
            params={"start_date": date_str, "end_date": date_str},
        )
        resp.raise_for_status()
        obs = resp.json().get("observations", [])
        if not obs:
            context.log.warning(f"No yield {sid} on {date_str}; skipping entire row.")
            return pd.DataFrame(columns=["date", *series_map.keys(), "spread_2_10", "oil", "unemploy"])
        yields[key] = float(obs[0][sid]["v"])

    spread_2_10 = yields["y2"] - yields["y10"]

    fred = Fred(api_key=context.resources.fred_api)
    oil_series = fred.get_series(
        "DCOILWTICO", observation_start=date_str, observation_end=date_str
    )
    if oil_series.empty:
        context.log.warning(f"No oil price on {date_str}")
        oil_val = None
    else:
        oil_val = float(oil_series.iloc[0])

    unem_series = fred.get_series(
        "LRUNTTTTCAQ156S", observation_start=date_str, observation_end=date_str
    )
    if unem_series.empty:
        context.log.warning(f"No unemployment rate on {date_str}")
        unemploy = None
    else:
        unemploy = float(unem_series.iloc[0])

    df = pd.DataFrame(
        {
            "date": [part_dt],
            "y2": [yields["y2"]],
            "y5": [yields["y5"]],
            "y10": [yields["y10"]],
            "spread_2_10": [spread_2_10],
            "oil": [oil_val],
            "unemploy": [unemploy],
        }
    )

    context.add_output_metadata(
        {
            "date": date_str,
            "y2": yields["y2"],
            "y5": yields["y5"],
            "y10": yields["y10"],
            "spread_2_10": spread_2_10,
            "oil": oil_val,
            "unemploy": unemploy,
            "preview": f"{date_str} | y2={yields['y2']:.2f}, y5={yields['y5']:.2f}, "
                       f"y10={yields['y10']:.2f}, oil={oil_val}, unemploy={unemploy}",
        }
    )

    return df