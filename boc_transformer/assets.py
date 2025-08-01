import os
import numpy as np
import pandas as pd
import pyarrow as pa, pyarrow.parquet as pq
import requests
from dagster import (
    asset,
    multi_asset,
    AssetIn,
    AssetOut,
    DailyPartitionsDefinition,
    WeeklyPartitionsDefinition,
    TimeWindowPartitionMapping
)
from typing import Dict
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

    oil_str = f"{oil_val:.2f}" if oil_val is not None else "NA"
    unemp_str = f"{unemploy:.2f}" if unemploy is not None else "NA"

    context.add_output_metadata({
        "preview": (
            f"{date_str} | y2={yields['y2']:.2f}, "
            f"y5={yields['y5']:.2f}, "
            f"y10={yields['y10']:.2f}, "
            f"spread={spread:.2f}, "
            f"oil={oil_str}, "
            f"unemploy={unemp_str}"
        )
    })

    return df

@multi_asset(
    partitions_def=DAILY,
    ins={
        "daily_policy_rate": AssetIn(
            partition_mapping=TimeWindowPartitionMapping(start_offset=-89, end_offset=0)
        ),
        "daily_cpi": AssetIn(
            partition_mapping=TimeWindowPartitionMapping(start_offset=-89, end_offset=0)
        ),
        "daily_yield_spread_and_macros": AssetIn(
            partition_mapping=TimeWindowPartitionMapping(start_offset=-89, end_offset=0)
        ),
    },
    outs={
        "X": AssetOut(
            description="Feature tensor (N, 90, D)",
            metadata={"dtype": "float32"},
        ),
        "y": AssetOut(
            description="Target array (N,)",
            metadata={"dtype": "float32"},
        ),
    },
    group_name="features",
    description="Merge 90 days of raw data, forward-fill gaps, build sliding windows.",
)
def daily_assemble_big_features(
    context,
    daily_policy_rate: Dict[str, pd.DataFrame],
    daily_cpi: Dict[str, pd.DataFrame],
    daily_yield_spread_and_macros: Dict[str, pd.DataFrame],
):
    def _concat(d):
         if not d:
             return pd.DataFrame(columns=["date"])
         return pd.concat(d.values(), ignore_index=True).sort_values("date")

    daily_policy_rate = _concat(daily_policy_rate)
    daily_cpi          = _concat(daily_cpi)
    daily_yield_spread_and_macros = _concat(daily_yield_spread_and_macros)

    df = (
        daily_policy_rate
        .merge(daily_cpi, on="date", how="outer")
        .merge(daily_yield_spread_and_macros, on="date", how="outer")
        .sort_values("date")
        .ffill()
        .dropna()
    )

    seq_len = 90
    rows, cols = df.shape

    features_dir = os.path.join(context.instance.storage_directory(), "features")
    os.makedirs(features_dir, exist_ok=True)
    parquet_path = os.path.join(features_dir, f"{context.partition_key}.parquet")
    pq.write_table(pa.Table.from_pandas(df), parquet_path)

    if rows < seq_len:
        context.add_output_metadata(
            output_name="X",
            metadata={
                "status": "insufficient_history",
                "rows_available": rows,
                "rows_needed": seq_len,
            },
        )
        return {
            "X": np.empty((0, seq_len, cols - 1), dtype=np.float32),
            "y": np.empty((0,), dtype=np.float32),
        }

    matrix = df.drop(columns="date").to_numpy(dtype=np.float32)
    X = np.stack([matrix[i : i + seq_len] for i in range(rows - seq_len)])
    y = matrix[seq_len:, 0]

    preview = (
        df.head(5)
          .append(df.tail(5))
          .to_markdown(index=False)
    )
    stats_md = df.describe().to_markdown()

    context.add_output_metadata(
        output_name="X",
        metadata={
            "path": parquet_path,
            "rows": rows,
            "cols": cols,
            "windows": X.shape[0],
            "date_start": df["date"].min().strftime("%Y-%m-%d"),
            "date_end": df["date"].max().strftime("%Y-%m-%d"),
            "preview": preview,
            "stats": stats_md,
        },
    )

    return {"X": X, "y": y}