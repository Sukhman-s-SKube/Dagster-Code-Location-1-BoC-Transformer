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

SEQ_LEN   = 90
HORIZON   = 7
STRIDE    = 1

def valet_asof(base_url: str, series: str, date_str: str, lookback_days: int = 540):
    end = pd.to_datetime(date_str)
    start = (end - pd.Timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    r = requests.get(f"{base_url.rstrip('/')}/observations/{series}/json",
                     params={"start_date": start, "end_date": date_str}, timeout=20)
    r.raise_for_status()
    obs = r.json().get("observations", [])
    if not obs:
        return None
    return float(obs[-1][series]["v"])

def fred_asof(fred, series: str, date_str: str, lookback_days: int = 720):
    end = pd.to_datetime(date_str)
    start = end - pd.Timedelta(days=lookback_days)
    s = fred.get_series(series, observation_start=start, observation_end=end)
    if s is None or s.empty:
        return None
    return float(pd.Series(s).dropna().iloc[-1])

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
    d = context.partition_key
    base = context.resources.boc_api.base_url
    # TODO: choose correct series for your intended target (policy rate vs 10y yield)
    series = "CORRECT_SERIES_ID"  # replace!
    val = valet_asof(base, series, d, 540)
    if val is None:
        context.log.warning(f"No as-of value for {series} up to {d}")
        return pd.DataFrame(columns=["date","rate"])
    return pd.DataFrame({"date":[pd.to_datetime(d)], "rate":[val]})

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
    d = context.partition_key
    base = context.resources.boc_api.base_url
    series = "V41690973"
    val = valet_asof(base, series, d, 540)
    if val is None:
        return pd.DataFrame(columns=["date","cpi"])
    return pd.DataFrame({"date":[pd.to_datetime(d)], "cpi":[val]})

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
    d    = context.partition_key
    base = context.resources.boc_api.base_url
    fred = Fred(api_key=context.resources.fred_api)

    y2  = valet_asof(base, "BD.CDN.2YR.DQ.YLD",  d, 540)
    y5  = valet_asof(base, "BD.CDN.5YR.DQ.YLD",  d, 540)
    y10 = valet_asof(base, "BD.CDN.10YR.DQ.YLD", d, 540)
    oil = fred_asof(fred, "DCOILWTICO", d, 540)
    un  = fred_asof(fred, "LRUNTTTTCAQ156S", d, 720)  # quarterly

    if any(v is None for v in (y2, y5, y10)):
        context.log.warning(f"Missing yields as-of {d}")
        return pd.DataFrame(columns=["date","y2","y5","y10","spread_2_10","oil","unemploy"])

    spread = y2 - y10
    return pd.DataFrame({
        "date":[pd.to_datetime(d)],
        "y2":[y2],"y5":[y5],"y10":[y10],
        "spread_2_10":[spread],
        "oil":[oil],"unemploy":[un],
    })

@multi_asset(
    partitions_def=DAILY,
    ins={
        "daily_policy_rate": AssetIn(
            partition_mapping=TimeWindowPartitionMapping(start_offset=-(SEQ_LEN+HORIZON-1),
                                                         end_offset=0)
        ),
        "daily_cpi": AssetIn(
            partition_mapping=TimeWindowPartitionMapping(start_offset=-(SEQ_LEN+HORIZON-1),
                                                         end_offset=0)
        ),
        "daily_yield_spread_and_macros": AssetIn(
            partition_mapping=TimeWindowPartitionMapping(start_offset=-(SEQ_LEN+HORIZON-1),
                                                         end_offset=0)
        ),
    },
    outs={
        "X": AssetOut(description=f"N×{SEQ_LEN}×D feature tensor", metadata={"dtype": "float32"}),
        "Y": AssetOut(description="Nlength target array",        metadata={"dtype": "float32"}),
    },
    group_name="features",
    description=(
        f"Sliding-window {SEQ_LEN}-day feature/target pairs with a {HORIZON} day "
        "forecast horizon."
    ),
)
def daily_assemble_big_features(
    context,
    daily_policy_rate: dict[str, pd.DataFrame],
    daily_cpi: dict[str, pd.DataFrame],
    daily_yield_spread_and_macros: dict[str, pd.DataFrame],
):
    def _concat(obj: pd.DataFrame | dict[str, pd.DataFrame] | None) -> pd.DataFrame:
        if obj is None:
            return pd.DataFrame(columns=["date"])
        if isinstance(obj, pd.DataFrame):
            return obj.sort_values("date")
        if isinstance(obj, dict) and obj:
            return pd.concat(obj.values(), ignore_index=True).sort_values("date")
        return pd.DataFrame(columns=["date"])
    
    raw = (
        _concat(daily_policy_rate)
        .merge(_concat(daily_cpi), on="date", how="outer")
        .merge(_concat(daily_yield_spread_and_macros), on="date", how="outer")
        .sort_values("date")
        .set_index("date")
    )

    raw_first_date = raw.index.min() if not raw.empty else pd.NaT

    full_range = pd.date_range(end=context.partition_key, periods=SEQ_LEN + HORIZON, freq="D")
    df = (
        raw.reindex(full_range)   # align to exact window
           .ffill()               # forward-fill only (NO bfill to avoid leakage)
           .reset_index()
           .rename(columns={"index": "date"})
    )

    feature_cols = [c for c in df.columns if c != "date"]
    y_col = "rate"
    if y_col not in feature_cols:
        raise ValueError(f"Target column '{y_col}' not found in assembled dataframe. Got: {feature_cols}")

    nan_counts = df[feature_cols].isna().sum().to_dict()
    has_nan = any(v > 0 for v in nan_counts.values())

    rows, cols = df.shape
    windows_available = max((rows - SEQ_LEN - HORIZON) // STRIDE + 1, 0)

    if pd.isna(raw_first_date):
        padded_days = SEQ_LEN + HORIZON
    else:
        padded_days = max(0, int((full_range[0] - raw_first_date).days))

    features_dir = os.path.join(context.instance.storage_directory(), "features")
    os.makedirs(features_dir, exist_ok=True)
    parquet_path = os.path.join(features_dir, f"{context.partition_key}.parquet")
    pq.write_table(pa.Table.from_pandas(df), parquet_path)

    if windows_available == 0 or has_nan:
        context.add_output_metadata(
            output_name="X",
            metadata={
                "status": "insufficient_history" if not has_nan else "insufficient_asof_history",
                "rows_available": rows,
                "rows_needed": SEQ_LEN + HORIZON,
                "padded_days": int(padded_days),
                "nan_counts": {k: int(v) for k, v in nan_counts.items()},
                "columns": feature_cols,
                "y_col": y_col,
                "ffill_only": True,
                "head_tail": pd.concat([df.head(3), df.tail(3)], ignore_index=True).to_markdown(index=False),
                "path": parquet_path,
            },
        )
        empty_shape = (0, SEQ_LEN, len(feature_cols))
        return np.empty(empty_shape, np.float32), np.empty((0,), np.float32)


    feat_df = df.drop(columns="date")
    matrix = feat_df.to_numpy(dtype=np.float32)
    y_idx = feat_df.columns.get_loc(y_col)

    idx = range(0, rows - SEQ_LEN - HORIZON + 1, STRIDE)
    X = np.stack([matrix[i : i + SEQ_LEN] for i in idx]).astype(np.float32, copy=False)
    Y = np.stack([matrix[i + SEQ_LEN + HORIZON - 1, y_idx] for i in idx]).astype(np.float32, copy=False)

    if not np.isfinite(Y).all():
        raise ValueError("Non-finite values found in Y after assembly. Check source assets and ffill window.")


    context.add_output_metadata(
        output_name="X",
        metadata={
            "path": parquet_path,
            "rows": rows,
            "cols": len(feature_cols),
            "columns": feature_cols,
            "y_col": y_col,
            "windows": int(X.shape[0]),
            "horizon_days": HORIZON,
            "rate_std": float(df[y_col].std(skipna=True)),
            "date_start": str(df["date"].min().date()),
            "date_end": str(df["date"].max().date()),
            "padded_days": int(padded_days),
            "ffill_only": True,
            "preview": pd.concat([df.head(5), df.tail(5)], ignore_index=True).to_markdown(index=False),
            "stats": df[feature_cols].describe().to_markdown(),
        },
    )

    return X, Y