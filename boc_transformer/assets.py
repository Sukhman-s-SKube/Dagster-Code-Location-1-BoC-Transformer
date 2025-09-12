import os
import pandas as pd
import requests
from dagster import (
    asset,
    AssetIn,
    DailyPartitionsDefinition,
)
from typing import Dict, Tuple, Optional
from fredapi import Fred

DAILY = DailyPartitionsDefinition(start_date="2015-01-01")

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

def valet_asof_with_date(
    base_url: str,
    series: str,
    date_str: str,
    lookback_days: int = 540,
) -> Tuple[Optional[float], Optional[pd.Timestamp], str]:
    end = pd.to_datetime(date_str)
    start = (end - pd.Timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    url = f"{base_url.rstrip('/')}/observations/{series}/json"
    r = requests.get(url, params={"start_date": start, "end_date": date_str}, timeout=20)
    r.raise_for_status()
    obs = r.json().get("observations", [])
    if not obs:
        return None, None, r.url
    for rec in reversed(obs):
        cell = rec.get(series) or {}
        v = cell.get("v")
        if v not in (None, ""):
            return float(v), pd.to_datetime(rec.get("d")), r.url
    return None, None, r.url

def fred_asof(fred, series: str, date_str: str, lookback_days: int = 720):
    end = pd.to_datetime(date_str)
    start = end - pd.Timedelta(days=lookback_days)
    s = fred.get_series(series, observation_start=start, observation_end=end)
    if s is None or s.empty:
        return None
    return float(pd.Series(s).dropna().iloc[-1])

def fred_asof_with_date(
    fred,
    series: str,
    date_str: str,
    lookback_days: int = 720,
) -> Tuple[Optional[float], Optional[pd.Timestamp]]:
    end = pd.to_datetime(date_str)
    start = end - pd.Timedelta(days=lookback_days)
    s = fred.get_series(series, observation_start=start, observation_end=end)
    if s is None or len(s) == 0:
        return None, None
    ser = pd.Series(s).dropna()
    if ser.empty:
        return None, None
    return float(ser.iloc[-1]), pd.to_datetime(ser.index[-1])

@asset(
    partitions_def=DAILY,
    required_resource_keys={"boc_api"},
    group_name="raw_daily",
    tags={"layer": "raw", "source": "BoC"},
    metadata={
        "columns": ["date", "y2"],
        "series_id": "BD.CDN.2YR.DQ.YLD",
    },
)
def daily_yield_2y(context) -> pd.DataFrame:
    d = context.partition_key
    part_dt = pd.to_datetime(d)
    base = context.resources.boc_api.base_url
    val, asof_date, query_url = valet_asof_with_date(base, "BD.CDN.2YR.DQ.YLD", d, 540)
    if val is None:
        context.add_output_metadata({
            "date": d,
            "series_id": "BD.CDN.2YR.DQ.YLD",
            "status": "no_data_in_lookback",
            "lookback_days": 540,
            "query_url": query_url,
        })
        return pd.DataFrame(columns=["date", "y2"])
    staleness_days = None if asof_date is None else int((part_dt.normalize() - asof_date.normalize()).days)
    context.add_output_metadata({
        "date": d,
        "series_id": "BD.CDN.2YR.DQ.YLD",
        "y2": float(val),
        "asof_date": None if asof_date is None else asof_date.date().isoformat(),
        "staleness_days": staleness_days,
        "lookback_days": 540,
        "query_url": query_url,
        "preview": f"{d}: y2={float(val):.3f} (as-of {None if asof_date is None else asof_date.date()}, {staleness_days}d stale)",
    })
    return pd.DataFrame({"date": [part_dt], "y2": [float(val)]})

@asset(
    partitions_def=DAILY,
    required_resource_keys={"boc_api"},
    group_name="raw_daily",
    tags={"layer": "raw", "source": "BoC"},
    metadata={
        "columns": ["date", "y5"],
        "series_id": "BD.CDN.5YR.DQ.YLD",
    },
)
def daily_yield_5y(context) -> pd.DataFrame:
    d = context.partition_key
    part_dt = pd.to_datetime(d)
    base = context.resources.boc_api.base_url
    val, asof_date, query_url = valet_asof_with_date(base, "BD.CDN.5YR.DQ.YLD", d, 540)
    if val is None:
        context.add_output_metadata({
            "date": d,
            "series_id": "BD.CDN.5YR.DQ.YLD",
            "status": "no_data_in_lookback",
            "lookback_days": 540,
            "query_url": query_url,
        })
        return pd.DataFrame(columns=["date", "y5"])
    staleness_days = None if asof_date is None else int((part_dt.normalize() - asof_date.normalize()).days)
    context.add_output_metadata({
        "date": d,
        "series_id": "BD.CDN.5YR.DQ.YLD",
        "y5": float(val),
        "asof_date": None if asof_date is None else asof_date.date().isoformat(),
        "staleness_days": staleness_days,
        "lookback_days": 540,
        "query_url": query_url,
        "preview": f"{d}: y5={float(val):.3f} (as-of {None if asof_date is None else asof_date.date()}, {staleness_days}d stale)",
    })
    return pd.DataFrame({"date": [part_dt], "y5": [float(val)]})

@asset(
    partitions_def=DAILY,
    required_resource_keys={"boc_api"},
    group_name="raw_daily",
    tags={"layer": "raw", "source": "BoC"},
    metadata={
        "columns": ["date", "y10"],
        "series_id": "BD.CDN.10YR.DQ.YLD",
    },
)
def daily_yield_10y(context) -> pd.DataFrame:
    d = context.partition_key
    part_dt = pd.to_datetime(d)
    base = context.resources.boc_api.base_url
    val, asof_date, query_url = valet_asof_with_date(base, "BD.CDN.10YR.DQ.YLD", d, 540)
    if val is None:
        context.add_output_metadata({
            "date": d,
            "series_id": "BD.CDN.10YR.DQ.YLD",
            "status": "no_data_in_lookback",
            "lookback_days": 540,
            "query_url": query_url,
        })
        return pd.DataFrame(columns=["date", "y10"])
    staleness_days = None if asof_date is None else int((part_dt.normalize() - asof_date.normalize()).days)
    context.add_output_metadata({
        "date": d,
        "series_id": "BD.CDN.10YR.DQ.YLD",
        "y10": float(val),
        "asof_date": None if asof_date is None else asof_date.date().isoformat(),
        "staleness_days": staleness_days,
        "lookback_days": 540,
        "query_url": query_url,
        "preview": f"{d}: y10={float(val):.3f} (as-of {None if asof_date is None else asof_date.date()}, {staleness_days}d stale)",
    })
    return pd.DataFrame({"date": [part_dt], "y10": [float(val)]})

@asset(
    partitions_def=DAILY,
    required_resource_keys={"fred_api"},
    group_name="raw_daily",
    tags={"layer": "raw", "source": "FRED"},
    metadata={
        "columns": ["date", "oil"],
        "series_id": "DCOILWTICO",
    },
)
def daily_oil(context) -> pd.DataFrame:
    d = context.partition_key
    part_dt = pd.to_datetime(d)
    fred = Fred(api_key=context.resources.fred_api)
    val, asof_date = fred_asof_with_date(fred, "DCOILWTICO", d, 540)
    if val is None:
        context.add_output_metadata({
            "date": d,
            "series_id": "DCOILWTICO",
            "status": "no_data_in_lookback",
            "lookback_days": 540,
        })
        return pd.DataFrame(columns=["date", "oil"])
    staleness_days = None if asof_date is None else int((part_dt.normalize() - asof_date.normalize()).days)
    context.add_output_metadata({
        "date": d,
        "series_id": "DCOILWTICO",
        "oil": float(val),
        "asof_date": None if asof_date is None else asof_date.date().isoformat(),
        "staleness_days": staleness_days,
        "lookback_days": 540,
        "preview": f"{d}: oil={float(val):.2f} (as-of {None if asof_date is None else asof_date.date()}, {staleness_days}d stale)",
    })
    return pd.DataFrame({"date": [part_dt], "oil": [float(val)]})

@asset(
    partitions_def=DAILY,
    required_resource_keys={"fred_api"},
    group_name="raw_daily",
    tags={"layer": "raw", "source": "FRED"},
    metadata={
        "columns": ["date", "unemploy"],
        "series_id": "LRUNTTTTCAQ156S",
    },
)
def daily_unemployment(context) -> pd.DataFrame:
    d = context.partition_key
    part_dt = pd.to_datetime(d)
    fred = Fred(api_key=context.resources.fred_api)
    val, asof_date = fred_asof_with_date(fred, "LRUNTTTTCAQ156S", d, 720)
    if val is None:
        context.add_output_metadata({
            "date": d,
            "series_id": "LRUNTTTTCAQ156S",
            "status": "no_data_in_lookback",
            "lookback_days": 720,
        })
        return pd.DataFrame(columns=["date", "unemploy"])
    staleness_days = None if asof_date is None else int((part_dt.normalize() - asof_date.normalize()).days)
    context.add_output_metadata({
        "date": d,
        "series_id": "LRUNTTTTCAQ156S",
        "unemploy": float(val),
        "asof_date": None if asof_date is None else asof_date.date().isoformat(),
        "staleness_days": staleness_days,
        "lookback_days": 720,
        "preview": f"{d}: unemploy={float(val):.2f}% (as-of {None if asof_date is None else asof_date.date()}, {staleness_days}d stale)",
    })
    return pd.DataFrame({"date": [part_dt], "unemploy": [float(val)]})

@asset(
    partitions_def=DAILY,
    required_resource_keys={"boc_api"},
    group_name="raw_daily",
    tags={"layer": "raw", "source": "BoC"},
    metadata={
        "frequency":  "daily (as-of; sparse updates)",
        "columns":    ["date", "rate"],
        "series_id":  "B114039",
        "source_url": "https://www.bankofcanada.ca/valet",
        "unit":       "percent",
    },
    description="BoC policy rate (target for the overnight rate), exposed daily via logic.",
)
def daily_policy_rate(context) -> pd.DataFrame:
    import requests
    import pandas as pd

    SERIES_ID = "B114039"
    LOOKBACK_DAYS = 540

    part_str = context.partition_key
    part_dt = pd.to_datetime(part_str)
    base = context.resources.boc_api.base_url.rstrip("/")

    start_str = (part_dt - pd.Timedelta(days=LOOKBACK_DAYS)).date().isoformat()
    end_str = part_dt.date().isoformat()

    url = f"{base}/observations/{SERIES_ID}/json"
    params = {"start_date": start_str, "end_date": end_str}
    r = requests.get(url, params=params)

    if r.status_code == 404:
        raise RuntimeError(
            f"BoC Valet 404 for series '{SERIES_ID}'. "
            f"Check the series id and base_url: {url}"
        )
    r.raise_for_status()

    data = r.json()
    obs = data.get("observations", []) or []

    asof_date = None
    asof_val = None
    for rec in reversed(obs):
        cell = rec.get(SERIES_ID) or {}
        v = cell.get("v")
        if v not in (None, ""):
            asof_val = float(v)
            asof_date = pd.to_datetime(rec.get("d"))
            break

    if asof_val is None:
        context.log.warning(
            f"No '{SERIES_ID}' observations in {start_str}..{end_str}; "
            "downstream will report insufficient_asof_history."
        )
        context.add_output_metadata(
            {
                "date": part_str,
                "series_id": SERIES_ID,
                "status": "no_data_in_lookback",
                "lookback_days": LOOKBACK_DAYS,
                "query_url": r.url,
            }
        )
        return pd.DataFrame(columns=["date", "rate"])

    staleness_days = int((part_dt.normalize() - asof_date.normalize()).days)
    df = pd.DataFrame({"date": [part_dt], "rate": [asof_val]})

    context.add_output_metadata(
        {
            "date": part_str,
            "series_id": SERIES_ID,
            "rate": asof_val,
            "asof_date": asof_date.date().isoformat(),
            "staleness_days": staleness_days,
            "lookback_days": LOOKBACK_DAYS,
            "preview": f"{part_str}: {asof_val:.2f}% (as-of {asof_date.date()}, {staleness_days}d stale)",
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
    d = context.partition_key
    part_dt = pd.to_datetime(d)
    base = context.resources.boc_api.base_url
    series = "V41690973"

    try:
        val, asof_date, query_url = valet_asof_with_date(base, series, d, 540)
    except requests.HTTPError as e:
        raise RuntimeError(
            f"BoC Valet error for series '{series}' while fetching CPI: {e}"
        ) from e

    if val is None or asof_date is None:
        context.log.warning(
            f"No '{series}' observations within lookback as-of {d}"
        )
        context.add_output_metadata(
            {
                "date": d,
                "series_id": series,
                "status": "no_data_in_lookback",
                "lookback_days": 540,
                "query_url": query_url,
            }
        )
        return pd.DataFrame(columns=["date", "cpi"])

    staleness_days = int((part_dt.normalize() - asof_date.normalize()).days)
    context.add_output_metadata(
        {
            "date": d,
            "series_id": series,
            "cpi": val,
            "asof_date": asof_date.date().isoformat(),
            "staleness_days": staleness_days,
            "lookback_days": 540,
            "preview": f"{d}: CPI={val:.2f} (as-of {asof_date.date()}, {staleness_days}d stale)",
        }
    )
    return pd.DataFrame({"date": [part_dt], "cpi": [val]})

@asset(
    partitions_def=DAILY,
    ins={
        "daily_policy_rate": AssetIn(),
        "daily_cpi": AssetIn(),
        "daily_yield_2y": AssetIn(),
        "daily_yield_5y": AssetIn(),
        "daily_yield_10y": AssetIn(),
        "daily_oil": AssetIn(),
        "daily_unemployment": AssetIn(),
    },
    io_manager_key="clickhouse_macro_io_manager",
    group_name="features",
)
def assemble_macro_daily_row(
    context,
    daily_policy_rate: pd.DataFrame,
    daily_cpi: pd.DataFrame,
    daily_yield_2y: pd.DataFrame,
    daily_yield_5y: pd.DataFrame,
    daily_yield_10y: pd.DataFrame,
    daily_oil: pd.DataFrame,
    daily_unemployment: pd.DataFrame,
) -> pd.DataFrame:
    d = pd.to_datetime(context.partition_key)
    row = pd.DataFrame({"date": [d]})

    def merge(base_df: Optional[pd.DataFrame], cols: list[str]):
        nonlocal row
        base = pd.DataFrame(columns=["date"] + cols) if base_df is None or base_df.empty else base_df[["date"] + cols]
        row = row.merge(base, on="date", how="left")

    merge(daily_policy_rate, ["rate"])
    merge(daily_cpi, ["cpi"])
    merge(daily_yield_2y, ["y2"])
    merge(daily_yield_5y, ["y5"])
    merge(daily_yield_10y, ["y10"])
    merge(daily_oil, ["oil"])
    merge(daily_unemployment, ["unemploy"])

    row["spread_2_10"] = row["y2"] - row["y10"]
    out = row[["date", "rate", "cpi", "y2", "y5", "y10", "spread_2_10", "oil", "unemploy"]].copy()
    out["date"] = pd.to_datetime(out["date"])
    for c in ["rate", "cpi", "y2", "y5", "y10", "spread_2_10", "oil", "unemploy"]:
        out[c] = pd.to_numeric(out[c], errors="coerce").astype("float64")
    # Emit observability metadata
    vals = out.iloc[0].to_dict() if not out.empty else {}
    missing = [k for k in ["rate", "cpi", "y2", "y5", "y10", "oil", "unemploy"] if (not vals) or pd.isna(vals.get(k))]
    context.add_output_metadata({
        "date": str(pd.to_datetime(context.partition_key).date()),
        "missing_fields": missing,
        "spread_2_10": None if not vals else vals.get("spread_2_10"),
        "preview": None if not vals else (
            f"rate={vals.get('rate')}, cpi={vals.get('cpi')}, y2={vals.get('y2')}, y10={vals.get('y10')}, "
            f"spread={vals.get('spread_2_10')}, oil={vals.get('oil')}, un={vals.get('unemploy')}"
        ),
    })
    return out
