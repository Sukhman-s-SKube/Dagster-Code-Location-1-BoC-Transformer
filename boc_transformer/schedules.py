from dagster import build_schedule_from_partitioned_job, define_asset_job, AssetSelection

daily_policy_rate_job = define_asset_job(
    name="materialize_daily_policy_rate",
    selection=AssetSelection.keys("daily_policy_rate"),
)
daily_cpi_job = define_asset_job(
    name="materialize_daily_cpi",
    selection=AssetSelection.keys("daily_cpi"),
)
daily_yield_2y_job = define_asset_job(
    name="materialize_daily_yield_2y",
    selection=AssetSelection.keys("daily_yield_2y"),
)
daily_yield_5y_job = define_asset_job(
    name="materialize_daily_yield_5y",
    selection=AssetSelection.keys("daily_yield_5y"),
)
daily_yield_10y_job = define_asset_job(
    name="materialize_daily_yield_10y",
    selection=AssetSelection.keys("daily_yield_10y"),
)
daily_oil_job = define_asset_job(
    name="materialize_daily_oil",
    selection=AssetSelection.keys("daily_oil"),
)
daily_unemployment_job = define_asset_job(
    name="materialize_daily_unemployment",
    selection=AssetSelection.keys("daily_unemployment"),
)

assemble_macro_daily_job = define_asset_job(
    name="materialize_assemble_macro_daily_row",
    selection=AssetSelection.keys("assemble_macro_daily_row"),
)

daily_policy_rate_schedule = build_schedule_from_partitioned_job(job=daily_policy_rate_job)
daily_cpi_schedule = build_schedule_from_partitioned_job(job=daily_cpi_job)
daily_yield_2y_schedule = build_schedule_from_partitioned_job(job=daily_yield_2y_job)
daily_yield_5y_schedule = build_schedule_from_partitioned_job(job=daily_yield_5y_job)
daily_yield_10y_schedule = build_schedule_from_partitioned_job(job=daily_yield_10y_job)
daily_oil_schedule = build_schedule_from_partitioned_job(job=daily_oil_job)
daily_unemployment_schedule = build_schedule_from_partitioned_job(job=daily_unemployment_job)
assemble_macro_daily_schedule = build_schedule_from_partitioned_job(job=assemble_macro_daily_job)
