from dagster import (
    build_schedule_from_partitioned_job,
    define_asset_job,
    AssetSelection,
)

daily_policy_rate_job = define_asset_job(
    name="materialize_daily_policy_rate",
    selection=AssetSelection.keys("daily_policy_rate")
)

daily_cpi_job = define_asset_job(
    name="materialize_daily_cpi",
    selection=AssetSelection.keys("daily_cpi")
)

daily_yield_spread_and_macros_job = define_asset_job(
    name="materialize_daily_yield_spread_and_macros",
    selection=AssetSelection.keys("daily_yield_spread_and_macros")
)

daily_features_job = define_asset_job(
    name="materialize_daily_features",
    selection=AssetSelection.keys("X", "Y")
)

daily_policy_rate_schedule = build_schedule_from_partitioned_job(
    job=daily_policy_rate_job,
    cron_schedule="15 6 * * *",
    execution_timezone="America/Toronto",
)

daily_cpi_schedule = build_schedule_from_partitioned_job(
    job=daily_cpi_job,
    cron_schedule="15 7 * * *",
    execution_timezone="America/Toronto",
)

daily_yield_spread_and_macros_schedule = build_schedule_from_partitioned_job(
    job=daily_yield_spread_and_macros_job,
    cron_schedule="15 8 * * *",
    execution_timezone="America/Toronto",
)

daily_features_schedule = build_schedule_from_partitioned_job(
    job=daily_features_job,
    cron_schedule="15 10 * * *",
    execution_timezone="America/Toronto",
)