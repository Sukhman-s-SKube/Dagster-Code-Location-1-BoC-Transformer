from dagster import (
    ScheduleDefinition,
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

daily_policy_rate_schedule = ScheduleDefinition(
    name="daily_policy_rate_schedule",
    cron_schedule="15 6 * * *",
    execution_timezone="America/Toronto",
    job=daily_policy_rate_job
)

daily_cpi_schedule = ScheduleDefinition(
    name="daily_cpi_schedule",
    cron_schedule="15 7 * * *",
    execution_timezone="America/Toronto",
    job=daily_cpi_job
)

daily_yield_spread_and_macros_schedule = ScheduleDefinition(
    name="daily_yield_spread_and_macros_schedule",
    cron_schedule="15 8 * * *",
    execution_timezone="America/Toronto",
    job=daily_yield_spread_and_macros_job
)

daily_features_schedule = ScheduleDefinition(
    name="daily_features_schedule",
    cron_schedule="15 10 * * *",
    execution_timezone="America/Toronto",
    job=daily_features_job
)
