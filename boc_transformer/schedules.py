from dagster import (
    ScheduleDefinition,
    define_asset_job,
    AssetSelection,
)

daily_policy_rate_job = define_asset_job(
    name="materialize_daily_policy_rate",
    selection=AssetSelection.keys("daily_policy_rate"),
)

daily_policy_rate_schedule = ScheduleDefinition(
    name="daily_policy_rate_schedule",
    cron_schedule="15 6 * * *",
    execution_timezone="America/Toronto",
    job=daily_policy_rate_job
)
