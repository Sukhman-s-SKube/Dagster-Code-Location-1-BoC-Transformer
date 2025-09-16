import os
from dagster import job, Definitions
from celery import Celery
from dagster_aws.s3 import s3_pickle_io_manager, s3_resource

from .assets import (
    daily_policy_rate,
    daily_cpi,
    daily_yield_2y,
    daily_yield_5y,
    daily_yield_10y,
    daily_oil,
    daily_unemployment,
    assemble_macro_daily_row,
)
from .resources import fred_api, boc_api, clickhouse_macro_io_manager
from .schedules import (
    daily_policy_rate_schedule,
    daily_policy_rate_job,
    daily_cpi_schedule,
    daily_cpi_job,
    daily_yield_2y_schedule,
    daily_yield_2y_job,
    daily_yield_5y_schedule,
    daily_yield_5y_job,
    daily_yield_10y_schedule,
    daily_yield_10y_job,
    daily_oil_schedule,
    daily_oil_job,
    daily_unemployment_schedule,
    daily_unemployment_job,
    assemble_macro_daily_schedule,
    assemble_macro_daily_job,
)
from .ops import (
    hello_world,
    enqueue_xgb_evaluation,
    enqueue_xgb_prediction,
    xg_boost_train,
    xg_boost_predict_today,
    xg_boost_evaluate_recent,
)

io_manager = s3_pickle_io_manager.configured({
    "s3_bucket":       {"env": "S3_BUCKET"},
    "s3_prefix":       "boc_io_manager",
})

s3 = s3_resource.configured(
    {
        "endpoint_url": {"env": "AWS_ENDPOINT_URL"},
        "region_name":       {"env": "AWS_REGION"},
        "use_ssl":      False,
    }
)

broker = os.getenv("REDIS_BROKER_URL")
backend = os.getenv("CELERY_BACKEND_URL")

boc_forecaster_celery = Celery("enqueue", broker=broker, backend=backend)

@job
def demo_job():
    hello_world()

@job
def xgb_training_job():
    xg_boost_train()

@job
def xgb_prediction_today_job():
    xg_boost_predict_today()

@job
def xgb_evaluation_recent_job():
    xg_boost_evaluate_recent()

defs = Definitions(
    jobs=[
        demo_job,
        xgb_training_job,
        xgb_prediction_today_job,
        xgb_evaluation_recent_job,
        daily_policy_rate_job,
        daily_cpi_job,
        daily_yield_2y_job,
        daily_yield_5y_job,
        daily_yield_10y_job,
        daily_oil_job,
        daily_unemployment_job,
        assemble_macro_daily_job,
    ],
    assets=[
        daily_policy_rate,
        daily_cpi,
        daily_yield_2y,
        daily_yield_5y,
        daily_yield_10y,
        daily_oil,
        daily_unemployment,
        assemble_macro_daily_row,
    ],
    resources={
        "s3": s3,
        "io_manager": io_manager,
        "boc_api": boc_api.configured({"base_url": "https://www.bankofcanada.ca/valet"}),
        "fred_api": fred_api.configured({"api_key": os.getenv("FRED_API_KEY")}),
        "clickhouse_macro_io_manager": clickhouse_macro_io_manager,
        "boc_forecaster_celery": boc_forecaster_celery,
    },
    schedules=[
        daily_policy_rate_schedule,
        daily_cpi_schedule,
        daily_yield_2y_schedule,
        daily_yield_5y_schedule,
        daily_yield_10y_schedule,
        daily_oil_schedule,
        daily_unemployment_schedule,
        assemble_macro_daily_schedule,
    ],
)
