import os
from dagster import job, op, Definitions
from dagster_aws.s3 import s3_pickle_io_manager, s3_resource
from .assets import daily_policy_rate, daily_cpi
from .resources import fred_api, boc_api
from .schedules import daily_policy_rate_schedule, daily_policy_rate_job, daily_cpi_job, daily_cpi_schedule

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

@op
def hello_world():
    return "Hello World!"

@job
def demo_job():
    hello_world()

defs = Definitions(jobs=[demo_job, daily_policy_rate_job, daily_cpi_job], assets=[daily_policy_rate, daily_cpi],
    resources={
        "s3": s3,
        "io_manager": io_manager,
        "boc_api": boc_api.configured({"base_url": "https://www.bankofcanada.ca/valet"}),
        "fred_api": fred_api.configured({"api_key": os.getenv("FRED_API_KEY")})
}, schedules=[daily_policy_rate_schedule, daily_cpi_schedule])
