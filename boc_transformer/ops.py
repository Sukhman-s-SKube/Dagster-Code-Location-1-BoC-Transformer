import os
from dagster import job, op, Definitions
from dagster_aws.s3 import s3_pickle_io_manager, s3_resource
from datetime import datetime
from dateutil.relativedelta import relativedelta
from zoneinfo import ZoneInfo

def _cap_first_of_three_months_ago_toronto() -> str:
    tz = ZoneInfo("America/Toronto")
    now = datetime.now(tz=tz)
    three_ago = now - relativedelta(months=3)
    cap = three_ago.replace(day=1)
    return cap.date().isoformat()

@op
def hello_world():
    return "Hello World!"

@op(config_schema={
    "bucket": str,          # "dagster"
    "io_prefix": str,       # "boc_io_manager"
    "seq_len": int,         # 90
    "horizon": int,         # 7
    "model_bucket": str,    # "models"
    "model_prefix": str,    # "boc_forecaster"
    "max_epochs": int,      # 10
}, required_resource_keys={"boc_forecaster_celery"},)
def enqueue_tft_training(context):
    cap_date = _cap_first_of_three_months_ago_toronto()

    cfg = context.op_config
    payload = {
        "bucket": cfg["bucket"],
        "io_prefix": cfg["io_prefix"],
        "seq_len": cfg["seq_len"],
        "horizon": cfg["horizon"],
        "cap_date": cap_date,
        "model_bucket": cfg["model_bucket"],
        "model_prefix": cfg["model_prefix"],
        "max_epochs": cfg["max_epochs"],
    }

    app = context.resources.boc_forecaster_celery

    result = app.send_task("trainer.train_tft_from_io", kwargs=payload)

    context.log.info(
        f"Enqueued training with payload={payload} task_id={result.id}"
    )
    return {"task_id": result.id, "cap_date": cap_date}
