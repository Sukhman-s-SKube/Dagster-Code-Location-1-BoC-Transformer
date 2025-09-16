import os
from dagster import job, op, Definitions, Field
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

@op(
    config_schema={
        "model_bucket": str,
        "model_prefix": str,
        "model_key": Field(str, is_required=False, default_value=""),
        "date_str": Field(str, is_required=False, default_value=""),  # ISO date, e.g., "2024-09-10"
    },
    required_resource_keys={"boc_forecaster_celery"},
)
def enqueue_xgb_prediction(context):

    cfg = context.op_config
    payload = {
        "model_bucket": cfg["model_bucket"],
        "model_prefix": cfg["model_prefix"],
        "model_key": (cfg.get("model_key") or None) if "model_key" in cfg else None,
        "date_str": (cfg.get("date_str") or None) if "date_str" in cfg else None,
    }

    app = context.resources.boc_forecaster_celery
    result = app.send_task("trainer.predict_xgb_from_io", kwargs=payload)
    context.log.info(
        f"Enqueued XGB prediction with payload={payload} task_id={result.id}"
    )
    return {"task_id": result.id}


@op(
    config_schema={
        "model_bucket": str,
        "model_prefix": str,
        "model_key": Field(str, is_required=False, default_value=""),
        "start_date": Field(str, is_required=False, default_value=""),
        "cap_date": Field(str, is_required=False, default_value=""),
        "seq_len": Field(int, is_required=False, default_value=90),
        "horizon": Field(int, is_required=False, default_value=30),
    },
    required_resource_keys={"boc_forecaster_celery"},
)
def enqueue_xgb_evaluation(context):
    cfg = context.op_config
    payload = {
        "model_bucket": cfg["model_bucket"],
        "model_prefix": cfg["model_prefix"],
        "model_key": (cfg.get("model_key") or None) if "model_key" in cfg else None,
        "start_date": (cfg.get("start_date") or None) if "start_date" in cfg else None,
        "cap_date": (cfg.get("cap_date") or None) if "cap_date" in cfg else None,
        "seq_len": cfg.get("seq_len", 90),
        "horizon": cfg.get("horizon", 30),
    }

    app = context.resources.boc_forecaster_celery
    result = app.send_task("trainer.eval_xgb_from_io", kwargs=payload)
    context.log.info(
        f"Enqueued XGB evaluation with payload={payload} task_id={result.id}"
    )
    return {"task_id": result.id}
