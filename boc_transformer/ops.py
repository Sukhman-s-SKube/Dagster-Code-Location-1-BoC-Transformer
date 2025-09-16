import os
from dagster import job, op, Definitions, Field
from dagster_aws.s3 import s3_pickle_io_manager, s3_resource
from datetime import datetime, timedelta
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
    },
    required_resource_keys={"boc_forecaster_celery"},
)
def xg_boost_predict_today(context):
    cfg = context.op_config
    tz = ZoneInfo("America/Toronto")
    date_str = (datetime.now(tz=tz) - timedelta(days=1)).date().isoformat()

    payload = {
        "model_bucket": cfg["model_bucket"],
        "model_prefix": cfg["model_prefix"],
        "model_key": (cfg.get("model_key") or None) if "model_key" in cfg else None,
        "date_str": date_str,
    }

    app = context.resources.boc_forecaster_celery
    result = app.send_task("trainer.predict_xgb_from_io", kwargs=payload)
    context.log.info(
        f"Enqueued XGB prediction for {date_str} (yesterday) with payload={payload} task_id={result.id}"
    )
    return {"task_id": result.id, "date": date_str}


@op(
    config_schema={
        "model_bucket": str,
        "model_prefix": str,
        "model_key": Field(str, is_required=False, default_value=""),
        "months_back": Field(int, is_required=False, default_value=3),
    },
    required_resource_keys={"boc_forecaster_celery"},
)
def xg_boost_evaluate_recent(context):
    cfg = context.op_config
    tz = ZoneInfo("America/Toronto")
    months_back = max(0, int(cfg.get("months_back", 3) or 3))
    start_dt = (datetime.now(tz=tz) - relativedelta(months=months_back)).replace(day=1)
    start_date = start_dt.date().isoformat()

    payload = {
        "model_bucket": cfg["model_bucket"],
        "model_prefix": cfg["model_prefix"],
        "model_key": (cfg.get("model_key") or None) if "model_key" in cfg else None,
        "start_date": start_date,
    }

    app = context.resources.boc_forecaster_celery
    result = app.send_task("trainer.eval_xgb_from_io", kwargs=payload)
    context.log.info(
        f"Enqueued XGB evaluation from {start_date} (to yesterday) with payload={payload} task_id={result.id}"
    )
    return {"task_id": result.id, "start_date": start_date}

@op(
    config_schema={
        "seq_len": int,          #e.g. 90
        "horizon": int,          #e.g. 30
        "model_bucket": str,     #e.g. "models"
        "model_prefix": str,     #e.g. "boc_policy_classifier"
        #optional controls
        "start_date": Field(str, is_required=False, default_value=""),           #ISO date
        "cap_date": Field(str, is_required=False, default_value=""),             #ISO date; default computed
        "val_holdout_days": Field(int, is_required=False, default_value=0),       #0 worker default
        "recency_half_life_days": Field(int, is_required=False, default_value=0), #0 disabled
    },
    required_resource_keys={"boc_forecaster_celery"},
)
def xg_boost_train(context):
    cfg = context.op_config

    cap_date = cfg.get("cap_date") or _cap_first_of_three_months_ago_toronto()
    payload = {
        "seq_len": cfg["seq_len"],
        "horizon": cfg["horizon"],
        "cap_date": cap_date,
        "model_bucket": cfg["model_bucket"],
        "model_prefix": cfg["model_prefix"],
    }

    if cfg.get("start_date"):
        payload["start_date"] = cfg["start_date"]
    if int(cfg.get("val_holdout_days", 0) or 0) > 0:
        payload["val_holdout_days"] = int(cfg["val_holdout_days"]) 
    if int(cfg.get("recency_half_life_days", 0) or 0) > 0:
        payload["recency_half_life_days"] = int(cfg["recency_half_life_days"]) 

    app = context.resources.boc_forecaster_celery
    result = app.send_task("trainer.train_xgb_from_io", kwargs=payload)

    context.log.info(
        f"Enqueued XGB training with payload={payload} task_id={result.id}"
    )
    return {"task_id": result.id, "cap_date": cap_date}

@op(
    config_schema={
        "model_bucket": str,
        "model_prefix": str,
        "model_key": Field(str, is_required=False, default_value=""),
        "date_str": Field(str, is_required=False, default_value=""),
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
