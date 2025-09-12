import os
from dagster import Definitions

from boc_transformer.assets import (
    daily_policy_rate,
    daily_cpi,
    daily_yield_2y,
    daily_yield_5y,
    daily_yield_10y,
    daily_oil,
    daily_unemployment,
    assemble_macro_daily_row,
)
from boc_transformer.resources import boc_api, fred_api, clickhouse_macro_io_manager
from boc_transformer.schedules import assemble_macro_daily_schedule

defs = Definitions(
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
        "boc_api": boc_api.configured({"base_url": "https://www.bankofcanada.ca/valet"}),
        "fred_api": fred_api.configured({"api_key": os.getenv("FRED_API_KEY")}),
        "clickhouse_macro_io_manager": clickhouse_macro_io_manager,
    },
    schedules=[assemble_macro_daily_schedule],
)
