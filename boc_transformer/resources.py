import os
from dagster import resource, IOManager, io_manager
import requests
import pandas as pd
import clickhouse_connect

class ClickHouseDailyRowIOManager(IOManager):
    def __init__(self, host, port, username, password, database="features", table="macro_daily"):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.table = table
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
        )

    def handle_output(self, context, obj: pd.DataFrame):
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        self.client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {self.database}.{self.table} (
              date Date,
              rate Float64, cpi Float64,
              y2 Float64, y5 Float64, y10 Float64,
              spread_2_10 Float64, oil Float64, unemploy Float64
            ) ENGINE=MergeTree
            PARTITION BY toYYYYMM(date)
            ORDER BY date
            """
        )

        if obj is None or obj.empty:
            context.log.info("No rows to write to ClickHouse for this partition.")
            return

        df = obj.copy()
        df["date"] = pd.to_datetime(df["date"]).dt.date

        self.client.command(
            f"ALTER TABLE {self.database}.{self.table} DELETE WHERE date = toDate(%(d)s)",
            parameters={"d": context.partition_key},
        )

        self.client.insert_df(f"{self.database}.{self.table}", df)

    def load_input(self, context):
        self.client.command(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        self.client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {self.database}.{self.table} (
              date Date,
              rate Float64, cpi Float64,
              y2 Float64, y5 Float64, y10 Float64,
              spread_2_10 Float64, oil Float64, unemploy Float64
            ) ENGINE=MergeTree
            PARTITION BY toYYYYMM(date)
            ORDER BY date
            """
        )

        pk = None
        if hasattr(context, "asset_partition_key"):
            pk = context.asset_partition_key
        if pk is None and getattr(context, "upstream_output", None) is not None:
            pk = getattr(context.upstream_output, "partition_key", None)
        if pk is None and hasattr(context, "partition_key"):
            pk = context.partition_key

        if not pk:
            return pd.DataFrame(
                columns=[
                    "date",
                    "rate",
                    "cpi",
                    "y2",
                    "y5",
                    "y10",
                    "spread_2_10",
                    "oil",
                    "unemploy",
                ]
            )

        query = (
            f"SELECT date, rate, cpi, y2, y5, y10, spread_2_10, oil, unemploy "
            f"FROM {self.database}.{self.table} "
            f"WHERE date = toDate(%(d)s)"
        )

        df = self.client.query_df(query, parameters={"d": pk})
        if not df.empty:
            df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None)
            for c in [
                "rate",
                "cpi",
                "y2",
                "y5",
                "y10",
                "spread_2_10",
                "oil",
                "unemploy",
            ]:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype("float64")
        return df

@resource(config_schema={"base_url": str})
def boc_api(context):
    session = requests.Session()
    session.base_url = context.resource_config["base_url"]
    return session

@resource(config_schema={"api_key": str})
def fred_api(context):
    return context.resource_config["api_key"]

@io_manager
def clickhouse_macro_io_manager(_):
    return ClickHouseDailyRowIOManager(
        host=os.getenv("CH_HOST"),
        port=int(os.getenv("CH_PORT", "30090")),
        username=os.getenv("CH_USER", "app"),
        password=os.getenv("CH_PASS", ""),
        database=os.getenv("CH_DB", "features"),
        table=os.getenv("CH_TABLE", "macro_daily"),
    )
