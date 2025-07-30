from dagster import resource
import requests

@resource(config_schema={"base_url": str})
def boc_api(context):
    session = requests.Session()
    session.base_url = context.resource_config["base_url"]
    return session

@resource(config_schema={"api_key": str})
def fred_api(context):
    return context.resource_config["api_key"]