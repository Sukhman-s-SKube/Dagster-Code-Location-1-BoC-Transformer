from dagster import job, op, Definitions

@op
def hello_world():
    return "Hello World!"

@job
def demo_job():
    hello_world()

defs = Definitions(jobs=[demo_job])
