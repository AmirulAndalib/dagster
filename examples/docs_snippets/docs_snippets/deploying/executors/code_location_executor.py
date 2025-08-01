import dagster as dg


@dg.definitions
def executor() -> dg.Definitions:
    return dg.Definitions(executor=dg.multiprocess_executor)
