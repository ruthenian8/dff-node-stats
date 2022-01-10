from fastapi import FastAPI
import pandas as pd

def api_run(self, app: FastAPI, df: pd.DataFrame) -> FastAPI:
    """
    Attach methods to api, returns unchanged object
    if no endpoints should be added
    """
    raise NotImplementedError


def api_run(self, app: FastAPI, df: pd.DataFrame) -> FastAPI:
    return app