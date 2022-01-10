from functools import partial
from typing import Callable, List

import pandas as pd


class DffDashboardException(Exception):
    pass


class DffApiException(Exception):
    pass


def requires_columns(cols: List[str], exctype: type):
    def check_func(func: Callable):
        @wraps(func)
        def wrapper(*args, df: pd.DataFrame):
            df_columns = df.columns
            for col in cols:
                if col not in df_columns:
                    raise exctype(f"Required column missing: {col}")
            return func(*args, df)
        
        return wrapper

    return check_func

dashboard_requires = partial(requires_columns, exctype=DffDashboardException)

api_requires = partial(requires_columns, exctype=DffApiException)