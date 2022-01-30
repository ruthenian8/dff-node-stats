from functools import partial, wraps
from typing import List, Callable

import pandas as pd


TransformType = Callable[[pd.DataFrame], pd.DataFrame]


class DffStatsException(Exception):
    pass


def transform_once(func: TransformType):
    """
    Caches the transformations results by columns
    """

    @wraps(func)
    def wrapper(dataframe: pd.DataFrame):
        cols_as_string = ".".join(dataframe.columns)
        if cols_as_string != wrapper.columns:
            new_df = func(dataframe)
            wrapper.columns = ".".join(new_df.columns)
            return new_df
        return dataframe

    wrapper.columns = ""
    return wrapper


def check_transform(transform: TransformType, exctype: type):
    """
    Applies a specified transform operation to the dataset
    before the decorated function is executed
    """

    def check_func(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if len(args) == 0 and len(kwargs) == 0:
                raise exctype(f"No dataframe found.")
            df = kwargs.get("df") or args[0]
            if not isinstance(df, pd.DataFrame):
                raise exctype(f"No dataframe found.")
            df = transform(df)
            return func(df)

        return wrapper

    return check_func


def check_columns(cols: List[str], exctype: type):
    """
    Raises an error, if the columns needed for a transformation
    or for a plotting operation are missing.
    """

    def check_func(func: Callable):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if len(args) == 0 and len(kwargs) == 0:
                raise exctype(f"No dataframe found.")
            df = kwargs.get("df") or args[0]
            if not isinstance(df, pd.DataFrame):
                raise exctype(f"No dataframe found.")
            missing = [col for col in cols if col not in df.columns]
            if len(missing) > 0:
                raise exctype(
                    """
                    Required columns missing: {}. 
                    Did you collect them?
                    """.format(
                        ", ".join(missing)
                    )
                )
            return func(*args, **kwargs)

        return wrapper

    return check_func


requires_transform = partial(check_transform, exctype=DffStatsException)

requires_columns = partial(check_columns, exctype=DffStatsException)
