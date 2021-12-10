from typing import List, Dict, Protocol, runtime_checkable, Any
from types import ModuleType
import datetime

from pydantic import BaseModel, validate_arguments
from dff.core import Context, Actor
import pandas as pd
from fastapi import FastAPI


@runtime_checkable
class Collector(Protocol):
    @property
    def column_dtypes(self) -> Dict[str, str]:
        """
        String names and string pandas types for the collected data
        """
        return None

    @property
    def parse_dates(self) -> List[str]:
        """
        String names of columns that should be parsed as dates
        """
        return []

    def collect_stats(
        self, ctx: Context, actor: Actor, *args, **kwargs
    ) -> Dict[str, Any]:
        """
        Extract the required data from the context
        """
        raise NotImplementedError

    def streamlit_run(self, streamlit: ModuleType, df: pd.DataFrame) -> pd.DataFrame:
        """
        Create a streamlit representation for the data
        collected by the colllector
        """
        raise NotImplementedError

    def api_run(self, app: FastAPI, df: pd.DataFrame) -> FastAPI:
        """
        Attach methods to api, returns unchanged object
        if no endpoints should be added
        """
        raise NotImplementedError


class DefaultCollector(BaseModel):
    @property
    def column_dtypes(self) -> Dict[str, str]:
        return {
            "context_id": "string",
            "history_id": "int64",
            "start_time": "datetime64[ns]",
            "duration_time": "float64",
        }

    @property
    def parse_dates(self) -> List[str]:
        return ["start_time"]

    @validate_arguments
    def collect_stats(
        self, ctx: Context, actor: Actor, *args, **kwargs
    ) -> Dict[str, Any]:
        indexes = list(ctx.labels) or [-1]
        current_index = indexes[-1]
        start_time = kwargs.get("start_time") or datetime.datetime.now()
        return {
            "context_id": [str(ctx.id)],
            "history_id": [current_index],
            "start_time": [start_time],
            "duration_time": [(datetime.datetime.now() - start_time).total_seconds()],
        }

    def streamlit_run(self, streamlit: ModuleType, df: pd.DataFrame) -> pd.DataFrame:
        """TODO: implement"""

        @streamlit.cache()
        def get_datatimes():
            start_time = pd.to_datetime(df.start_time.min()) - datetime.timedelta(
                days=1
            )
            end_time = pd.to_datetime(df.start_time.max()) + datetime.timedelta(days=1)
            return start_time, end_time

        start_time_border, end_time_border = get_datatimes()

        def get_sidebar_chnges():
            start_date = pd.to_datetime(
                streamlit.sidebar.date_input("Start date", start_time_border)
            )
            end_date = pd.to_datetime(
                streamlit.sidebar.date_input("End date", end_time_border)
            )
            if start_date < end_date:
                streamlit.sidebar.success(
                    "Start date: `%s`\n\nEnd date:`%s`" % (start_date, end_date)
                )
            else:
                streamlit.sidebar.error("Error: End date must fall after start date.")

            context_id = streamlit.sidebar.selectbox(
                "Choose context_id",
                options=["all"] + df.context_id.unique().tolist(),
            )
            return start_date, end_date, context_id

        start_date, end_date, context_id = get_sidebar_chnges()

        @streamlit.cache()
        def slice_df_origin(df_origin, start_date, end_date, context_id):
            return df_origin[
                (df_origin.start_time >= start_date)
                & (df_origin.start_time <= end_date)
                & ((df_origin.context_id == context_id) | (context_id == "all"))
            ]

        df = slice_df_origin(df, start_date, end_date, context_id)
        return df

    def api_run(self, app: FastAPI, df: pd.DataFrame) -> FastAPI:
        return app


class NodeLabelCollector(BaseModel):
    @property
    def column_dtypes(self) -> Dict[str, str]:
        return {
            "flow_label": "string",
            "node_label": "string",
        }

    @property
    def parse_dates(self) -> List[str]:
        return []

    @validate_arguments
    def collect_stats(
        self, ctx: Context, actor: Actor, *args, **kwargs
    ) -> Dict[str, Any]:
        last_label = ctx.last_label or actor.start_label
        return {
            "flow_label": [last_label[0]],
            "node_label": [last_label[1]],
        }

    def streamlit_run(self, streamlit: ModuleType, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        def add_nodes_and_edges(df: pd.DataFrame):
            for context_id in df.context_id.unique():
                ctx_index = df.context_id == context_id
                df.loc[ctx_index, "node"] = (
                    df.loc[ctx_index, "flow_label"]
                    + ":"
                    + df.loc[ctx_index, "node_label"]
                )
                df.loc[ctx_index, "edge"] = (
                    df.loc[ctx_index, "node"]
                    .shift(periods=1)
                    .combine(df.loc[ctx_index, "node"], lambda *x: list(x))
                )
                flow_label = df.loc[ctx_index, "flow_label"]
                df.loc[ctx_index, "edge_type"] = flow_label.where(
                    flow_label.shift(periods=1) == flow_label, "MIXED"
                )
            return df

        df = add_nodes_and_edges(df)
        return df

    def api_run(self, app: FastAPI, df: pd.DataFrame) -> FastAPI:
        def transition_counts(df: pd.DataFrame) -> Dict[str, int]:
            df = df.copy()
            df["node"] = df.apply(
                lambda row: f"{row.flow_label}:{row.node_label}", axis=1
            )
            df = df.drop(["flow_label", "node_label"], axis=1)
            df = df.sort_values(["context_id"], kind="stable")
            df["next_node"] = df.node.shift()
            df = df[df.history_id != 0]
            transitions = df.apply(lambda row: f"{row.node}->{row.next_node}", axis=1)
            return {k: int(v) for k, v in dict(transitions.value_counts()).items()}

        tc = transition_counts(df)

        def transition_probs(df: pd.DataFrame) -> Dict[str, float]:
            return {k: v / sum(tc.values, 0) for k, v in tc.items()}

        @app.get("/api/v1/stats/transition-counts", response_model=Dict[str, int])
        async def get_transition_counts():
            return tc

        @app.get("/api/v1/stats/transition-probs", response_model=Dict[str, float])
        async def get_transition_probs():
            return transition_probs(df)

        return app


class RequestCollector(BaseModel):
    @property
    def column_dtypes(self) -> Dict[str, str]:
        return {"user_request": "string"}

    @property
    def parse_dates(self) -> List[str]:
        return []

    @validate_arguments
    def collect_stats(
        self, ctx: Context, actor: Actor, *args, **kwargs
    ) -> Dict[str, Any]:
        return {"user_request": [ctx.last_request or ""]}

    def streamlit_run(self, streamlit: ModuleType, df: pd.DataFrame) -> pd.DataFrame:
        """TODO: implement"""
        return df

    def api_run(self, app: FastAPI, df: pd.DataFrame) -> FastAPI:
        return app


class ResponseCollector(BaseModel):
    @property
    def column_dtypes(self) -> Dict[str, str]:
        return {"bot_response": "string"}

    @property
    def parse_dates(self) -> List[str]:
        return []

    @validate_arguments
    def collect_stats(
        self, ctx: Context, actor: Actor, *args, **kwargs
    ) -> Dict[str, Any]:
        return {"bot_response": [ctx.last_response or ""]}

    def streamlit_run(self, streamlit: ModuleType, df: pd.DataFrame) -> pd.DataFrame:
        """TODO: implement"""
        return df

    def api_run(self, app: FastAPI, df: pd.DataFrame) -> FastAPI:
        return app


class ContextCollector(BaseModel):
    """
    The user needs to provide a datatype for each
    key that must be extracted from the ctx.misc
    object. In case the value is a dict or a list,
    the required type is 'object'.
    Names of columns with type 'datetime' can be
    optionally listed in 'parse_dates'
    """

    column_dtypes: Dict[str, str]
    parse_dates: List[str]

    def __init__(
        self, column_dtypes: Dict[str, str], parse_dates: List[str] = []
    ) -> None:
        """
        :param column_dtypes: names and pd types of columns
        :param parse_dates: names of columns with datetime
        """
        super(ContextCollector, self).__init__(column_dtypes, parse_dates)

    @property
    def column_dtypes(self) -> Dict[str, str]:
        return self.column_dtypes

    @property
    def parse_dates(self) -> List[str]:
        return self.parse_dates

    @validate_arguments
    def collect_stats(
        self, ctx: Context, actor: Actor, *args, **kwargs
    ) -> Dict[str, Any]:
        misc_stats = dict()
        for key in self.column_dtypes:
            value = ctx.misc.get(key, None)
            misc_stats[key] = [value]
        return misc_stats

    def streamlit_run(self, streamlit: ModuleType, df: pd.DataFrame) -> pd.DataFrame:
        """TODO: implement"""
        return df

    def api_run(self, app: FastAPI, df: pd.DataFrame) -> FastAPI:
        return app
