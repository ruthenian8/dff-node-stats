from typing import List, Dict, Protocol, runtime_checkable, Any
from types import ModuleType
import datetime

from pydantic import BaseModel, validate_arguments
from dff.core import Context, Actor


@runtime_checkable
class Collector(Protocol):
    @property
    def column_dtypes(self) -> Dict[str, str]:
        return None

    @property
    def parse_dates(self) -> List[str]:
        return []    

    def collect_stats(self, ctx: Context, actor: Actor, *args, **kwargs) -> None:
        raise NotImplementedError

    def visualize(streamlit: ModuleType) -> None:
        raise NotImplementedError


class BasicCollector(BaseModel):
    @property
    def column_dtypes(self) -> Dict[str, str]:
        return {
            "context_id": "string",
            "flow_label": "string",
            "node_label": "string",
            "history_id": "int64",
            "start_time": "datetime64[ns]",
            "duration_time": "float64",
        }

    @property
    def parse_dates(self) -> List[str]:
        return ["start_time"]

    @validate_arguments
    def collect_stats(
        self,
        ctx: Context,
        actor: Actor,
        *args,
        **kwargs
    ) -> Dict[str, Any]:
        indexes = list(ctx.labels)
        current_index = indexes[-1] if indexes else -1
        start_time = kwargs.get("start_time", datetime.datetime.now())
        return {
            "context_id": [str(ctx.id)],
            "history_id": [current_index],
            "start_time": [start_time],
            "duration_time": [(datetime.datetime.now() - start_time).total_seconds()],
            "flow_label": [ctx.last_label[0]],
            "node_label": [ctx.last_label[1]],
        }

    def visualize(streamlit: ModuleType) -> None:
        """TODO: implement"""
        return


class RequestCollector(BaseModel):
    @property
    def column_dtypes(self) -> Dict[str, str]:
        return {"user_request": "string"}

    @property
    def parse_dates(self) -> List[str]:
        return []

    @validate_arguments
    def collect_stats(
        self,
        ctx: Context,
        actor: Actor,
        *args,
        **kwargs
    ) -> Dict[str, Any]:
        return {"user_request": ctx.last_request or ""}

    def visualize(streamlit: ModuleType) -> None:
        """TODO: implement"""
        return


class ResponseCollector(BaseModel):
    @property
    def column_dtypes(self) -> Dict[str, str]:
        return {"bot_response": "string"}

    @property
    def parse_dates(self) -> List[str]:
        return []

    @validate_arguments
    def collect_stats(
        self,
        ctx: Context,
        actor: Actor,
        *args,
        **kwargs
    ) -> Dict[str, Any]:
        return {ctx.last_response or ""}

    def visualize(streamlit: ModuleType) -> None:
        """TODO: implement"""
        return


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
        self, 
        column_dtypes: Dict[str, str],
        parse_dates: List[str] = []
    ) -> None:
        """
        :param column_dtypes: names and pd types of columns
        :param parse_dates: names of columns with datetime
        """
        super(ContextCollector, self).__init__(
            column_dtypes, parse_dates
        )

    @property
    def column_dtypes(self) -> Dict[str, str]:
        return self.column_dtypes

    @property
    def parse_dates(self) -> List[str]:
        return self.parse_dates

    @validate_arguments
    def collect_stats(
        self,
        ctx: Context,
        actor: Actor,
        *args,
        **kwargs
    ) -> Dict[str, Any]:
        misc_stats = dict()
        for key in self.column_dtypes:
            value = ctx.misc.get(key, None)
            misc_stats[key] = value
        return misc_stats

    def visualize(streamlit: ModuleType) -> None:
        """TODO: implement"""
        return