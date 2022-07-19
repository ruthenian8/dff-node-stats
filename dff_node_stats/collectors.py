"""
Collectors
**********
| This module provides the basic (:py:class:`~dff_node_stats.collectors.Collector`) class,
| as well as a set of ready collectors that can be used out of the box.
| Collectors are passed to the (:py:class:`~dff_node_stats.stats.Stats`) class on construction.
| Their method collect_stats is invoked each turn of the (:py:class:`~df_engine.core.actor.Actor`)
| to extract and save (:py:class:`~df_engine.core.context.Context`) parameters.

"""
from typing import List, Dict, Optional, Any

try:
    from typing import Protocol, runtime_checkable
except ImportError:
    from typing_extensions import Protocol, runtime_checkable
import datetime

from pydantic import validate_arguments
from df_engine.core import Context, Actor


@runtime_checkable
class Collector(Protocol):
    """
    | Base protocol class that defines the required methods for a collector object.
    | User-defined collectors do not have to inherit from this class, but implementing all methods is obligatory.

    """

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

    def collect_stats(self, ctx: Context, actor: Actor, *args, **kwargs) -> Dict[str, Any]:
        """
        Extract the required data from the context
        """
        raise NotImplementedError


class DefaultCollector(Collector):
    @property
    def column_dtypes(self) -> Dict[str, str]:
        return {
            "context_id": "str",
            "history_id": "int64",
            "start_time": "datetime64[ns]",
            "duration_time": "float64",
        }

    @property
    def parse_dates(self) -> List[str]:
        return ["start_time"]

    @validate_arguments
    def collect_stats(self, ctx: Context, actor: Actor, *args, **kwargs) -> Dict[str, Any]:
        indexes = list(ctx.labels) or [-1]
        current_index = indexes[-1]
        start_time = kwargs.get("start_time") or datetime.datetime.now()
        return {
            "context_id": [str(ctx.id)],
            "history_id": [current_index],
            "start_time": [start_time],
            "duration_time": [(datetime.datetime.now() - start_time).total_seconds()],
        }


class NodeLabelCollector(Collector):
    @property
    def column_dtypes(self) -> Dict[str, str]:
        return {"flow_label": "str", "node_label": "str", "full_label": "str"}

    @property
    def parse_dates(self) -> List[str]:
        return []

    @validate_arguments
    def collect_stats(self, ctx: Context, actor: Actor, *args, **kwargs) -> Dict[str, Any]:
        last_label = ctx.last_label or actor.start_label
        return {"flow_label": [last_label[0]], "node_label": [last_label[1]], "full_label": [":".join(last_label[:2])]}


class RequestCollector(Collector):
    @property
    def column_dtypes(self) -> Dict[str, str]:
        return {"user_request": "str"}

    @property
    def parse_dates(self) -> List[str]:
        return []

    @validate_arguments
    def collect_stats(self, ctx: Context, actor: Actor, *args, **kwargs) -> Dict[str, Any]:
        return {"user_request": [ctx.last_request or ""]}


class ResponseCollector(Collector):
    @property
    def column_dtypes(self) -> Dict[str, str]:
        return {"bot_response": "str"}

    @property
    def parse_dates(self) -> List[str]:
        return []

    @validate_arguments
    def collect_stats(self, ctx: Context, actor: Actor, *args, **kwargs) -> Dict[str, Any]:
        return {"bot_response": [ctx.last_response or ""]}


class ContextCollector(Collector):
    """
    Parameters
    ----------
    :param column_dtypes: names and pandas types of columns
    :param parse_dates: names of columns with datetime
    The user needs to provide a datatype for each
    key that must be extracted from the ctx.misc
    object. In case the value is a dict or a list,
    the required type is 'object'.
    Names of columns with type 'datetime' can be
    optionally listed in 'parse_dates'
    """

    def __init__(
        self, column_dtypes: Dict[str, str], source_field: str = "misc", parse_dates: Optional[List[str]] = None
    ) -> None:
        self._source_field = source_field
        self._column_dtypes = column_dtypes
        self._parse_dates = parse_dates or []
        return

    @property
    def column_dtypes(self) -> Dict[str, str]:
        return self._column_dtypes

    @property
    def parse_dates(self) -> List[str]:
        return self._parse_dates

    @validate_arguments
    def collect_stats(self, ctx: Context, actor: Actor, *args, **kwargs) -> Dict[str, Any]:
        ctx_stats = dict()
        for key in self.column_dtypes:
            value = ctx.__dict__.get(self._source_field, {}).get(key, None)
            ctx_stats[key] = [value]
        return ctx_stats
