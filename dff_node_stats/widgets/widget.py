from typing import Any, Callable, List, Optional, NamedTuple

import pandas as pd

from . import visualizers as vs

default_plots = [
    vs.show_table,
    vs.show_duration_time,
    vs.show_transition_graph,
    vs.show_transition_trace,
    vs.show_node_counters,
    vs.show_transition_counters,
    vs.show_transition_duration,
]


class FilterType(NamedTuple):
    label: str
    colname: str
    comparison_func: Callable[[Any, Any], bool]
    default: str = "None"


default_filters = [
    FilterType("Choose context_id", "context_id", lambda x, y: x == y, "None"),
]


class AbstractDasboard():
    def __init__(self,
        df: pd.DataFrame,
        plots: Optional[List[vs.VisualizerType]]=None,
        filters: Optional[List[FilterType]]=None
    ) -> None:
        self._filters: List[FilterType] = (
            default_filters if filters is None else default_filters + filters
        )
        self._plots: List[vs.VisualizerType] = (
            default_plots if plots is None else default_plots + plots
        )
        self._df_cache = df # original df used to construct the widget
        self._df = df # current state

    def plots(self):
        raise NotImplementedError

    @property
    def controls(self):
        raise NotImplementedError

    def __call__(self):
        raise NotImplementedError
