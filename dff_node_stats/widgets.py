from lib2to3.pgen2.token import OP
from typing import Any, Callable, List, Optional, Protocol, NamedTuple
from functools import partial
import datetime

from ipywidgets import widgets
import plotly.graph_objects as go
import streamlit as st
import pandas as pd

from dff_node_stats import visualizers as vs

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
    default: Any = None


default_filters = [
    # FilterType(
    #     "Choose start date",
    #     "start_time",
    #     lambda x, y: x >= y,
    #     datetime.datetime.now() - datetime.timedelta(days=1),
    # ),
    # FilterType(
    #     "Choose end date",
    #     "start_time",
    #     lambda x, y: x <= y,
    #     datetime.datetime.now() + datetime.timedelta(days=1),
    # ),
    FilterType("Choose context_id", "context_id", lambda x, y: x == y, None),
]


class AbstractDasboard(Protocol):
    @property
    def plots(self):
        raise NotImplementedError

    @property
    def controls(self):
        raise NotImplementedError

    def __call__(self):
        raise NotImplementedError


class WidgetDashboard(widgets.Tab):
    def __init__(self,
        df: pd.DataFrame,
        plots: Optional[List[vs.VisualizerType]]=None,
        filters: Optional[List[FilterType]]=None
    ) -> None:
        super().__init__()
        self._filters: List[FilterType] = (
            default_filters if filters is None else default_filters + filters
        )
        self._plots: List[vs.VisualizerType] = (
            default_plots if plots is None else default_plots + filters
        )
        self._df_cache = df
        self._df = df

    def _slice(self):
        masks = []
        for _filter, dropdown in zip(self._filters, self.controls):
            val = dropdown.value
            if val == _filter.default:
                masks += [pd.Series(([True] * self._df.shape[0]), copy=False)]
            else:            
                func_to_apply = partial(_filter.comparison_func, y=val)
                masks += [self._df[_filter.colname].apply(func_to_apply)]
        mask = masks[0]
        for m in masks[1:]:
            mask = mask & m
        if mask.sum() == 0:
            return
        self._df = self._df.loc[mask]      

    def update(self):
        def handleChange(change):
            self._slice()
            self.__call__()

        return handleChange

    @property
    def controls(self):
        box = widgets.VBox()
        filters = []
        for _filter in self._filters:
            options = [(_filter.default, _filter.default)] + [
                (i, i) for i in self._df[_filter.colname].unique()
            ]
            dropdown = widgets.Dropdown(
                value=None, options=options, description=_filter.colname
            )
            dropdown.observe(self.update(), "value")
            filters += [dropdown]
        box.children = filters
        return box

    @property
    def plots(self):
        box = widgets.VBox()
        plot_list = []
        df = self._df.copy()
        for plot_func in self._plots:
            plot = plot_func(df)
            plot_list += [go.FigureWidget(data=plot)]
        box.children = plot_list
        return box

    def __call__(self):
        self.children = [
            # self.controls,
            self.plots
        ]
        self.titles = (
            # "Filters", 
            "Plots"
        )
        return self


class StreamlitDashboard(AbstractDasboard):
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
        self._df_cache: pd.DataFrame = df
        self._df: pd.DataFrame = self._slice(self._df_cache, *self.controls)

    @st.cache(allow_output_mutation=True)
    def _slice(self, df_origin: pd.DataFrame, *args):
        masks = []
        for _filter, dropdown in zip(self._filters, args):
            val = dropdown
            if val == _filter.default:
                masks += [pd.Series(([True] * df_origin.shape[0]), copy=False)]
            else:
                func_to_apply = partial(_filter.comparison_func, y=val)
                masks += [df_origin[_filter.colname].apply(func_to_apply)]
        mask = masks[0]
        for m in masks[1:]:
            mask = mask & m
        if mask.sum() == 0:
            return df_origin
        return df_origin.loc[mask]

    @property
    def controls(self):
        return tuple(
            [
                st.sidebar.selectbox(
                    _filter.label,
                    options=(
                        [_filter.default]
                        + self._df_cache[_filter.colname].unique().tolist()
                    ),
                )
                for _filter in self._filters
            ]
        )

    @property
    def plots(self):
        df = self._df.copy()
        for plot_func in self._plots:
            plot = plot_func(df)
            st.plotly_chart(plot, use_container_width=True)

    def __call__(self):
        st.title("DialogFlow Framework Statistic Dashboard")
        self.plots
