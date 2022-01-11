from typing import Any, Callable, List, Protocol, NamedTuple
from functools import partial, cached_property
import datetime

from ipywidgets import widgets, Box, Label
import plotly.graph_objects as go
import streamlit
import pandas as pd


class FilterType(NamedTuple):
    label: str
    colname: str
    comparison_func: Callable[[Any, Any], bool]
    default: Any = None


default_filters = [
    FilterType("Choose start date", "start_time", lambda x, y: x >= y, datetime.datetime.now() - datetime.timedelta(days=1)),
    FilterType("Choose end date", "start_time", lambda x, y: x <= y, datetime.datetime.now() + datetime.timedelta(days=1)),
    FilterType("Choose context_id", "context_id", lambda x, y: x == y, None)
]


class AbstractDasboard(Protocol):
    @cached_property
    def plots(self):
        raise NotImplementedError

    @property
    def controls(self):
        raise NotImplementedError

    def __call__(self):
        raise NotImplementedError


class WidgetDashboard(widgets.Tab):
    def __init__(self,
        df,
        plots,
        filters=None
    ) -> None:
        super().__init__()
        self._filters: List[FilterType] = default_filters if filters is None else default_filters.extend(filters)
        self._plots = plots
        self._df_cache = df
        self._df = df

    def _slice(self):
        for _filter, dropdown in zip(self._filters, self.controls):
            masks = []
            if dropdown.value != _filter.default:
                val = dropdown.value
                func_to_apply = partial(_filter.comparison_func, y=val)
                masks += [self._df[_filter.colname].apply(func_to_apply)]
            mask = masks[0]
            for m in masks[1:]:
                mask = mask & m
            if mask.sum() == 0:
                return
            self._df = self._df_cache.loc[mask]

    def update(self, comparison_func: Callable):
        def handleChange(change):
            colname = change.owner.description
            value = change.new
            if value is None:
                return
            func_to_apply = partial(comparison_func, y=value)
            mask = self._df[colname].apply(func_to_apply)
            if mask.sum() != 0:
                self._df = self._df.loc[mask]
                self.__call__()
        return handleChange

    @property
    def controls(self):
        box = widgets.VBox()
        for _filter in self._filters:
            options = (
                [(_filter.default, _filter.default)] 
                + [(i, i) for i in self._df[_filter.colname].unique()]
            )
            dropdown = widgets.Dropdown(
                value=None,
                options=options,
                description=_filter.colname
            )
            dropdown.observe(self.update(_filter.comparison_func), 'value')
            box.children += dropdown
        return box

    @cached_property
    def plots(self):
        return widgets.VBox([go.FigureWidget(data=plot) for plot in self._plots])

    def __call__(self):
        self.children = [self.controls, self.plots]
        self.titles = ('Filters', 'Plots')
        return self


class StreamlitDashboard(AbstractDasboard):
    def __init__(self,
        df,
        plots,
        filters=None
    ) -> None:
        self._filters: List[FilterType] = default_filters if filters is None else default_filters.extend(filters)
        self._plots = plots
        self._df_cache = df
        self._df = df
    
    @property
    def controls(self):
        raise NotImplementedError

    @cached_property
    def plots(self):
        raise NotImplementedError

    def __call__(self):
        raise NotImplementedError

# def streamlit_run(dataframe) -> None:
#     """
#     Methods for visualizing data
#     will be in corresponding collectors,
#     so that we don't assume that some data is collected
#     by default
#     """

#     df = dataframe.copy()
#     streamlit.title("DialogFlow Framework Statistic Dashboard")
#     for collector in self.collectors:
#         streamlit.plotly_chart(fig, use_container_width=True)
#     return

# @dashboard_requires(["context_id", "start_time"])
# def show_dates(df: pd.DataFrame) -> pd.DataFrame:
#     """
#     """
    
#     @streamlit.cache()
#     def get_datatimes():
#         start_time = pd.to_datetime(df.start_time.min()) - datetime.timedelta(
#             days=1
#         )
#         end_time = pd.to_datetime(df.start_time.max()) + datetime.timedelta(days=1)
#         return start_time, end_time

#     start_time_border, end_time_border = get_datatimes()

#     def get_sidebar_chnges():
#         start_date = pd.to_datetime(
#             streamlit.sidebar.date_input("Start date", start_time_border)
#         )
#         end_date = pd.to_datetime(
#             streamlit.sidebar.date_input("End date", end_time_border)
#         )
#         if start_date < end_date:
#             streamlit.sidebar.success(
#                 "Start date: `%s`\n\nEnd date:`%s`" % (start_date, end_date)
#             )
#         else:
#             streamlit.sidebar.error("Error: End date must fall after start date.")

#         context_id = streamlit.sidebar.selectbox(
#             "Choose context_id",
#             options=["all"] + df.context_id.unique().tolist(),
#         )
#         return start_date, end_date, context_id

#     start_date, end_date, context_id = get_sidebar_chnges()

#     @streamlit.cache(allow_output_mutation=True)
#     def slice_df_origin(df_origin, start_date, end_date, context_id):
#         return df_origin[
#             (df_origin.start_time >= start_date)
#             & (df_origin.start_time <= end_date)
#             & ((df_origin.context_id == context_id) | (context_id == "all"))
#         ]

#     df = slice_df_origin(df, start_date, end_date, context_id)

#     col1, col2 = streamlit.columns(2)
#     col1.subheader("Data")
#     col1.dataframe(df)
#     col2.subheader("Timings")
#     col2.dataframe(df.describe().duration_time)
#     col2.write(f"Data shape {df.shape}")
#     return df