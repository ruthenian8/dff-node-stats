# %%
from typing import Dict, List, Optional
import pathlib
import datetime
from functools import cached_property

import pandas as pd
from pydantic import validate_arguments, BaseModel, Field
from dff.core import Context, Actor
from dff.core.types import ActorStage

from collectors import *
from savers import Saver, CsvSaver


class Stats(BaseModel):
    saver: Saver
    collectors: List[Collector]
    column_dtypes: Dict[str, str] = {}
    parse_dates: List[str] = []
    dfs: list = Field(default_factory=list)
    start_time: Optional[datetime.datetime] = None

    def __init__(
        self,
        saver: Saver,
        collectors: List[Collector],
    ) -> None:
        column_dtypes = dict()
        parse_dates = list()
        for collector in collectors:
            column_dtypes.update(collector.column_dtypes)
            parse_dates.extend(collector.parse_dates)

        super(Stats, self).__init__(saver, collectors, column_dtypes, parse_dates)

    @cached_property
    def dataframe(self) -> pd.DataFrame:
        return self.saver.load(
            column_types=self.column_dtypes, parse_dates=self.parse_dates
        )

    def add_df(self, stats: Dict[str, Any]) -> None:
        self.dfs += [pd.DataFrame(stats)]

    def save(self, *args, **kwargs):
        self.saver.save(
            self.dfs, column_types=self.column_dtypes, parse_dates=self.parse_dates
        )
        self.dfs.clear()

    @validate_arguments
    def _update_handlers(self, actor: Actor, stage: ActorStage, handler) -> Actor:
        actor.handlers[stage] = actor.handlers.get(stage, []) + [handler]
        return actor

    def update_actor_handlers(
        self, actor: Actor, auto_save: bool = True, *args, **kwargs
    ):
        self._update_handlers(actor, ActorStage.CONTEXT_INIT, self.get_start_time)
        self._update_handlers(actor, ActorStage.FINISH_TURN, self.collect_stats)
        if auto_save:
            self._update_handlers(actor, ActorStage.FINISH_TURN, self.save)

    @validate_arguments
    def get_start_time(self, ctx: Context, actor: Actor, *args, **kwargs) -> None:
        self.start_time = datetime.datetime.now()
        self.collect_stats(ctx, actor, *args, **kwargs)

    @validate_arguments
    def collect_stats(self, ctx: Context, actor: Actor, *args, **kwargs) -> None:
        stats = dict()
        for collector in self.collectors:
            stats.update(
                collector.collect_stats(ctx, actor, start_time=self.start_time)
            )
        self.add_df(stats=stats)

    def streamlit_run(self) -> None:
        """
        Methods for visualizing data
        will be in corresponding collectors,
        so that we don't assume that some data is collected
        by default
        """
        import streamlit as st

        df = self.dataframe.copy()
        st.title("DialogFlow Framework Statistic Dashboard")
        for collector in self.collectors:
            df = collector.streamlit_run(st, df)
        return

    def api_run(self, port=8000) -> None:
        """
        Methods for API get defined
        inside the collectors as well
        """
        import uvicorn
        from fastapi import FastAPI

        app = FastAPI()
        df = self.dataframe
        for collector in self.collectors:
            app = collector.api_run(app, df)
        uvicorn.run(app, host="0.0.0.0", port=port)


class StatsBuilder:
    def __init__(self) -> None:
        self.collector_mapping: Dict[str, Collector] = {
            "NodeLabelCollector": NodeLabelCollector(),
            "RequestCollector": RequestCollector(),
            "ResponseCollector": ResponseCollector(),
        }

    @validate_arguments
    def __call__(
        self, saver: Optional[Saver] = None, collectors: Optional[List[str]] = None
    ) -> Stats:
        if saver is None:
            saver = CsvSaver(csv_file=pathlib.Path("./stats.csv"))
        if collectors is None:
            collectors = ["NodeLabelCollector"]
        return Stats(
            saver,
            [
                DefaultCollector(),
                *[
                    self.collector_mapping[i]
                    for i in collectors
                    if i in self.collector_mapping
                ],
            ],
        )

    def register(self, collector: Collector) -> None:
        if not issubclass(collector.__class__, Collector):
            raise TypeError("The class should implement the Collector protocol")
        self.collector_mapping[collector.__class__.__name__] = collector
