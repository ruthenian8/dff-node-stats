"""
Stats
**********
| Defines the Stats class that is used to collect information on each turn of the :py:class:`~df_engine.core.actor.Actor` .
| An instance of the :py:class:`~df_engine.core.actor.Actor` class should be passed to the update_actor_handlers method in order to register a callback.

Example::

    stats = Stats()

    actor = Actor()

    stats.update_actor_handlers(actor, auto_save=False)

"""
from random import randint
from typing import Any, Dict, List, Optional
import datetime
from functools import cached_property
from copy import copy

import pandas as pd
from pydantic import validate_arguments
from df_engine.core import Context, Actor
from df_engine.core.types import ActorStage

from . import collectors as DSC
from .savers import Saver


class Stats:
    """
    The class which is used to collect information from :py:class:`~df_engine.core.context.Context`
    on each turn of the :py:class:`~df_engine.core.actor.Actor`.

    Parameters
    ----------

    saver: :py:class:`~dff_node_stats.savers.Saver`
        An instance of the Saver class that is used to save the collected data in the desired storage.
    collectors: Optional[List[:py:class:`~dff_node_stats.collectors.Collector`]]
        Instances of the :py:class:`~dff_node_stats.collectors.Collector` class.
        Their method :py:meth:`~dff_node_stats.collectors.Collector.collect_stats`
        is invoked each turn of the :py:class:`~df_engine.core.actor.Actor` to save the desired information.

    """

    def __init__(
        self,
        saver: Saver,
        collectors: Optional[List[DSC.Collector]] = None,
        mock_dates: bool = False
    ) -> None:
        col_default = [
            DSC.DefaultCollector(),
            DSC.NodeLabelCollector(),
            DSC.ContextCollector(column_dtypes={"attitude": "int64"}, source_field="misc")
        ]
        collectors = col_default if collectors is None else col_default + collectors
        type_check = lambda x: isinstance(x, DSC.Collector) and not isinstance(x, type)
        if not all(map(type_check, collectors)):
            raise TypeError("Param `collectors` should be a list of collector instances")
        column_dtypes = dict()
        parse_dates = list()
        for collector in collectors:
            column_dtypes.update(collector.column_dtypes)
            parse_dates.extend(collector.parse_dates)

        self.saver: Saver = saver
        self.collectors: List[DSC.Collector] = collectors
        self.column_dtypes: Dict[str, str] = column_dtypes
        self.parse_dates: List[str] = parse_dates
        self.dfs: list = []
        self.start_time: Optional[datetime.datetime] = None
        self._mock_dates: bool = mock_dates

    def __deepcopy__(self, *args, **kwargs):
        return copy(self)

    @cached_property
    def dataframe(self) -> pd.DataFrame:
        return self.saver.load(column_types=self.column_dtypes, parse_dates=self.parse_dates)

    def add_df(self, stats: Dict[str, Any]) -> None:
        self.dfs += [pd.DataFrame(stats)]

    def save(self, *args, **kwargs):
        self.saver.save(self.dfs, column_types=self.column_dtypes, parse_dates=self.parse_dates)
        self.dfs.clear()

    def _update_handlers(self, actor: Actor, stage: ActorStage, handler) -> Actor:
        actor.handlers[stage] = actor.handlers.get(stage, []) + [handler]

    def update_actor_handlers(self, actor: Actor, auto_save: bool = True, *args, **kwargs):
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
            stats.update(collector.collect_stats(ctx, actor, start_time=self.start_time))
        if self._mock_dates:
            date_offset: int = hash(str(stats["context_id"])) % 30 # yields a 'random' number from 0 to 99 equal for same contexts
            hour_offset: int = hash(str(stats["context_id"])) % 24
            minute_offset: int = hash(str(stats["context_id"])) % 60
            stats["start_time"] = [
                stats["start_time"][0] - datetime.timedelta(days = date_offset, hours=hour_offset, minutes=minute_offset)
            ] # mock different dates
            stats["duration_time"] = [stats["duration_time"][0] + randint(4,12)]
        self.add_df(stats=stats)
