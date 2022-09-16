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
import json
import asyncio
import datetime
from typing import List, Optional, TypedDict

from df_runner import Wrapper, WrapperRuntimeInfo
from df_engine.core.context import Context, get_last_index

from .savers import Saver

STATS_KEY = "STATS"


StatsData = TypedDict(
    "StatsData",
    {
        "context_id": str,
        "request_id": str,
        "time": str,
        "data_key": str,
        "data": str
    }
)


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
    def __init__(self, saver: Saver, batch_size: int) -> None:
        self.saver: Saver = saver
        self.batch_size: int = batch_size
        self.data_dicts: List[StatsData] = []
        self.start_time: Optional[datetime.datetime] = None

    async def save(self):
        if len(self.data_dicts) == self.batch_size:
            await self.flush()
        return
    
    async def flush(self):
        async with asyncio.Lock():
            await self.saver.save(self.data_dicts)
        self.data_dicts.clear()

    def collect_stats(self, data_attr: str, data_keys: Optional[List[str]] = None) -> None:
        """
        data_attr: an attribute like `misc`.
        data_keys: keys of the attribute to recursively follow.
        """

        async def get_timestamp(ctx: Context, _, info: WrapperRuntimeInfo):
            if STATS_KEY not in ctx.framework_states:
                ctx.framework_states[STATS_KEY] = {}
            ctx.framework_states[STATS_KEY][info["component"]["name"]] = str(datetime.datetime.now())

        async def collect(ctx: Context, _, info: WrapperRuntimeInfo):
            attr = getattr(ctx, data_attr)
            data = attr
            if data_keys:
                for key in data_keys:
                    data = data[key]
            cast_data = data if isinstance(data, str) else json.dumps(data)
            
            self.data_dicts.append(dict(
                context_id=str(ctx.id),
                request_id=get_last_index(ctx.requests),
                time=ctx.framework_states[STATS_KEY][info["component"]["name"]],
                data_key=info["component"]["name"],
                data=cast_data
            ))

            await self.save()

        return Wrapper(
            name="stats_wrapper",
            before=get_timestamp,
            after=collect
        )


