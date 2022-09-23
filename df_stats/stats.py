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
# TODO: fix docs

import asyncio
from functools import partial
from typing import List, Tuple, Callable, Union

from df_engine.core import Context, Actor
from df_runner import Wrapper, WrapperRuntimeInfo
from pydantic.typing import ForwardRef

from .savers import Saver
from .utils import StatsItem


Stats = ForwardRef("Stats")

StatsFunction = Callable[[Stats, Context, Actor, WrapperRuntimeInfo], None]


# TODO: fix docs
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

    def __init__(self, saver: Saver, batch_size: int = 1) -> None:
        self.saver: Saver = saver
        self.batch_size: int = batch_size
        self.data: List[dict] = []

    async def save(self):
        if len(self.data) >= self.batch_size:
            await self.flush()

    async def flush(self):
        async with asyncio.Lock():
            await self.saver.save(self.data)
        self.data.clear()

    def get_wrapper(self, func: StatsFunction) -> None: # TODO: wrong typing
        """
        data_attr: an attribute like `misc`.
        data_keys: keys of the attribute to recursively follow.
        """
        async def wrapper_func(ctx, _, info):
            return await func(self, ctx, _, info)
        return wrapper_func
        
    def get_default_actor_wrapper(self):
        async def get_default_actor_data(stats, ctx, _, info):
            last_label = ctx.last_label or ("", "")
            default_data = StatsItem.from_context(ctx, info, {
                "flow": last_label[0],
                "node": last_label[1],
                "label": ": ".join(last_label)
            })
            stats.data.append(default_data)
            await stats.save()
        
        return self.get_wrapper(get_default_actor_data)
