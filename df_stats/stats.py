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

import asyncio
from functools import partial
from typing import List, Tuple, Callable, Union

from df_engine.core import Context, Actor
from df_runner import Wrapper, WrapperRuntimeInfo
from pydantic.typing import ForwardRef

from .savers import Saver
from .utils import StatsItem, WRAPPER_NAME


Stats = ForwardRef("Stats")

StatsFunction = Callable[[Stats, Context, Actor, WrapperRuntimeInfo], None]


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
        self.data: List[dict] = []

    async def save(self):
        if len(self.data) >= self.batch_size:
            await self.flush()
        return

    async def flush(self):
        async with asyncio.Lock():
            await self.saver.save(self.data)
        self.data.clear()

    def get_wrapper(self, funcs: Union[StatsFunction, Tuple[StatsFunction, StatsFunction]]) -> None:
        """
        data_attr: an attribute like `misc`.
        data_keys: keys of the attribute to recursively follow.
        """
        if asyncio.iscoroutinefunction(funcs):
            after = partial(funcs, self)
            return Wrapper(name=WRAPPER_NAME, before=None, after=after)
        else:
            before = partial(funcs[0], self)
            after = partial(funcs[1], self)
            return Wrapper(name=WRAPPER_NAME, before=before, after=after)
