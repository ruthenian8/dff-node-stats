"""
Stats
**********
| Defines the Stats class that is used to collect information on each turn of the :py:class:`~df_engine.core.actor.Actor` .

"""

import asyncio
import functools
from typing import List, Callable

from df_engine.core import Context, Actor
from df_runner import WrapperRuntimeInfo
from pydantic.typing import ForwardRef

from .savers import Saver
from .item import StatsItem


Stats = ForwardRef("Stats")

StatsFunction = Callable[[Stats, Context, Actor, WrapperRuntimeInfo], None]


class Stats:
    """
    This class is used to collect information from :py:class:`~df_engine.core.context.Context`
    on each dialogue turn. Define a wrapper function and pass it to the `get_wrapper` method.
    Each time the function fires, its output will be saved to a batch. When the batch size limit
    is reached, the data will be uploaded to the database.

    Parameters
    ----------

    saver: :py:class:`~dff_node_stats.savers.Saver`
        An instance of the Saver class that is used to save the collected data.

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

    def get_wrapper(self, func: StatsFunction) -> None:
        @functools.wraps(func)
        async def wrapper_func(ctx, _, info):
            result = await func(ctx, _, info)
            if result is not None:
                self.data.append(result)
                await self.save()

        return wrapper_func

    def get_default_actor_wrapper(self):
        async def get_default_actor_data(ctx, _, info):
            last_label = ctx.last_label or ("", "")
            default_data = StatsItem.from_context(
                ctx, info, {"flow": last_label[0], "node": last_label[1], "label": ": ".join(last_label)}
            )
            return default_data

        return self.get_wrapper(get_default_actor_data)

    @classmethod
    def from_uri(cls, uri: str, table: str = "df_stats"):
        """
        Instantiates the saver from the given arguments.
        """
        return cls(saver=Saver(uri, table))
