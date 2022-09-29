"""
StatsStorage
**********
| Defines the StatsStorage class that is used to collect information on each turn of the :py:class:`~df_engine.core.actor.Actor` .

"""

import asyncio
from typing import List

from .savers import Saver
from .pool import ExtractorPool


class StatsStorage:
    """
    This class serves as an intermediate collection of data records that stores 
    batches of data and persists them to the database. The batch size is individual 
    for each instance.

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

    def add_extractor_pool(self, pool: ExtractorPool):
        pool.subscribers.append(self)

    @classmethod
    def from_uri(cls, uri: str, table: str = "df_stats"):
        """
        Instantiates the saver from the given arguments.
        """
        return cls(saver=Saver(uri, table))
