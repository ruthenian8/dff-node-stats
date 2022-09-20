"""
Postgresql
---------------------------
Provides the Postgresql version of the :py:class:`~dff_node_stats.savers.saver.Saver`. 
You don't need to interact with this class manually, as it will be automatically 
imported and initialized when you construct :py:class:`~dff_node_stats.savers.saver.Saver` with specific parameters.

"""
from re import M
from typing import List
from urllib import parse
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import inspect, Table, MetaData, Column, String, Integer, JSON, DateTime, select, insert

from ..utils import StatsItem
from .saver import Saver


class PostgresSaver(Saver):
    """
    Saves the stats dataframe to - and reads from a Postgresql database.
    You don't need to interact with this class manually, as it will be automatically
    initialized when you construct :py:class:`~dff_node_stats.savers.saver.Saver` with specific parameters.

    Parameters
    ----------

    path: str
        | The construction path.
        | It should match the sqlalchemy :py:class:`~sqlalchemy.engine.Engine` initialization string.

        .. code-block::

            Saver("postgresql://user:password@localhost:5432/default")

    table: str
        Sets the name of the db table to use. Defaults to "dff_stats".
    """

    def __init__(self, path: str, table: str = "df_stats") -> None:
        self.table = table
        self.table_exists = False
        parsed_path = parse.urlparse(path)
        self.engine = create_async_engine(parse.urlunparse([(parsed_path.scheme + "+asyncpg"), *parsed_path[1:]]))
        self.metadata = MetaData()
        self.sqla_table = Table(
            self.table,
            self.metadata,
            Column("context_id", String),
            Column("request_id", Integer),
            Column("time", DateTime),
            Column("data_key", String),
            Column("data", JSON),
        )

    async def save(self, data: List[StatsItem]) -> None:
        async with self.engine.connect() as conn:
            await conn.execute(insert(self.table).values([item.dict() for item in data]))
            await conn.commit()

    async def load(self) -> List[StatsItem]:
        stats = []

        async with self.engine.connect() as conn:
            result = await conn.execute(select(self.sqla_table))

        async for item in result.all():
            stats.append(StatsItem.from_orm(item))

        return stats

    async def _create_table(self):
        def table_exists(conn):
            return inspect(conn).has_table(self.table)

        async with self.engine.connect() as conn:
            exist_result = await conn.run_sync(table_exists)
            if exist_result:
                return

            await conn.run_sync(self.metadata.create_all)
