"""
Clickhouse
---------------------------
Provides the Clickhouse version of the :py:class:`~dff_node_stats.savers.saver.Saver`. 
You don't need to interact with this class manually, as it will be automatically 
imported and initialized when you construct :py:class:`~dff_node_stats.savers.saver.Saver` with specific parameters.

"""
import json
from typing import List
from urllib import parse

from pydantic import validator
from httpx import AsyncClient
from aiochclient import ChClient

from ..utils import StatsItem


class CHItem(StatsItem):
    data: str

    @validator("data", pre=True)
    def val_data(cls, data):
        if not isinstance(data, str):
            return json.dumps(data)
        return data


class ClickHouseSaver:
    """
    Saves and reads the stats dataframe from a csv file.
    You don't need to interact with this class manually, as it will be automatically
    initialized when you construct :py:class:`~dff_node_stats.savers.saver.Saver` with specific parameters.

    Parameters
    ----------

    path: str
        | The construction path.
        | It should match the sqlalchemy :py:class:`~sqlalchemy.engine.Engine` initialization string.

        .. code-block::

            ClickHouseSaver("clickhouse://user:password@localhost:8000/default")

    table: str
        Sets the name of the db table to use. Defaults to "dff_stats".
    """

    def __init__(self, path: str, table: str = "df_stats") -> None:
        self.table = table
        parsed_path = parse.urlparse(path)
        auth, _, address = parsed_path.netloc.partition("@")
        self.db = parsed_path.path.strip("/")
        self.url = parse.urlunparse(("http", address, "/", "", "", ""))
        self._user, _, self._password = auth.partition(":")
        self._http_client = AsyncClient()
        self._table_exists = False
        if not all([self.db, self.url, self._user, self._password]):
            raise ValueError("Invalid database URI or credentials")
        self.ch_client = ChClient(
            self._http_client, url=self.url, user=self._user, password=self._password, database=self.db
        )

    async def save(self, data: List[StatsItem]) -> None:
        if not self._table_exists:
            await self._create_table()
            self._table_exists = True
        await self.ch_client.execute(
            f"INSERT INTO {self.table} VALUES", *[tuple(CHItem.parse_obj(item).dict().values()) for item in data]
        )

    async def load(self) -> List[StatsItem]:
        results = []
        async for row in self.ch_client.iterate(f"SELECT * FROM {self.table}"):
            results.append(StatsItem.parse_obj({key: row[key] for key in row.keys()}))
        return results

    async def _create_table(self):
        await self.ch_client.execute(
            f"CREATE TABLE if not exists {self.table} ("
            "context_id String, "
            "request_id Int32, "
            "time DateTime64, "
            "data_key String, "
            "data String"
            ") ENGINE = Memory"
        )
