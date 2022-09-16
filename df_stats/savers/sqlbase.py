""""""
from typing import List, Optional, Union, Dict
from abc import abstractmethod


from sqlalchemy import create_engine, inspect
from sqlalchemy.schema import MetaData, Table
from ..stats import StatsData

class SqlSaver:
    def __init__(self, path: str, table: str = "dff_stats") -> None:
        self.path: str = path
        self.schema: str = self.path.rpartition("/")[2]
        self.table = table
        self.engine = create_engine(self.path)

    def save(
        self,
        data: List[StatsData]
    ) -> None:
        self.insert_method(self.table, self.engine, data)


    def load(self) -> list[StatsData]:
        df = pd.read_sql_table(table_name=self.table, con=self.engine, parse_dates=parse_dates)
        return df
    
    @staticmethod
    @abstractmethod
    def insert_method(table, conn, keys, data_iter):
        raise NotImplementedError
