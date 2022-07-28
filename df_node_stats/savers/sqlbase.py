""""""
from typing import List, Optional, Union, Dict

import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.schema import MetaData, Table


class SqlSaver:
    def __init__(self, path: str, table: str = "dff_stats") -> None:
        self.path: str = path
        self.schema: str = self.path.rpartition("/")[2]
        self.table = table
        self.engine = create_engine(self.path)

    def save(
        self,
        dfs: List[pd.DataFrame],
        column_types: Optional[Dict[str, str]] = None,
        parse_dates: Union[List[str], bool] = False,
    ) -> None:

        df = pd.concat(dfs)

        if not inspect(self.engine).has_table(self.table):
            df.to_sql(name=self.table, index=False, con=self.engine, if_exists="append")
            return

        metadata = MetaData()
        ExistingModel = Table(self.table, metadata, autoload_with=self.engine)
        existing_columns = set(ExistingModel.columns)

        if bool(column_types.keys() ^ existing_columns):  # recreate table if the schema was altered
            dates_to_parse = list(set(parse_dates) & existing_columns)  # make sure we do not parse non-existent cols
            existing_df = self.load(parse_dates=dates_to_parse)

            shallow_df, wider_df = sorted([df, existing_df], key=lambda x: len(x.columns))
            df = wider_df.append(shallow_df, ignore_index=True)

        df.to_sql(name=self.table, index=False, con=self.engine, if_exists="replace")

    def load(
        self,
        column_types: Optional[Dict[str, str]] = None,
        parse_dates: Union[List[str], bool] = False,
    ) -> pd.DataFrame:

        df = pd.read_sql_table(table_name=self.table, con=self.engine, parse_dates=parse_dates)

        return df
