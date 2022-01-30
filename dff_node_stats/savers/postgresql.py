from typing import List, Optional, Union, Dict

import pandas as pd
from sqlalchemy import create_engine, inspect
from sqlalchemy.schema import MetaData, Table


class PostgresSaver:
    """Class to save stats to Postgres database"""

    def __init__(self, path: str, table: str = "dff_stats") -> None:
        self.path: str = path
        self.schema: str = self.path[self.path.rfind("/") + 1 :]
        self.table = table
        self.engine = create_engine(self.path)
        self.engine.dialect._psycopg2_extensions().register_adapter(dict, self.engine.dialect._psycopg2_extras().Json)

    def save(
        self,
        dfs: List[pd.DataFrame],
        column_types: Optional[Dict[str, str]] = None,
        parse_dates: Union[List[str], bool] = False,
    ) -> None:

        # recreate table if the schema was altered
        df = pd.concat(dfs)
        if inspect(self.engine).has_table(self.table):
            metadata = MetaData()
            ExistingModel = Table(self.table, metadata, autoload_with=self.engine)
            # if current schema contains new columns, drop the table to recreate it later
            existing_columns = set(ExistingModel.columns)
            if not len(column_types.keys() & existing_columns) == len(existing_columns):
                dates_to_parse = list(set(parse_dates) & existing_columns)
                existing_df = self.load(parse_dates=dates_to_parse)
                df = pd.concat([existing_df, df], axis=0)
                ExistingModel.drop(bind=self.engine)

        df.to_sql(name=self.table, con=self.engine, if_exists="append")

    def load(
        self,
        column_types: Optional[Dict[str, str]] = None,
        parse_dates: Union[List[str], bool] = False,
    ) -> pd.DataFrame:

        df = pd.read_sql_table(table_name=self.table, con=self.engine, parse_dates=parse_dates)

        return df
