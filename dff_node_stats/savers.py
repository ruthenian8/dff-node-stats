"""Savers for stats"""
from typing import Dict, List, Union, Optional, Protocol, runtime_checkable
import pathlib
import pandas as pd


@runtime_checkable
class Saver(Protocol):
    def save(self, dfs: List[pd.DataFrame], **kwargs):
        """
        Save the data to a database or a file
        Append if already exists
        """
        raise NotImplementedError

    def load(self, **kwargs):
        """
        Load the data from a database or a file
        """
        raise NotImplementedError


class CsvSaver:
    def __init__(self, csv_file: pathlib.Path) -> None:
        self.csv_file = csv_file

    def save(self, dfs: List[pd.DataFrame], **kwargs) -> None:
        column_types: Optional[Dict[str, str]] = kwargs.get("column_types")
        parse_dates: Optional[List[str]] = kwargs.get("parse_dates", False)
        saved_df = (
            self.load(self.csv_file, column_types=column_types, parse_dates=parse_dates)
            if self.csv_file.exists()
            else pd.DataFrame()
        )
        pd.concat([saved_df] + dfs).to_csv(self.csv_file, index=False)

    def load(self, **kwargs) -> pd.DataFrame:
        column_types: Optional[Dict[str, str]] = kwargs.get("column_types")
        parse_dates: Optional[List[str]] = kwargs.get("parse_dates", False)
        return pd.read_csv(self.csv_file, dtype=column_types, parse_dates=parse_dates)


class PGSaver:
    def __init__(self, table: str, engine_params: dict) -> None:
        """
        :param table: table name
        :param engine_params: dict of format {user, password, host, port, db}
        """
        import sqlalchemy as sa
        self.json_type = sa.dialects.postgresql.JSONB
        self.table = table
        self.engine = sa.create_engine(
            "postgresql://{user}:{password}@{host}:{port}/{db}".format(**engine_params)
        )

    def save(self, dfs: List[pd.DataFrame], **kwargs) -> None:
        column_types: Optional[Dict[str, str]] = kwargs.get("column_types")
        for key in column_types:
            if column_types[key] == "object":
                column_types[key] = self.json_type
        pd.concat(dfs).to_sql(
            name=self.table,
            con=self.engine,
            dtype=column_types,
            if_exists="append"
        )

    def load(self, **kwargs) -> pd.DataFrame:
        parse_dates: Union[List[str], bool] = kwargs.get("parse_dates", False)
        return pd.read_sql(self.table, self.engine, parse_dates=parse_dates)


class CHSaver:
    def __init__(self, table: str, engine_params: dict) -> None:
        """
        :param table: table name
        :param engine_params: dict of format {user, password, host, port, db}
        """
        import sqlalchemy as sa

        self.engine = sa.create_engine(
            "clickhouse://{user}:{password}@{host}:{port}/{db}".format(**engine_params)
        )
        self.table = table

    def save(self, dfs: List[pd.DataFrame], **kwargs) -> None:
        column_types: Optional[Dict[str, str]] = kwargs.get("column_types")
        pd.concat(dfs).to_sql(
            name=self.table,
            con=self.engine,
            dtype=column_types,
            if_exists="append"
        )

    def load(self, **kwargs) -> pd.DataFrame:
        parse_dates: Union[List[str], bool] = kwargs.get("parse_dates", False)
        return pd.read_sql(self.table, self.engine, parse_dates=parse_dates)
