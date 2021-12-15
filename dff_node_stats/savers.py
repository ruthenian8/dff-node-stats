"""Savers for stats"""
from typing import Dict, List, Union, Optional, Protocol, runtime_checkable
import pathlib
import pandas as pd
import csv

@runtime_checkable
class Saver(Protocol):
    _saver_mapping = {}

    def __init_subclass__(cls, _id: str, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        cls._saver_mapping[_id] = cls

    def __new__(cls, path: str):
        triple = path.partition("://")
        if not all(triple):
            raise ValueError(
                """Saver should be initialized with either:
                csv://path_to_file 
                or
                postgresql://sqlalchemy_engine_params
                clickhouse://sqlalchemy_engine_params
                """
            )
        _id = triple[0]
        subclass = cls._saver_mapping[_id]
        obj = object.__new__(subclass)
        obj.path = path
        return obj


    def save(self, dfs: List[pd.DataFrame], **kwargs) -> None:
        """
        Save the data to a database or a file
        Append if the table already exists
        """
        raise NotImplementedError

    def load(self, **kwargs) -> pd.DataFrame:
        """
        Load the data from a database or a file
        """
        raise NotImplementedError


class CsvSaver(Saver, _id="csv"):
    def __init__(
        self, 
        path: str, 
    ) -> None:
        if hasattr(self, "path"):
            path = self.path.partition("://")[2]
        self.path = pathlib.Path(path)

    def save(self, dfs: List[pd.DataFrame], **kwargs) -> None:
        column_types: Optional[Dict[str, str]] = kwargs.get("column_types")
        parse_dates: Optional[List[str]] = kwargs.get("parse_dates", False)
        saved_df = (
            self.load(column_types=column_types, parse_dates=parse_dates)
            if self.path.exists()
            else pd.DataFrame()
        )
        pd.concat([saved_df] + dfs).to_csv(self.path, index=False)

    def load(self, **kwargs) -> pd.DataFrame:
        column_types: Optional[Dict[str, str]] = kwargs.get("column_types")
        parse_dates: Optional[List[str]] = kwargs.get("parse_dates", False)
        return pd.read_csv(self.path, dtype=column_types, parse_dates=parse_dates)


class PGSaver(Saver, _id="postgresql"):
    def __init__(
        self, 
        path: str
    ) -> None:
        """
        :param engine_params: dict of format {user, password, host, port, db}
        """
        import sqlalchemy as sa
        if not hasattr(self, "path"):
            self.path = path
        self.table = "dff_stats"
        self.json_type = sa.dialects.postgresql.JSONB
        self.engine = sa.create_engine(
            self.path,
            echo=True,
            echo_pool='debug'            
        )

    def save(self, dfs: List[pd.DataFrame], **kwargs) -> None:
        column_types: Optional[Dict[str, str]] = kwargs.get("column_types")
        for key in column_types:
            if column_types[key] == "object":
                column_types[key] = self.json_type
        pd.concat(dfs).to_sql(
            name=self.table, con=self.engine, dtype=column_types, if_exists="append"
        )

    def load(self, **kwargs) -> pd.DataFrame:
        parse_dates: Union[List[str], bool] = kwargs.get("parse_dates", False)
        return pd.read_sql(self.table, self.engine, parse_dates=parse_dates)


class CHSaver(Saver, _id="clickhouse"):
    def __init__(
        self, 
        path: str
    ) -> None:
        """
        :param table: table name
        :param engine_params: dict of format {user, password, host, port, db}
        """
        import sqlalchemy as sa
        if not hasattr(self, "path"):
            self.path = path
        self.table = "dff_stats"
        self.engine = sa.create_engine(
            self.path,
            echo=True,
            echo_pool='debug'
        )

    def save(self, dfs: List[pd.DataFrame], **kwargs) -> None:
        column_types: Optional[Dict[str, str]] = kwargs.get("column_types")
        
        if not self.engine.dialect.has_table(self.engine, self.table):
            data_model = self.create_clickhouse_table(column_types)
            with self.engine.connect() as conn:
                conn.connection.create_table(data_model)

        pd.concat(dfs).to_sql(
            name=self.table,
            con=self.engine,
            if_exists="append",
            index=False
        )

    def load(self, **kwargs) -> pd.DataFrame:
        parse_dates: Union[List[str], bool] = kwargs.get("parse_dates", False)
        return pd.read_sql(self.table, self.engine, parse_dates=parse_dates)

    @staticmethod
    def create_clickhouse_table(column_types: Dict[str, str]):
        import infi.clickhouse_orm as orm
        model_namespace = {
            "engine": orm.engines.Memory()
        }
        ch_mapping = {
            'object': orm.fields.StringField,
            'uint64': orm.fields.UInt64Field,
            'uint32': orm.fields.UInt32Field,
            'uint16': orm.fields.UInt16Field,
            'uint8': orm.fields.UInt8Field,
            'bool': orm.fields.UInt8Field,
            'float64': orm.fields.Float64Field,
            'float32': orm.fields.Float32Field,
            'int64': orm.fields.Int64Field,
            'int32': orm.fields.Int32Field,
            'int16': orm.fields.Int16Field,
            'int8': orm.fields.Int8Field,
            'datetime64[D]': orm.fields.DateField,
            'datetime64[ns]': orm.fields.DateTimeField
        }
        for column, _type in column_types.items():
            model_namespace.update(
                {column: ch_mapping[_type]()}
            )
        dff_stats = type(
            "dff_stats",
            (orm.models.Model,),
            model_namespace
        )
        return dff_stats
