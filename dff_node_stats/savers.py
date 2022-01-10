"""Savers for stats
TODO: try removing thread pools instead of removing engines, which is inefficient
TODO: Implement another loader using pure infi.orm instead of SQLAlchemy
"""
from typing import Dict, List, Union, Optional, Protocol, runtime_checkable
import pathlib
import pandas as pd


@runtime_checkable
class Saver(Protocol):
    _saver_mapping = {}
    _instance = None

    def __init_subclass__(cls, _id: str, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        cls._saver_mapping[_id] = cls

    def __new__(cls, path: Optional[str] = None):
        if cls._instance is None:
            assert isinstance(
                path, str
            ), """
            Saver should be initialized with a string
            """
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
            cls._instance = obj
        return cls._instance

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

        for key in parse_dates:
            if key in column_types:
                column_types.pop(key)
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
    def __init__(self, path: str) -> None:
        import sqlalchemy as sa

        if not hasattr(self, "path"):
            self.path: str = path
        self.schema: str = self.path[self.path.rfind("/") + 1 :]
        self.table: str = "dff_stats"
        self.engine_factory = sa.create_engine
        self.engine = None

    def save(self, dfs: List[pd.DataFrame], **kwargs) -> None:
        column_types: Optional[Dict[str, str]] = kwargs.get("column_types")

        # create engine on the first call of the method
        if self.engine is None:
            self.engine = self.engine_factory(
                self.path,
                # echo=True,
                # echo_pool="debug"
            )
        # recreate table if the schema was altered
        df = pd.concat(dfs)
        if self.engine.dialect.has_table("dff_stats"):
            metadata = PGSaver.sa.schema.MetaData()
            ExistingModel = Table('dff_stats', metadata, autoload_with=self.engine)
            # if current schema contains new columns, drop the table to recreate it later
            if not all([list(column_types.keys()) in ExistingModel.columns]):
                existing_df = self.load(parse_dates=kwargs.get("parse_dates", False))
                df = pd.concat([existing_df, df], axis=0)
                ExistingModel.drop(bind=self.engine)

        self.engine.dialect._psycopg2_extensions().register_adapter(
            dict,
            self.engine.dialect._psycopg2_extras().Json
        )
        # for key in column_types:
        #     if column_types[key] == "object":
        #         column_types[key] = self.engine.dialects.postgresql.JSONB

        df.to_sql(name=self.table, con=self.engine, if_exists="append")
        self.engine = None

    def load(self, **kwargs) -> pd.DataFrame:
        parse_dates: Union[List[str], bool] = kwargs.get("parse_dates", False)
        # create engine on the first call of the method
        if self.engine is None:
            self.engine = self.engine_factory(
                self.path,
                # echo=True,
                # echo_pool="debug"
            )
        df = pd.read_sql_table(
            table_name=self.table, con=self.engine, parse_dates=parse_dates
        )
        self.engine = None
        return df


class InfiSaver(Saver, _id="clickhouse"):
    """Alternative clickhouse saver"""
    def __init__(self, path: str) -> None:
        from infi.clickhouse_orm.database import Database
        if not hasattr(self, "path"):
            self.path = path
        auth, _, address = path.partition("@")
        address, _, db_name = address.partition("/")
        address = "http://" + address
        username, _, password = auth.partition("://")[2].partition(":")
        if not all([db_name, address, username, password]):
            raise ValueError("Invalid database URI or credentials")
        self.db = Database(db_name, db_url=address, username=username, password=password)
        return

    def save(self, dfs: List[pd.DataFrame], **kwargs) -> None:
        column_types: Dict[str, str] = kwargs.get("column_types")
        parse_dates: List[str] = kwargs.get("parse_dates")
        df = pd.concat(dfs)
        
        Model = self.create_clickhouse_table(column_types)
        
        # in case new columns have been added, we recreate the table
        if self.db.does_table_exist(Model):
            ExistingModel = self.db.get_model_for_table('dff_stats', system_table=False)
            if Model.fields() != ExistingModel.fields():
                existing_df = self.load()
                self.db.drop_table(ExistingModel)
                df = pd.concat([existing_df, df], axis=0)
            
        def lazyupload(df):
            for _, row in df.iterrows():
                row = row.to_dict()
                for column in parse_dates:
                    row[column] = row[column].to_pydatetime()
                yield Model(**row)
            
        self.db.create_table(Model)
        self.db.insert(lazyupload(df), batch_size=1000)

    def load(self, **kwargs) -> pd.DataFrame:
        Model = self.db.get_model_for_table('dff_stats', system_table=False)
        results = self.db.select(query="SELECT * FROM dff_stats", model_class=Model)
        df = pd.DataFrame.from_records([item.to_dict() for item in results])
        for column in parse_dates:
            df[column] = df[column].astype("datetime64[ns]")
        return df

    @staticmethod
    def create_clickhouse_table(column_types: Dict[str, str]):
        import infi.clickhouse_orm as orm

        model_namespace = {"engine": orm.engines.Memory()}
        ch_mapping = {
            "object": orm.fields.StringField,
            "str": orm.fields.StringField,
            "uint64": orm.fields.UInt64Field,
            "uint32": orm.fields.UInt32Field,
            "uint16": orm.fields.UInt16Field,
            "uint8": orm.fields.UInt8Field,
            "bool": orm.fields.UInt8Field,
            "float64": orm.fields.Float64Field,
            "float32": orm.fields.Float32Field,
            "int64": orm.fields.Int64Field,
            "int32": orm.fields.Int32Field,
            "int16": orm.fields.Int16Field,
            "int8": orm.fields.Int8Field,
            "datetime64[D]": orm.fields.DateField,
            "datetime64[ns]": orm.fields.DateTimeField,
        }
        for column, _type in column_types.items():
            model_namespace.update(
                {column: orm.fields.NullableField(
                    ch_mapping[_type](), extra_null_values=[float("nan")]
                )}
            )
        dff_stats = type("dff_stats", (orm.models.Model,), model_namespace)
        return dff_stats