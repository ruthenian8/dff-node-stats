"""
Savers for stats
"""
from typing import Dict, List, Union, Optional, Protocol, runtime_checkable
import pathlib
import importlib

import pandas as pd


@runtime_checkable
class Saver(Protocol):
    """
    A generic Saver class that imports and instantiates the required saver type
    depending on the input arguments
    """

    _saver_mapping = {}
    _path = None

    def __init_subclass__(cls, storage_type: str, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        cls._saver_mapping[storage_type] = cls.__name__

    def __new__(cls, path: Optional[str] = None, table: str = "dff_stats"):
        if not path and not cls._path:
            raise ValueError(
                """
            Saver should be initialized with a string
            """
            )
        cls._path = path = path or cls._path  # need this workaround cause pydantic constantly calls __new__
        storage_and_path = path.partition("://")
        if not all(storage_and_path):
            raise ValueError(
                """Saver should be initialized with either:
                csv://path_to_file or dbname://engine_params
                Available options: {}
                """.format(
                    ", ".join(list(cls._saver_mapping.keys()))
                )
            )
        storage_type = storage_and_path[0]
        subclass_name = cls._saver_mapping.get(storage_type)
        if not subclass_name:
            raise ValueError(
                """
                Cannot recognize option: {}
                Available options: {}            
                """.format(
                    storage_type, ", ".join(list(cls._saver_mapping.keys()))
                )
            )
        subclass = getattr(
            importlib.import_module(f".{storage_type}", package="dff_node_stats.savers"),
            subclass_name,
        )
        obj = object.__new__(subclass)
        obj.__init__(str(path), table)
        return obj

    def save(
        self,
        dfs: List[pd.DataFrame],
        column_types: Optional[Dict[str, str]] = None,
        parse_dates: Union[List[str], bool] = False,
    ) -> None:
        """
        Save the data to a database or a file
        Append if the table already exists
        """
        raise NotImplementedError

    def load(
        self,
        column_types: Optional[Dict[str, str]] = None,
        parse_dates: Union[List[str], bool] = False,
    ) -> pd.DataFrame:
        """
        Load the data from a database or a file
        """
        raise NotImplementedError


class ClickHouseSaver(Saver, storage_type="clickhouse"):
    """ClickHouseSaver Class prototype"""

    pass


class CsvSaver(Saver, storage_type="csv"):
    """CsvSaver Class prototype"""

    pass


class PostgresSaver(Saver, storage_type="postgresql"):
    """PostgresSaver Class prototype"""

    pass
