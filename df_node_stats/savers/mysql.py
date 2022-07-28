"""
Mysql
---------------------------
Provides the Mysql version of the :py:class:`~dff_node_stats.savers.saver.Saver`. 
You don't need to interact with this class manually, as it will be automatically 
imported and initialized when you construct :py:class:`~dff_node_stats.savers.saver.Saver` with specific parameters.

"""
from typing import List, Optional, Union, Dict
import pandas as pd

from .sqlbase import SqlSaver


class MysqlSaver(SqlSaver):
    """
    Saves the stats dataframe to - and reads from a Mysql database.
    You don't need to interact with this class manually, as it will be automatically
    initialized when you construct :py:class:`~dff_node_stats.savers.saver.Saver` with specific parameters.

    Parameters
    ----------

    path: str
        | The construction path.
        | It should match the sqlalchemy :py:class:`~sqlalchemy.engine.Engine` initialization string.

        .. code-block::

            Saver("mysql+pymysql://user:password@localhost:3306/default")

    table: str
        Sets the name of the db table to use. Defaults to "dff_stats".
    """

    def __init__(self, path: str, table: str = "dff_stats") -> None:
        super().__init__(path=path, table=table)

    def save(
        self,
        dfs: List[pd.DataFrame],
        column_types: Optional[Dict[str, str]] = None,
        parse_dates: Union[List[str], bool] = False,
    ) -> None:
        df = pd.concat(dfs)
        dfs = [
            pd.concat(
                [
                    df.loc[:, df.dtypes != "object"],
                    df.loc[:, df.dtypes == "object"].astype("str"),
                ],
                axis=1,
            )
        ]  # cast objects to strings
        super().save(self, dfs, column_types, parse_dates)
