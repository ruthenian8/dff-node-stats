"""
Postgresql
---------------------------
Provides the Postgresql version of the :py:class:`~dff_node_stats.savers.saver.Saver`. 
You don't need to interact with this class manually, as it will be automatically 
imported and initialized when you construct :py:class:`~dff_node_stats.savers.saver.Saver` with specific parameters.

"""
from .sqlbase import SqlSaver


class PostgresSaver(SqlSaver):
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

    def __init__(self, path: str, table: str = "dff_stats") -> None:
        super().__init__(path=path, table=table)
        self.engine.dialect._psycopg2_extensions().register_adapter(dict, self.engine.dialect._psycopg2_extras().Json)
