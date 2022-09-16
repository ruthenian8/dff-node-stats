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

    @staticmethod
    def insert_method(table, conn, keys, data_iter):
        """
        Execute SQL statement inserting data

        Parameters
        ----------
        table : pandas.io.sql.SQLTable
        conn : sqlalchemy.engine.Engine or sqlalchemy.engine.Connection
        keys : list of str
            Column names
        data_iter : Iterable that iterates the values to be inserted
        """
        # gets a DBAPI connection that can provide a cursor
        dbapi_conn = conn.connection
        with dbapi_conn.cursor() as cur:
            s_buf = StringIO()
            writer = csv.writer(s_buf)
            writer.writerows(data_iter)
            s_buf.seek(0)

            columns = ", ".join(['"{}"'.format(k) for k in keys])
            if table.schema:
                table_name = "{}.{}".format(table.schema, table.name)
            else:
                table_name = table.name

            sql = "COPY {} ({}) FROM STDIN WITH CSV".format(table_name, columns)
            cur.copy_expert(sql=sql, file=s_buf)


s = Session()
objects = [User(name="u1"), User(name="u2"), User(name="u3")]
s.bulk_save_objects(objects)
s.commit()
sqlalchemy.ext.asyncio.AsyncSession.add_all
