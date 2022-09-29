"""
Saver
******
Provides the base class :py:class:`~dff_node_stats.savers.saver.Saver`. 
It is an interface class that defines methods for saving and loading dataframes.
On the other hand, it is also used to automatically construct the child classes 
depending on the input parameters. See the class documentation for more info.

"""
from typing import List, Optional
from abc import ABC, abstractmethod

from ..record import StatsRecord


class Saver(ABC):
    """
    :py:class:`~dff_node_stats.savers.saver.Saver` interface requires two methods to be impemented:

    #. :py:meth:`~dff_node_stats.savers.saver.Saver.save`
    #. :py:meth:`~dff_node_stats.savers.saver.Saver.load`

    | A call to Saver is needed to instantiate one of the predefined child classes.
    | The subclass is chosen depending on the `path` parameter value (see Parameters).

    | Your own Saver can be implemented in the following manner:
    | You should subclass the `Saver` class and pass the url prefix as the `storage_type` parameter.
    | Abstract methods `save` and `load` should necessarily be implemented.

    .. code: python
        class MongoSaver(Saver, storage_type="mongo"):
            def __init__(self, path, table):
                ...

            def save(self, data):
                ...

            def load(self):
                ...

    Parameters
    ----------

    path: str
        A string that contains a prefix and a url of the target data storage, separated by ://.
        The prefix is used to automatically import a child class from one of the submodules
        and instantiate it.
        For instance, a call to `Saver("csv://...")` will eventually produce a :py:class:`~dff_node_stats.savers.csv_saver.CsvSaver`,
        while a call to `Saver("clickhouse://...")` produces a :py:class:`~dff_node_stats.savers.clickhouse.ClickHouseSaver`

    table: str
        Sets the name of the db table to use, if necessary. Defaults to "dff_stats".
    """

    _saver_mapping = {}

    # TODO: remove __init_subclass__
    def __init_subclass__(cls, storage_type: str, **kwargs) -> None:
        super().__init_subclass__(**kwargs)
        cls._saver_mapping[storage_type] = cls

    # TODO: remove __new__
    def __new__(cls, path: Optional[str] = None, table: str = "df_stats"):
        storage_type, _, _ = path.partition("://")
        assert storage_type, "Saver should be initialized with either:" "csv://path_to_file or dbname://engine_params"

        subclass = cls._saver_mapping.get(storage_type)
        assert subclass, f"Cannot recognize option: {storage_type}"
        obj = object.__new__(subclass)
        obj.__init__(str(path), table)
        return obj

    @abstractmethod
    def save(
        self,
        data: List[StatsRecord],
    ) -> None:
        """
        Save the data to a database or a file.
        Append if the table already exists.

        Parameters
        ----------

        dfs: List[pd.DataFrame]
        column_types: Optional[Dict[str, str]] = None
        parse_dates: Union[List[str], bool] = False
        """
        raise NotImplementedError

    @abstractmethod
    def load(self) -> List[StatsRecord]:
        """
        Load the data from a database or a file.

        Parameters
        ----------

        column_types: Optional[Dict[str, str]] = None
        parse_dates: Union[List[str], bool] = False
        """
        raise NotImplementedError
