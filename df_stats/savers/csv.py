"""
CSV
---------------------------
Provides the CSV version of the :py:class:`~dff_node_stats.savers.saver.Saver`. 
You don't need to interact with this class manually, as it will be automatically 
initialized when you construct a :py:class:`~dff_node_stats.savers.saver.Saver` with specific parameters.

"""
import csv
from typing import List
import pathlib
import os

from ..stats import StatsData


class CsvSaver:
    """
    Saves and reads the stats dataframe from a csv file.
    You don't need to interact with this class manually, as it will be automatically
    initialized when you construct :py:class:`~dff_node_stats.savers.saver.Saver` with specific parameters.

    Parameters
    ----------

    path: str
        | The construction path.
        | The part after :// should contain a path to the file that pandas will be able to recognize.

        .. code-block::

            CsvSaver("csv://foo/bar.csv")

    table: str
        Does not affect the class. Added for constructor uniformity.
    """

    def __init__(self, path: str, table: str = "dff_stats") -> None:
        path = path.partition("://")[2]
        self.path = pathlib.Path(path)

    async def save(self, data_dicts: List[StatsData]) -> None:

        saved_data = []
        if self.path.exists() and os.path.getsize(self.path) > 0:
            saved_data = await self.load()

        data = saved_data + data_dicts

        with open(self.path, "w", encoding="utf-8") as file:
            writer = csv.writer(file, delimiter=",", quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            writer.writerow(data[0].keys())
            for item in data:
                writer.writerow(item.values())

    async def load(self) -> List[dict]:
        with open(self.path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file, delimiter=",", quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
            return [row for row in reader]
