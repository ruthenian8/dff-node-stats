from typing import List, Optional, Union, Dict
import pathlib

import pandas as pd


class CsvSaver:
    """Class to save stats to a csv file"""

    def __init__(self, path: str, table: str = "dff_stats") -> None:
        path = path.partition("://")[2]
        self.path = pathlib.Path(path)

    def save(
        self,
        dfs: List[pd.DataFrame],
        column_types: Optional[Dict[str, str]] = None,
        parse_dates: Union[List[str], bool] = False,
    ) -> None:

        for key in parse_dates:
            if key in column_types:
                column_types.pop(key)
        saved_df = (
            self.load(column_types=column_types, parse_dates=parse_dates) if self.path.exists() else pd.DataFrame()
        )
        pd.concat([saved_df] + dfs).to_csv(self.path, index=False)

    def load(
        self,
        column_types: Optional[Dict[str, str]] = None,
        parse_dates: Union[List[str], bool] = False,
    ) -> pd.DataFrame:
        if parse_dates and column_types:
            true_types = {k: v for k, v in column_types.items() if k in (column_types.keys() ^ set(parse_dates))}
        return pd.read_csv(
            self.path,
            usecols=column_types.keys(),
            dtype=true_types,
            parse_dates=parse_dates,
        )
