from this import d
import pytest

from dff_node_stats.widgets.visualizers import colorize, show_duration_time 
from dff_node_stats.utils import DffStatsException
import pandas as pd


def test_colors():
    for color, num in colorize(range(100)):
        assert color


def test_error_raising():
    with pytest.raises(DffStatsException) as error:
        df = pd.DataFrame(columns=["foo", "bar"])
        fig = show_duration_time(df)
    assert "Required columns missing" in error


