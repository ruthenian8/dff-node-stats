import pytest
import sys

try:
    import streamlit
    import ipywidgets
except ImportError:
    pass

from dff_node_stats.widgets.jupyter import WidgetDashboard
from dff_node_stats.widgets.streamlit import StreamlitDashboard
from dff_node_stats.widgets.widget import AbstractDashboard

if "streamlit" in sys.modules:
    @pytest.fixture(scope="session")
    def testing_swidget(testing_dataframe):
        yield StreamlitDashboard(testing_dataframe)    

    def test_slice_streamlit(testing_swidget):
        assert True

    def test_plots_streamlit(testing_swidget):
        assert True

    def test_controls_streamlit(testing_swidget):
        assert True

if "ipywidgets" in sys.modules:
    @pytest.fixture(scope="session")
    def testing_jwidget(testing_dataframe):
        yield WidgetDashboard(testing_dataframe)

    def test_slice_jupyter(testing_jwidget):
        assert True

    def test_plots_jupyter(testing_jwidget):
        assert True

    def test_controls_jupyter(testing_jwidget):
        assert True