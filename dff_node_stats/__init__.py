# flake8: noqa: F401
from .stats import Stats
from .stats_new import StatsBuilder

stats_builder = StatsBuilder()
from .savers import Saver
from .collectors import ContextCollector

from .widgets import (
    WidgetDashboard,
    StreamlitDashboard
)
