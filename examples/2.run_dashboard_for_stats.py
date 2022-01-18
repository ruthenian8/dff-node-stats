from email.policy import default
import dff_node_stats
from dff_node_stats.widgets import StreamlitDashboard

stats = dff_node_stats.stats_builder(
    dff_node_stats.Saver(path="csv://examples/stats.csv"),
    collectors= [
        "NodeLabelCollector",
        "RequestCollector",
        "ResponseCollector"
    ]
)

df = stats.dataframe
from dff_node_stats.widgets import FilterType
filt = FilterType("Choose flow", "flow_label", lambda x, y: x == y, default=None)
filt2 = FilterType("Choose turn", "history_id", lambda x, y: x == y, default=None)

StreamlitDashboard(df, filters=[filt, filt2])()
