import dff_node_stats
from dff_node_stats.widgets import StreamlitDashboard

stats = dff_node_stats.stats_builder(
    dff_node_stats.Saver(path="csv://D:/project/dff-node-stats/examples/stats.csv"),
    collectors= [
        "NodeLabelCollector",
        "RequestCollector",
        "ResponseCollector"
    ]
)

df = stats.dataframe

StreamlitDashboard(df)()
