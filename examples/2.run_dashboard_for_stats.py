import dff_node_stats

# stats = dff_node_stats.Stats(csv_file="examples/stats.csv")
stats = dff_node_stats.stats_builder(
    dff_node_stats.Saver(path="clickhouse://root:***@localhost:8123/test"),
    collectors= [
        "NodeLabelCollector",
        "RequestCollector",
        "ResponseCollector"
    ]
)

stats.streamlit_run()
