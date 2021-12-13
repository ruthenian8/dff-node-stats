import dff_node_stats

# stats = dff_node_stats.Stats(csv_file="examples/stats.csv")
stats = dff_node_stats.stats_builder(
    saver=dff_node_stats.CsvSaver("examples/stats.csv"),
    collectors= [
        "NodeLabelCollector",
        "RequestCollector",
        "ResponseCollector"
    ]
)

stats.streamlit_run()
