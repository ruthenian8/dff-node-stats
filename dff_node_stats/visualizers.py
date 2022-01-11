import streamlit
import graphviz
import pandas as pd
pd.options.plotting.backend = "plotly"

def show_node(df: pd.DataFrame) -> pd.DataFrame:
    fig = df.plot.bar()
    return fig

def show_nodes(df: pd.DataFrame) -> pd.DataFrame:
    @streamlit.cache(allow_output_mutation=True)
    def get_nodes_and_edges(df: pd.DataFrame):
        for context_id in df.context_id.unique():
            ctx_index = df.context_id == context_id
            df.loc[ctx_index, "node"] = (
                df.loc[ctx_index, "flow_label"]
                + ":"
                + df.loc[ctx_index, "node_label"]
            )
            df.loc[ctx_index, "edge"] = (
                df.loc[ctx_index, "node"]
                .shift(periods=1)
                .combine(df.loc[ctx_index, "node"], lambda *x: list(x))
            )
            flow_label = df.loc[ctx_index, "flow_label"]
            df.loc[ctx_index, "edge_type"] = flow_label.where(
                flow_label.shift(periods=1) == flow_label, "MIXED"
            )
        return df

    df = get_nodes_and_edges(df)
    node_counter = df.node.value_counts()
    edge_counter = df.edge.value_counts()
    node2code = {key: f"n{index}" for index, key in enumerate(df.node.unique())}

    streamlit.subheader("Graph of Transitions")
    graph = graphviz.Digraph()
    graph.attr(compound="true")
    flow_labels = df.flow_label.unique()
    for i, flow_label in enumerate(flow_labels):
        with graph.subgraph(name=f"cluster{i}") as sub_graph:
            sub_graph.attr(style="filled", color="lightgrey")
            sub_graph.attr(label=flow_label)

            sub_graph.node_attr.update(style="filled", color="white")

            for _, (history_id, node, node_label) in df.loc[
                df.flow_label == flow_label, ("history_id", "node", "node_label")
            ].iterrows():
                counter = node_counter[node]
                label = f"{node_label} ({counter=})"
                if history_id == -1:
                    sub_graph.node(node2code[node], label=label, shape="Mdiamond")
                else:
                    sub_graph.node(node2code[node], label=label)

    for (in_node, out_node), counter in edge_counter.items():
        if isinstance(in_node, str):
            label = f"(probs={counter/node_counter[in_node]:.2f})"
            graph.edge(node2code[in_node], node2code[out_node], label=label)

    streamlit.graphviz_chart(graph)

    streamlit.subheader("Transition Trace")
    df_trace = df[["history_id", "flow_label", "node"]]
    df_trace.index = df_trace.history_id
    df_trace = df_trace.drop(columns=["history_id"])
    df_trace
    node_trace = {}
    for flow_label in df_trace.flow_label.unique():
        node_trace[flow_label] = df_trace.loc[
            df_trace.flow_label == flow_label, "node"
        ]
    streamlit.bar_chart(df_trace.loc[:, "node"])

    streamlit.subheader("Node counters")
    node_counters = {}
    for flow_label in flow_labels:
        node_counters[flow_label] = df.loc[
            df.flow_label == flow_label, "node_label"
        ].value_counts()
    streamlit.bar_chart(node_counters)

    streamlit.subheader("Transitions counters")
    edge_counters = {}
    for edge_type in df.edge_type.unique():
        edge_counters[edge_type] = (
            df.loc[df.edge_type == edge_type, "edge"].astype("str").value_counts()
        )
    streamlit.bar_chart(edge_counters)

    streamlit.subheader("Transitions duration [sec]")
    edge_time = df[["edge", "edge_type", "duration_time"]]
    edge_time = edge_time.astype({"edge": "str"})
    edge_time = edge_time.groupby(["edge", "edge_type"], as_index=False).mean()
    edge_time.index = edge_time.edge

    edge_duration = {}
    for edge_type in df.edge_type.unique():
        edge_duration[edge_type] = edge_time.loc[
            edge_time.edge_type == edge_type, "duration_time"
        ]
    streamlit.bar_chart(edge_duration)
    return df