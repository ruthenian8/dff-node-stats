from typing import List, Dict, Protocol, runtime_checkable, Any
import datetime

from pydantic import validate_arguments, Field
from dff.core import Context, Actor
import pandas as pd


@runtime_checkable
class Collector(Protocol):
    @property
    def column_dtypes(self) -> Dict[str, str]:
        """
        String names and string pandas types for the collected data
        """
        return None

    @property
    def parse_dates(self) -> List[str]:
        """
        String names of columns that should be parsed as dates
        """
        return []

    def collect_stats(
        self, ctx: Context, actor: Actor, *args, **kwargs
    ) -> Dict[str, Any]:
        """
        Extract the required data from the context
        """
        raise NotImplementedError


class DefaultCollector():
    @property
    def column_dtypes(self) -> Dict[str, str]:
        return {
            "context_id": "str",
            "history_id": "int64",
            "start_time": "datetime64[ns]",
            "duration_time": "float64",
        }

    @property
    def parse_dates(self) -> List[str]:
        return ["start_time"]

    @validate_arguments
    def collect_stats(
        self, ctx: Context, actor: Actor, *args, **kwargs
    ) -> Dict[str, Any]:
        indexes = list(ctx.labels) or [-1]
        current_index = indexes[-1]
        start_time = kwargs.get("start_time") or datetime.datetime.now()
        return {
            "context_id": [str(ctx.id)],
            "history_id": [current_index],
            "start_time": [start_time],
            "duration_time": [(datetime.datetime.now() - start_time).total_seconds()],
        }


class NodeLabelCollector():
    @property
    def column_dtypes(self) -> Dict[str, str]:
        return {
            "flow_label": "str",
            "node_label": "str",
        }

    @property
    def parse_dates(self) -> List[str]:
        return []

    @validate_arguments
    def collect_stats(
        self, ctx: Context, actor: Actor, *args, **kwargs
    ) -> Dict[str, Any]:
        last_label = ctx.last_label or actor.start_label
        return {
            "flow_label": [last_label[0]],
            "node_label": [last_label[1]],
        }

    def streamlit_run(self, streamlit: ModuleType, df: pd.DataFrame) -> pd.DataFrame:
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

    def api_run(self, app: FastAPI, df: pd.DataFrame) -> FastAPI:
        def transition_counts(df: pd.DataFrame) -> Dict[str, int]:
            df["node"] = df.apply(
                lambda row: f"{row.flow_label}:{row.node_label}", axis=1
            )
            df = df.drop(["flow_label", "node_label"], axis=1)
            df = df.sort_values(["context_id"], kind="stable")
            df["next_node"] = df.node.shift()
            df = df[df.history_id != 0]
            transitions = df.apply(lambda row: f"{row.node}->{row.next_node}", axis=1)
            return {k: int(v) for k, v in dict(transitions.value_counts()).items()}

        tc = transition_counts(df)

        def transition_probs(df: pd.DataFrame) -> Dict[str, float]:
            return {k: v / sum(tc.values, 0) for k, v in tc.items()}

        @app.get("/api/v1/stats/transition-counts", response_model=Dict[str, int])
        async def get_transition_counts():
            return tc

        @app.get("/api/v1/stats/transition-probs", response_model=Dict[str, float])
        async def get_transition_probs():
            return transition_probs(df)

        return app


class RequestCollector():
    @property
    def column_dtypes(self) -> Dict[str, str]:
        return {"user_request": "str"}

    @property
    def parse_dates(self) -> List[str]:
        return []

    @validate_arguments
    def collect_stats(
        self, ctx: Context, actor: Actor, *args, **kwargs
    ) -> Dict[str, Any]:
        return {"user_request": [ctx.last_request or ""]}

    def streamlit_run(self, streamlit: ModuleType, df: pd.DataFrame) -> pd.DataFrame:
        """TODO: implement"""
        return df

    def api_run(self, app: FastAPI, df: pd.DataFrame) -> FastAPI:
        return app


class ResponseCollector():
    @property
    def column_dtypes(self) -> Dict[str, str]:
        return {"bot_response": "str"}

    @property
    def parse_dates(self) -> List[str]:
        return []

    @validate_arguments
    def collect_stats(
        self, ctx: Context, actor: Actor, *args, **kwargs
    ) -> Dict[str, Any]:
        return {"bot_response": [ctx.last_response or ""]}

    def streamlit_run(self, streamlit: ModuleType, df: pd.DataFrame) -> pd.DataFrame:
        """TODO: implement"""
        return df

    def api_run(self, app: FastAPI, df: pd.DataFrame) -> FastAPI:
        return app


class ContextCollector():
    """
    :param column_dtypes: names and pd types of columns
    :param parse_dates: names of columns with datetime
    The user needs to provide a datatype for each
    key that must be extracted from the ctx.misc
    object. In case the value is a dict or a list,
    the required type is 'object'.
    Names of columns with type 'datetime' can be
    optionally listed in 'parse_dates'
    """

    def __init__(
        self,
        column_dtypes: Dict[str, str],
        parse_dates: List[str]    
    ) -> None:
        self._column_dtypes = column_dtypes,
        self._parse_dates = parse_dates
        return

    @property
    def column_dtypes(self) -> Dict[str, str]:
        return self._column_dtypes

    @property
    def parse_dates(self) -> List[str]:
        return self._parse_dates

    @validate_arguments
    def collect_stats(
        self, ctx: Context, actor: Actor, *args, **kwargs
    ) -> Dict[str, Any]:
        misc_stats = dict()
        for key in self.column_dtypes:
            value = ctx.misc.get(key, None)
            misc_stats[key] = [value]
        return misc_stats

    def streamlit_run(self, streamlit: ModuleType, df: pd.DataFrame) -> pd.DataFrame:
        """TODO: implement"""
        return df

    def api_run(self, app: FastAPI, df: pd.DataFrame) -> FastAPI:
        return app
