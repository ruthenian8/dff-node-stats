from typing import Dict

from fastapi import FastAPI
import pandas as pd
import uvicorn

from dff_node_stats.utils import requires_transform, requires_columns, transform_once

app = FastAPI()


def api_run(df, port=8000) -> None:
    """
    Launch an API server for a given dataframe
    """

    @transform_once
    @requires_columns(["flow_label", "node_label"])
    def transitions(df: pd.DataFrame) -> pd.DataFrame:
        df["node"] = df.apply(lambda row: f"{row.flow_label}:{row.node_label}", axis=1)
        df = df.drop(["flow_label", "node_label"], axis=1)
        df = df.sort_values(["context_id"], kind="stable")
        df["next_node"] = df.node.shift()
        df = df[df.history_id != 0]
        transitions = df.apply(lambda row: f"{row.node}->{row.next_node}", axis=1)
        return transitions.value_counts()

    @requires_transform(transitions)
    def transition_counts(df) -> Dict[str, int]:
        return {k: int(v) for k, v in dict(df).items()}

    @app.get("/api/v1/stats/transition-counts", response_model=Dict[str, int])
    async def get_transition_counts():
        return transition_counts(df)

    @requires_transform(transitions)
    def transition_probs(df) -> Dict[str, float]:
        tc = {k: int(v) for k, v in dict(df).items()}
        return {k: v / sum(tc.values, 0) for k, v in tc.items()}

    @app.get("/api/v1/stats/transition-probs", response_model=Dict[str, float])
    async def get_transition_probs():
        return transition_probs(df)

    uvicorn.run(app, host="0.0.0.0", port=port)
