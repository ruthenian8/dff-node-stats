from typing import Dict

from fastapi import FastAPI
import pandas as pd
import uvicorn

def api_run(self, port=8000) -> None:
    """
    Methods for API get defined
    inside the collectors as well
    """
    import uvicorn
    from fastapi import FastAPI

    app = FastAPI()
    df = self.dataframe
    for collector in self.collectors:
        app = collector.api_run(app, df)
    uvicorn.run(app, host="0.0.0.0", port=port)


def api_run(app: FastAPI, df: pd.DataFrame) -> FastAPI:
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