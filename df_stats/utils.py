import json
import datetime
from typing import Any

from pydantic import BaseModel, Field
from df_engine.core.context import Context, get_last_index
from df_runner import WrapperRuntimeInfo

STATS_KEY = "STATS_KEY"


def get_wrapper_field(info: WrapperRuntimeInfo) -> str:
    return f"{info['component']['path']}-{str(info['function'])}"


class StatsItem(BaseModel):
    context_id: str
    request_id: int
    time: datetime.datetime = Field(default_factory=datetime.datetime.now)
    data_key: str
    data: dict

    @classmethod
    def from_context(cls, ctx: Context, info: WrapperRuntimeInfo, data: Any):
        context_id = str(ctx.id)
        request_id = get_last_index(ctx.requests)
        data_key = get_wrapper_field(info)
        data = data
        return cls(context_id=context_id, request_id=request_id, data_key=data_key, data=data)

    class Config:
        orm_mode = True
