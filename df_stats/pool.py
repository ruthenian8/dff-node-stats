import functools
from typing import List, Callable, Optional

from df_engine.core import Context
from df_runner import WrapperRuntimeInfo
from .record import StatsRecord


class ExtractorPool:
    def __init__(self, extractors: Optional[List[Callable]] = None):
        self.subscribers = []
        self.extractors = [self.wrap_extractor(item) for item in extractors] if extractors is not None else []

    def wrap_extractor(self, extractor: Callable) -> Callable:
        @functools.wraps(extractor)
        async def extractor_wrapper(ctx: Context, _, info: WrapperRuntimeInfo):
            result = await extractor(ctx, _, info)
            if result is None:
                return
            for stats_storage in self.subscribers:
                stats_storage.data.append(result)
                await stats_storage.save()
        return extractor_wrapper
    
    def new_extractor(self, extractor: Callable) -> Callable:
        wrapped_extractor = self.wrap_extractor(extractor)
        self.extractors.append(wrapped_extractor)
        return self.extractors[-1]


default_extractor_pool = ExtractorPool()


@default_extractor_pool.new_extractor
async def get_default_actor_data(ctx: Context, _, info: WrapperRuntimeInfo):
    last_label = ctx.last_label or ("", "")
    default_data = StatsRecord.from_context(
        ctx, info, {"flow": last_label[0], "node": last_label[1], "label": ": ".join(last_label)}
    )
    return default_data
