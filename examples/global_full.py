import sys
import asyncio
import random
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).absolute().parent))

from df_engine.core import Context, Actor
from df_runner import Pipeline, WrapperRuntimeInfo, GlobalWrapperType
from df_stats import StatsStorage, ExtractorPool, StatsRecord, get_wrapper_field

from _utils import parse_args, script

"""
Like with regular wrappers, you can define global statistic wrappers, 
which will be applied to every element inside the pipeline.

Use the `add_global_wrapper` method.
"""


extractor_pool = ExtractorPool()


async def heavy_service(_):
    await asyncio.sleep(random.randint(0, 2))


@extractor_pool.new_extractor
async def get_start_time(ctx: Context, _, info: WrapperRuntimeInfo):
    start_time = datetime.now()
    ctx.misc[get_wrapper_field(info)] = start_time


@extractor_pool.new_extractor
async def get_pipeline_state(ctx: Context, _, info: WrapperRuntimeInfo):
    start_time = ctx.misc[get_wrapper_field(info)]
    data = {"execution_time": datetime.now() - start_time}
    group_stats = StatsRecord.from_context(ctx, info, data)
    return group_stats


actor = Actor(script, ("root", "start"), ("root", "fallback"))

pipeline_dict = {
    "components": [
        [heavy_service for _ in range(0, 5)],
        actor,
    ],
}
pipeline = Pipeline.from_dict(pipeline_dict)
pipeline.add_global_wrapper(GlobalWrapperType.BEFORE_ALL, get_start_time)
pipeline.add_global_wrapper(GlobalWrapperType.AFTER_ALL, get_pipeline_state)

if __name__ == "__main__":
    args = parse_args()
    stats = StatsStorage.from_uri(args["uri"], table=args["table"])
    stats.add_extractor_pool(extractor_pool)
    pipeline.run()
