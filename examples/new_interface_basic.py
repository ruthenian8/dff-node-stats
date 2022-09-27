import sys
import asyncio
import random
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).absolute().parent))

from df_engine.core import Context, Actor
from df_runner import Pipeline, WrapperRuntimeInfo, GlobalWrapperType
from df_stats import Stats, Saver, StatsItem, get_wrapper_field, to_stats

from _utils import parse_args, script

"""
Like with regular wrappers, you can define global statistic wrappers, 
which will be applied to every element inside the pipeline.

Use the `add_global_wrapper` method.
"""
stats_extractors_pool = StatisticsExtractorsPool()

async def heavy_service(_):
    await asyncio.sleep(random.randint(0, 2))

async def get_start_time(ctx: Context, _, info: WrapperRuntimeInfo):
    start_time = datetime.now()
    ctx.misc[get_wrapper_field(info)] = start_time


@stats_extractors_pool.new_extractor
async def get_pipeline_state(ctx: Context, _, info: WrapperRuntimeInfo):
    start_time = ctx.misc[get_wrapper_field(info)]
    data = {"execution_time": datetime.now() - start_time}
    return StatisticRecord(data)

def get_pipeline(args) -> Pipeline:

    actor = Actor(script, ("root", "start"), ("root", "fallback"))

    pipeline_dict = {
        "components": [
            [heavy_service for _ in range(0, 5)],
            actor,
        ],
    }
    pipeline = Pipeline.from_dict(pipeline_dict)
    pipeline.add_global_wrapper(GlobalWrapperType.BEFORE_ALL, get_start_time)
    pipeline.add_global_wrapper(GlobalWrapperType.BEFORE_ALL, get_pipeline_state)
    StatisticsStorage(args["uri"], table=args["table"]).add_extractors_pool(stats_extractors_pool)
    return pipeline


if __name__ == "__main__":
    args = parse_args()
    pipeline = get_pipeline(args)
    pipeline.run()
