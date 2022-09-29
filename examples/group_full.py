import sys
import asyncio
import random
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).absolute().parent))

from df_engine.core import Context, Actor
from df_runner import Pipeline, ServiceGroup, WrapperRuntimeInfo
from df_stats import StatsStorage, StatsRecord, ExtractorPool, get_wrapper_field

from _utils import parse_args, script


extractor_pool = ExtractorPool()


async def heavy_service(_):
    await asyncio.sleep(random.randint(0, 2))


async def get_start_time(ctx: Context, _, info: WrapperRuntimeInfo):
    start_time = datetime.now()
    ctx.misc[get_wrapper_field(info)] = start_time


@extractor_pool.new_extractor
async def get_group_state(ctx: Context, _, info: WrapperRuntimeInfo):
    start_time = ctx.misc[get_wrapper_field(info)]
    data = {"execution_time": datetime.now() - start_time}
    group_stats = StatsRecord.from_context(ctx, info, data)
    return group_stats


actor = Actor(script, ("root", "start"), ("root", "fallback"))

pipeline = Pipeline.from_dict(
    {
        "components": [
            ServiceGroup(
                before_wrapper=[get_start_time],
                after_wrapper=[get_group_state],
                components=[{"handler": heavy_service}, {"handler": heavy_service}],
            ),
            actor,
        ],
    }
)

if __name__ == "__main__":
    args = parse_args()
    stats = StatsStorage.from_uri(args["uri"], table=args["table"])
    stats.add_extractor_pool(extractor_pool)
    pipeline.run()
