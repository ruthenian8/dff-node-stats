import sys
import asyncio, random
from pathlib import Path

sys.path.insert(0, str(Path(__file__).absolute().parent))

from df_engine.core import Context, Actor
from df_runner import Pipeline, ServiceGroup, WrapperRuntimeInfo
from df_stats import StatsStorage, StatsRecord, ExtractorPool

from _utils import parse_args, script

"""
Wrappers can be applied to any pipeline parameter, including service groups.
The `ServiceGroup` constructor has `before_wrapper` and `after_wrapper` parameters, 
to which wrapper functions can be passed.

"""

extractor_pool = ExtractorPool()


async def heavy_service(_):# TODO: add ordeanary args
    await asyncio.sleep(random.randint(0, 2))


@extractor_pool.new_extractor
async def get_group_stats(ctx: Context, _, info: WrapperRuntimeInfo):
    data = {"runtime_state": info["component"]["execution_state"]}
    group_stats = StatsRecord.from_context(ctx, info, data)
    return group_stats


actor = Actor(script, ("root", "start"), ("root", "fallback"))

pipeline = Pipeline.from_dict(
    {
        "components": [
            ServiceGroup(
                after_wrapper=[get_group_stats],
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
