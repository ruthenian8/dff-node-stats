import sys
import asyncio
import random
from pathlib import Path

sys.path.insert(0, str(Path(__file__).absolute().parent))

from df_engine.core import Context, Actor
from df_runner import Pipeline, WrapperRuntimeInfo, GlobalWrapperType, to_service
from df_stats import Stats, Saver, StatsItem

from _utils import parse_args, script


async def heavy_service(_):
    await asyncio.sleep(random.randint(0, 2))


async def get_pipeline_state(ctx: Context, _, info: WrapperRuntimeInfo):
    data = {"runtime_state": info["component"]["execution_state"]}
    group_stats = StatsItem.from_context(ctx, info, data)
    return group_stats


def get_pipeline(args) -> Pipeline:
    stats = Stats.from_uri(args["uri"], table=args["table"])
    global_wrapper = stats.get_wrapper(get_pipeline_state)

    actor = Actor(script, ("root", "start"), ("root", "fallback"))

    pipeline_dict = {
        "components": [
            [heavy_service for _ in range(0, 5)],
            actor,
        ],
    }
    pipeline = Pipeline.from_dict(pipeline_dict)
    pipeline.add_global_wrapper(GlobalWrapperType.AFTER_ALL, global_wrapper)
    return pipeline


if __name__ == "__main__":
    args = parse_args()
    pipeline = get_pipeline(args)
    pipeline.run()
