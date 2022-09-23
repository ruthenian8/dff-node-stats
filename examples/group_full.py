import sys
import asyncio
import random
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).absolute().parent))

from df_engine.core import Context, Actor
from df_runner import Pipeline, ServiceGroup, WrapperRuntimeInfo
from df_stats import Stats, StatsItem, Saver, get_wrapper_field

from _utils import parse_args, script


async def heavy_service(_):
    await asyncio.sleep(random.randint(0, 2))


async def get_start_time(stats: Stats, ctx: Context, _, info: WrapperRuntimeInfo):
    start_time = datetime.now()
    ctx.misc[get_wrapper_field(info)] = start_time


async def get_group_state(stats: Stats, ctx: Context, _, info: WrapperRuntimeInfo):
    start_time = ctx.misc[get_wrapper_field(info)]
    data = {"execution_time": datetime.now() - start_time}
    group_stats = StatsItem.from_context(ctx, info, data)
    stats.data.append(group_stats)
    await stats.save()


def get_pipeline(args) -> Pipeline:
    saver = Saver(args["dsn"], table=args["table"])
    stats = Stats(saver=saver)

    actor = Actor(script, ("root", "start"), ("root", "fallback"))

    pipeline = Pipeline.from_dict(
        {
            "components": [
                ServiceGroup(
                    before_wrapper=[stats.get_wrapper(get_start_time)],
                    after_wrapper=[stats.get_wrapper(get_group_state)],
                    components=[{"handler": heavy_service}, {"handler": heavy_service}],
                ),
                actor,
            ],
        }
    )
    return pipeline


if __name__ == "__main__":
    args = parse_args()
    pipeline = get_pipeline(args)
    pipeline.run()
