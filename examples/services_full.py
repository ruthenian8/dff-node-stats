import sys
import asyncio
import random
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).absolute().parent))

from df_engine.core import Context, Actor
from df_runner import Pipeline, Service, WrapperRuntimeInfo, to_service
from df_stats import Stats, Saver, StatsItem, get_wrapper_field

from _utils import parse_args, script

"""

"""


async def heavy_service(_):
    await asyncio.sleep(random.randint(0, 5))


async def get_start_time(stats: Stats, ctx: Context, _, info: WrapperRuntimeInfo):
    start_time = datetime.now()
    ctx.misc[get_wrapper_field(info)] = start_time


async def get_service_state(stats: Stats, ctx: Context, _, info: WrapperRuntimeInfo):
    start_time = ctx.misc[get_wrapper_field(info)]
    data = {"execution_time": datetime.now() - start_time}
    group_stats = StatsItem.from_context(ctx, info, data)
    stats.data.append(group_stats)
    await stats.save()


def get_pipeline(args) -> Pipeline:
    saver = Saver(args["dsn"], table=args["table"])
    stats = Stats(saver=saver)
    before_wrapper = stats.get_wrapper(get_service_state)
    after_wrapper = stats.get_wrapper(get_service_state)

    actor = Actor(script, ("root", "start"), ("root", "fallback"))

    pipeline = Pipeline.from_dict(
        {"components": [
            Service(handler=to_service(
                before_wrapper=[before_wrapper], after_wrapper=[after_wrapper]
            )(heavy_service)),
            Service(handler=to_service(
                before_wrapper=[before_wrapper], after_wrapper=[after_wrapper]
            )(actor))
        ]}
    )
    return pipeline


if __name__ == "__main__":
    args = parse_args()
    pipeline = get_pipeline(args)
    pipeline.run()
