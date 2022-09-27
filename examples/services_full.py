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
As is the case with the regular wrappers, you can add df_stats wrappers both before and after the
target service. You can use a wrapper that runs before the service to compare the pre-service and post-service
states of the context, measure the running time, etc. Use `get_wrapper_field` function to save the required
values to the context.

Pass before- and after-wrappers to the respective parameters of the `to_service` decorator.

"""


async def heavy_service(_):
    await asyncio.sleep(random.randint(0, 2))


async def get_start_time(ctx: Context, _, info: WrapperRuntimeInfo):
    start_time = datetime.now()
    ctx.misc[get_wrapper_field(info)] = start_time


async def get_service_state(ctx: Context, _, info: WrapperRuntimeInfo):
    start_time = ctx.misc[get_wrapper_field(info)]
    data = {"execution_time": datetime.now() - start_time}
    return StatsItem.from_context(ctx, info, data)


def get_pipeline(args) -> Pipeline:
    stats = Stats.from_uri(args["uri"], table=args["table"])
    before_wrapper = stats.get_wrapper(get_start_time)
    after_wrapper = stats.get_wrapper(get_service_state)

    actor = Actor(script, ("root", "start"), ("root", "fallback"))

    pipeline = Pipeline.from_dict(
        {
            "components": [
                Service(
                    handler=to_service(before_wrapper=[before_wrapper], after_wrapper=[after_wrapper])(heavy_service)
                ),
                Service(handler=to_service(before_wrapper=[before_wrapper], after_wrapper=[after_wrapper])(actor)),
            ]
        }
    )
    return pipeline


if __name__ == "__main__":
    args = parse_args()
    pipeline = get_pipeline(args)
    pipeline.run()
