import sys
import asyncio
import random
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).absolute().parent))

from df_engine.core import Context, Actor
from df_runner import Pipeline, Service, WrapperRuntimeInfo, to_service
from df_stats import StatsStorage, ExtractorPool, StatsRecord, get_wrapper_field

from _utils import parse_args, script

"""
As is the case with the regular wrappers, you can add extractors both before and after the
target service. You can use a wrapper that runs before the service to compare the pre-service and post-service
states of the context, measure the running time, etc. Use the `get_wrapper_field` function to save the required
values to the context.

Pass before- and after-wrappers to the respective parameters of the `to_service` decorator.

"""

extractor_pool = ExtractorPool()


async def get_start_time(ctx: Context, _, info: WrapperRuntimeInfo):
    start_time = datetime.now()
    ctx.misc[get_wrapper_field(info)] = start_time


@extractor_pool.new_extractor
async def get_service_state(ctx: Context, _, info: WrapperRuntimeInfo):
    start_time = ctx.misc[get_wrapper_field(info)]
    data = {"execution_time": datetime.now() - start_time}
    return StatsRecord.from_context(ctx, info, data)


@to_service(before_wrapper=[get_start_time], after_wrapper=[get_service_state])
async def heavy_service(_):
    await asyncio.sleep(random.randint(0, 2))


actor = Actor(script, ("root", "start"), ("root", "fallback"))

pipeline = Pipeline.from_dict(
    {
        "components": [
            Service(handler=heavy_service),
            Service(handler=to_service(before_wrapper=[get_start_time], after_wrapper=[get_service_state])(actor)),
        ]
    }
)

if __name__ == "__main__":
    args = parse_args()
    stats = StatsStorage.from_uri(args["uri"], table=args["table"])
    stats.add_extractor_pool(extractor_pool)
    pipeline.run()
