import sys
import asyncio
import random
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).absolute().parent))

from df_engine.core import Context, Actor
from df_runner import Pipeline, Service, WrapperRuntimeInfo
from df_stats import Stats, Saver, StatsItem, get_wrapper_field

from _utils import parse_args, script

"""

"""


async def heavy_service(_):
    await asyncio.sleep(random.randint(0, 5))


async def get_start_time(stats: Stats, ctx: Context, _, info: WrapperRuntimeInfo):
    start_time = datetime.now()
    ctx.misc[get_wrapper_field(info, "time")] = start_time


async def get_service_state(stats: Stats, ctx: Context, _, info: WrapperRuntimeInfo):
    start_time = ctx.misc[get_wrapper_field(info, "time")]
    data = {"execution_state": info["component"]["execution_state"], "execution_time": datetime.now() - start_time}
    group_stats = StatsItem.from_context(ctx, info, data)
    stats.data.append(group_stats)
    await stats.save()


def main(args = None):
    if args is None:
        args = parse_args()

    saver = Saver(args["dsn"], table=args["table"])
    stats = Stats(saver=saver, mock_dates=True)
    wrapper = stats.get_wrapper((get_start_time, get_service_state))

    actor = Actor(script, ("root", "start"), ("root", "fallback"))
    pipeline = Pipeline.from_dict(
        {"components": [Service(handler=heavy_service, wrappers=[wrapper]), Service(handler=actor, wrappers=[wrapper])]}
    )
    pipeline.run()


if __name__ == "__main__":
    main()
