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
    await asyncio.sleep(random.randint(0, 5))


async def get_start_time(stats: Stats, ctx: Context, _, info: WrapperRuntimeInfo):
    start_time = datetime.now()
    ctx.misc[get_wrapper_field(info, "time")] = start_time


async def get_group_state(stats: Stats, ctx: Context, _, info: WrapperRuntimeInfo):
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
    wrapper = stats.get_wrapper((get_start_time, get_group_state))

    actor = Actor(script, ("root", "start"), ("root", "fallback"))
    pipeline = Pipeline.from_dict(
        {
            "components": [
                ServiceGroup(
                    wrappers=[wrapper],
                    components=[{"handler": heavy_service}, {"handler": heavy_service}],
                ),
                actor,
            ],
        }
    )
    pipeline.run()


if __name__ == "__main__":
    main()
