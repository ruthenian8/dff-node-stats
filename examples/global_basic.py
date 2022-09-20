import sys
import asyncio
import random
from pathlib import Path

sys.path.insert(0, str(Path(__file__).absolute().parent))

from df_engine.core import Context, Actor
from df_runner import Pipeline, WrapperRuntimeInfo, GlobalWrapperType
from df_stats import Stats, Saver, StatsItem

from _utils import parse_args, script


async def heavy_service(_):
    await asyncio.sleep(random.randint(0, 5))


async def get_pipeline_state(stats: Stats, ctx: Context, _, info: WrapperRuntimeInfo):
    data = {"runtime_state": info["component"]["execution_state"]}
    group_stats = StatsItem.from_context(ctx, info, data)
    stats.data.append(group_stats)
    await stats.save()


def main(args = None):
    if args is None:
        args = parse_args()

    saver = Saver(args["dsn"], table=args["table"])
    stats = Stats(saver=saver, mock_dates=True)
    global_wrapper = stats.get_wrapper(get_pipeline_state)

    actor = Actor(script, ("root", "start"), ("root", "fallback"))

    pipeline_dict = {
        "components": [
            [heavy_service for _ in range(0, 5)],
            actor,
        ],
    }

    pipeline = Pipeline(**pipeline_dict)
    pipeline.add_global_wrapper(GlobalWrapperType.AFTER_ALL, global_wrapper)
    pipeline.run()


if __name__ == "__main__":
    main()
