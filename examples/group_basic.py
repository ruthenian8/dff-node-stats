import sys
import asyncio, random
from pathlib import Path

sys.path.insert(0, str(Path(__file__).absolute().parent))

from df_engine.core import Context, Actor
from df_runner import Pipeline, ServiceGroup, WrapperRuntimeInfo
from df_stats import Stats, StatsItem, Saver

from _utils import parse_args, script


async def heavy_service(_):
    await asyncio.sleep(random.randint(0, 5))


"""
Let's suppose we want to save the runtime states of services to the database.
"""


async def get_group_stats(stats: Stats, ctx: Context, _, info: WrapperRuntimeInfo):
    data = {"runtime_state": info["component"]["execution_state"]}
    group_stats = StatsItem.from_context(ctx, info, data)
    stats.data.append(group_stats)
    await stats.save()


def main(args = None):
    if args is None:
        args = parse_args()

    saver = Saver(args["dsn"], table=args["table"])
    stats = Stats(saver=saver, mock_dates=True)
    wrapper = stats.get_wrapper(get_group_stats)

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
