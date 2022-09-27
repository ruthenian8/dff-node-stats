import sys
import asyncio, random
from pathlib import Path

sys.path.insert(0, str(Path(__file__).absolute().parent))

from df_engine.core import Context, Actor
from df_runner import Pipeline, ServiceGroup, WrapperRuntimeInfo
from df_stats import Stats, StatsItem, Saver

from _utils import parse_args, script


async def heavy_service(_):
    await asyncio.sleep(random.randint(0, 2))


"""
Wrappers can be applied to any pipeline parameter, including service groups.
The `ServiceGroup` constructor has `before_wrapper` and `after_wrapper` parameters, 
to which wrapper functions can be passed.

"""


async def get_group_stats(stats: Stats, ctx: Context, _, info: WrapperRuntimeInfo):
    data = {"runtime_state": info["component"]["execution_state"]}
    group_stats = StatsItem.from_context(ctx, info, data)
    stats.data.append(group_stats)
    await stats.save()


def get_pipeline(args) -> Pipeline:
    saver = Saver(args["dsn"], table=args["table"])
    stats = Stats(saver=saver)
    wrapper = stats.get_wrapper(get_group_stats)

    actor = Actor(script, ("root", "start"), ("root", "fallback"))

    pipeline = Pipeline.from_dict(
        {
            "components": [
                ServiceGroup(
                    after_wrapper=[wrapper],
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
