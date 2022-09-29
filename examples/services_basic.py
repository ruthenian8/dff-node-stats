import sys
import asyncio
import random
from pathlib import Path

sys.path.insert(0, str(Path(__file__).absolute().parent))

from df_engine.core import Context, Actor
from df_runner import Pipeline, Service, WrapperRuntimeInfo, to_service
from df_stats import StatsStorage, ExtractorPool, StatsRecord

from _utils import parse_args, script

"""
To collect statistics from a service, regardless of its type, 
you should define a processing function as a wrapper and use it to wrap the target service.

These function signature equals that of regular Wrappers from df_pipeline with the exception
that the regular arguments go after the first argument of type :py:class:`~StatsStorage`.
This signature allows you to use explicit calls to :py:class:`~StatsStorage`. methods inside the functions.

Most of the time, you'll want to append the data you have collected to the `data` attribute and
then await the :py:meth:`save` method.

The data should always be collected as an instance of the :py:class:`~StatsRecord` class.
For your convenience, the latter offers the `from_context` class method, 
to which you can pass an arbitrary dict of data as the `data` parameter.

To add a wrapper to a concrete service, pass the wrapper to the `to_service` decorator.

"""

extractor_pool = ExtractorPool()


async def heavy_service(_):
    await asyncio.sleep(random.randint(0, 2))


@extractor_pool.new_extractor
async def get_service_state(ctx: Context, _, info: WrapperRuntimeInfo):
    data = {
        "execution_state": info["component"]["execution_state"],
    }
    return StatsRecord.from_context(ctx, info, data)


actor = Actor(script, ("root", "start"), ("root", "fallback"))

pipeline = Pipeline.from_dict(
    {
        "components": [
            Service(handler=to_service(after_wrapper=[get_service_state])(heavy_service)),
            Service(handler=to_service(after_wrapper=[get_service_state])(actor)),
        ]
    }
)

if __name__ == "__main__":
    args = parse_args()
    stats = StatsStorage.from_uri(args["uri"], table=args["table"])
    stats.add_extractor_pool(extractor_pool)
    pipeline.run()
