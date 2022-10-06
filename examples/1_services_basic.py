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
The statistics are collected from services by wrapping them in special 'extractor' functions.
These functions have a specific signature: their arguments are always a `Context`, an `Actor`, 
and a `WrapperRuntimeInfo`. Their return value is always a `StatsRecord` instance. 
It is a preferred practice to define them as asynchronous.

Before you use the said functions, you should create an `ExtractorPool` 
or import a ready one as a first step.

Then, you should define the wrappers and add them to some pool, using the `new_extractor` method.
The latter can be called by decorating the function (see below).

Finally, one should also create a `StatsStorage`, which compresses data into batches
and saves it to a database. The database credentials can be configured by either
instantiating a `Saver` class and passing it on construction, or by
passing the database credentials to the `from_uri` class method.

When this is done, subscribe the storage to one or more pools that you have created 
by calling the `add_extractor_pool` method.

The whole process is illustrated in the example below.

"""

# Create a pool.
extractor_pool = ExtractorPool()


# Create an extractor and add it to the pool.
@extractor_pool.new_extractor
async def get_service_state(ctx: Context, _, info: WrapperRuntimeInfo):
    # extract execution state of service from info
    data = {
        "execution_state": info["component"]["execution_state"],
    }
    # return a record to save into connected database
    return StatsRecord.from_context(ctx, info, data)


@to_service(after_wrapper=[get_service_state]) # set get_service_state to rubn it after the `heavy_service`
async def heavy_service(ctx: Context, actor: Actor):
    _ = ctx # get something from ctx if it needs
    _ = actor # get something from actor if it needs
    await asyncio.sleep(random.randint(0, 2))


actor = Actor(script, ("root", "start"), ("root", "fallback"))

pipeline = Pipeline.from_dict(
    {
        "components": [
            Service(handler=heavy_service),
            Service(handler=to_service(after_wrapper=[get_service_state])(actor)),
        ]
    }
)

if __name__ == "__main__":
    args = parse_args()

    # Create a storage object.
    stats = StatsStorage.from_uri(args["uri"], table=args["table"])

    # Subscribe the storage to the changes in the pool.
    stats.add_extractor_pool(extractor_pool)
    pipeline.run()
