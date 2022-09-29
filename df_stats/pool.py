import functools
import asyncio
from typing import List, Callable, Optional, Protocol

from df_engine.core import Context
from df_runner import WrapperRuntimeInfo
from .record import StatsRecord

# TODO: move to another module for inheriting by storage
class PoolSubscriber(Protocol):
    # TODO: maybe get_new_record_event ?
    def on_new_record(self, record: StatsRecord):
        raise NotImplementedError


class ExtractorPool:
    """
    This class can be used to store sets of wrappers for statistics collection, aka extractors.
    New wrappers can be added with the :py:meth:`new_extractor` decorator.
    The added wrappers can be accessed by their name:

    .. code: python

        pool[extractor.__name__]

    After execution, the result of each wrapper will be propagated to subscribers.
    Subscribers can belong to any class, given that they implement the `on_new_record` method.
    Currently, this method exists in the :py:class:`StatsStorage` class.

    When you call the `add_extractor_pool` method on the `StatsStorage`, you subscribe it
    to changes in the given pool.

    Parameters
    -----------

    extractors: Optional[List[Callable]]
        You can pass a set of wrappers as a list on the class construction.
        They will be registered as normal.

    """

    def __init__(self, extractors: Optional[List[Callable]] = None):
        self.subscribers: List[PoolSubscriber] = []
        if extractors is not None:
            self.extractors = {item.__name__: self.wrap_extractor(item) for item in extractors} # TODO: check if it is callable
        else:
            self.extractors = {}

    # TODO: add underscore if ti's private
    def wrap_extractor(self, extractor: Callable) -> Callable:
        @functools.wraps(extractor)
        async def extractor_wrapper(ctx: Context, _, info: WrapperRuntimeInfo):
            if asyncio.iscoroutinefunction(extractor):
                result = await extractor(ctx, _, info)
            else:
                result = extractor(ctx, _, info)

            if result is None:
                return result

            for stats_storage in self.subscribers: # TODO: bad naming for stats_storage, use subscriber
                await stats_storage.on_new_record(result)
            return result

        return extractor_wrapper

    def new_extractor(self, extractor: Callable) -> Callable:
        wrapped_extractor = self.wrap_extractor(extractor)
        self.extractors[extractor.__name__] = wrapped_extractor
        return self.extractors[extractor.__name__]

    def __getitem__(self, key: str):
        return self.extractors.__getitem__(key)


# TODO: move to another module
default_extractor_pool = ExtractorPool()


# TODO: add more, for example exec timing
@default_extractor_pool.new_extractor
async def get_default_actor_data(ctx: Context, _, info: WrapperRuntimeInfo): # TODO: bad naming extract_current_label
    last_label = ctx.last_label or ("", "")
    default_data = StatsRecord.from_context(
        ctx, info, {"flow": last_label[0], "node": last_label[1], "label": ": ".join(last_label)}
    )
    return default_data
