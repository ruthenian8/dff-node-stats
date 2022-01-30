import pytest
from dff_node_stats import collectors


def test_inheritance():
    class NewCollector(collectors.Collector):
        @property
        def column_dtypes(self):
            return None

        @property
        def parse_dates(self):
            return None

        def collect_stats(self, ctx, actor, *args, **kwargs):
            return None

    assert issubclass(NewCollector, collectors.Collector)

    new_collector = NewCollector()

    assert isinstance(new_collector, collectors.Collector)
