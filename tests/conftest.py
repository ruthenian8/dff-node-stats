import os
from uuid import uuid4
from random import choice

import pytest
from df_stats import Saver, StatsRecord


@pytest.fixture(scope="session")  # test saving configs to zip
def testing_cfg_dir(tmpdir_factory):
    cfg_dir = tmpdir_factory.mktemp("cfg")
    yield str(cfg_dir)


@pytest.fixture(scope="session")  # test saving to csv
def testing_file(tmpdir_factory):
    fn = tmpdir_factory.mktemp("data").join("stats.csv")
    return str(fn)


@pytest.fixture(scope="session")
def testing_saver(testing_file):
    yield Saver("csv://{}".format(testing_file))


@pytest.fixture(scope="session")
def testing_items():
    yield [
        StatsRecord(context_id=str(uuid4()), request_id=1, data_key="some_data", data={"duration": 0.0001}),
        StatsRecord(
            context_id=str(uuid4()),
            request_id=1,
            data_key="actor_data",
            data={
                "flow": choice(["one", "two", "three"]),
                "node": choice(["node_1", "node_2", "node_3", "node_4"]),
                "label": ":".join([choice(["one", "two", "three"]), choice(["node_1", "node_2", "node_3", "node_4"])]),
            },
        ),
    ] * 100


@pytest.fixture(scope="session")
def table():
    yield "test"


@pytest.fixture(scope="session")
def PG_uri_string():
    return "postgresql://{}:{}@localhost:5432/{}".format("root", "qwerty", "test")


@pytest.fixture(scope="session")
def CH_uri_string():
    return "clickhouse://{}:{}@localhost:8123/{}".format("root", "qwerty", "test")
