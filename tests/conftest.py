import os
import sys
from typing import Callable

import pytest
from df_stats import Saver, Stats
from df_stats import collectors as DSC


@pytest.fixture(scope="session")
def data_generator():
    sys.path.insert(0, "../")
    main: Callable
    from examples.collect_stats import main

    yield main


@pytest.fixture(scope="session")
def testing_cfg_dir(tmpdir_factory):
    cfg_dir = tmpdir_factory.mktemp("cfg")
    yield str(cfg_dir)


@pytest.fixture(scope="session")
def testing_file(tmpdir_factory):
    fn = tmpdir_factory.mktemp("data").join("stats.csv")
    return str(fn)


@pytest.fixture(scope="session")
def testing_saver(testing_file):
    yield Saver("csv://{}".format(testing_file))


@pytest.fixture(scope="session")
def testing_dataframe(data_generator, testing_saver):
    stats = Stats(saver=testing_saver, collectors=[DSC.NodeLabelCollector()])
    stats_object: Stats = data_generator(stats, 3)
    stats_object.save()
    yield stats_object.dataframe


@pytest.fixture(scope="session")
def table():
    yield "test"


@pytest.fixture(scope="session")
def PG_uri_string():
    return "postgresql://{}:{}@localhost:5432/{}".format(os.getenv("PG_USERNAME"), os.getenv("PG_PASSWORD"), "test")


@pytest.fixture(scope="session")
def CH_uri_string():
    return "clickhouse://{}:{}@localhost:8123/{}".format(os.getenv("CH_USERNAME"), os.getenv("CH_PASSWORD"), "test")


@pytest.fixture(scope="session")
def MS_uri_string():
    return "mysql+pymysql://{}:{}@localhost:3307/{}".format(os.getenv("MS_USERNAME"), os.getenv("MS_PASSWORD"), "test")
