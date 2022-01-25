import os
import dff_node_stats

import pytest
from sqlalchemy import create_engine

from . import config


@pytest.fixture
def data_generator():
    from .examples.1.collect_stats import main
    yield main


@pytest.fixture(scope="session")
def test_file(tmpdir_factory):
    fn = tmpdir_factory.mktemp("data").join("stats.csv")
    return str(fn)


@pytest.fixture(scope='session')
def test_saver(test_file):
    yield dff_node_stats.Saver("csv://{}".format(test_file))


@pytest.fixture(scope="session")
def PG_connection(scope='session'):
    engine = create_engine("postgresql://{}:{}@{}:{}/{}".format(
        config.PG_USERNAME,
        config.PG_PASSWORD,
        config.HOST,
        config.PG_PORT,
        config.DATABASE
    ))
    connection = engine.connect()
    yield connection
    connection.close()


@pytest.fixture(scope="session")
def CH_connection():
    engine = create_engine("clickhouse://{}:{}@{}:{}/{}".format(
        config.CH_USERNAME,
        config.CH_PASSWORD,
        config.HOST,
        config.CH_PORT,
        config.DATABASE
    ))
    connection = engine.connect()
    yield connection
    connection.close()