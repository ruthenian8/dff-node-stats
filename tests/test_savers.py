import sys
import pathlib
import socket

try:
    import sqlalchemy
    from infi import clickhouse_orm
except ImportError:
    pass
import pytest
from dff_node_stats import Saver, Stats


def ping_localhost(port: int, timeout=3):
    try:
        socket.setdefaulttimeout(timeout)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(("localhost", port))
    except OSError as error:
        return False
    else:
        s.close()
        return True


POSTGRES_ACTIVE = ping_localhost(5432)

CLICKHOUSE_ACTIVE = ping_localhost(8123)


def test_uri():
    with pytest.raises(ValueError) as error:
        saver = Saver("erroneous")
    assert "Saver should be initialized" in str(error.value)
    with pytest.raises(ValueError) as error:
        saver = Saver("mysql://auth")
    assert "Cannot recognize option" in str(error.value)


def test_uri_priority():
    saver1 = Saver("csv://file.csv")
    saver2 = Saver("csv://file2.csv")
    assert saver2.path == pathlib.Path("file2.csv")


def test_saver_registry():
    class MongoSaver(Saver, storage_type="mongo"):
        pass

    assert Saver._saver_mapping.get("mongo") == "MongoSaver"


@pytest.mark.skipif(POSTGRES_ACTIVE is False, reason="Postgres not available")
@pytest.mark.skipif("sqlalchemy" not in sys.modules, reason="Postgres extra not installed")
def test_PG_saving(PG_uri_string, data_generator, table):
    stats = Stats(saver=Saver(PG_uri_string, table=table))
    if sqlalchemy.inspect(stats.saver.engine).has_table(table):
        stats.saver.engine.execute(f"TRUNCATE {table}")
    stats_object = data_generator(stats, 3)
    initial_cols = set(stats_object.dfs[0].columns)
    stats_object.save()
    result = stats.saver.engine.execute(f"SELECT COUNT(*) FROM {table}")
    first = result.first()
    assert int(first[0]) > 0
    df = stats_object.dataframe
    assert set(df.columns) == initial_cols


@pytest.mark.skipif(CLICKHOUSE_ACTIVE is False, reason="Clickhouse not available")
@pytest.mark.skipif(
    ("infi" not in sys.modules or "sqlalchemy" not in sys.modules), reason="Clickhouse extra not installed"
)
def test_CH_saving(CH_uri_string, data_generator, table):
    stats = Stats(saver=Saver(CH_uri_string, table=table))
    if stats.saver.db.does_table_exist(type(table, (clickhouse_orm.Model,), {})):
        stats.saver.db.raw(f"TRUNCATE {table}")
    stats_object = data_generator(stats, 3)
    initial_cols = set(stats_object.dfs[0].columns)
    stats_object.save()
    result = stats.saver.db.raw(f"SELECT COUNT (*) FROM {table}")
    assert int(result) > 0
    df = stats_object.dataframe
    assert set(df.columns) == initial_cols
