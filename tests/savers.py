import pytest
from dff_node_stats.savers import Saver


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
    assert saver2.path == "file2.csv"


def test_saver_registry():
    class MongoSaver(Saver, _id="mongo"):
        def __init__(self, path) -> None:
            if not hasattr(self, "path"):
                self.path = path
            return

        def save(self):
            return

        def load(self):
            return


    assert issubclass(MongoSaver, Saver)

    mongo_saver = Saver("mongo://mongoaddress")

    assert isinstance(mongo_saver, Saver)
    assert isinstance(mongo_saver, MongoSaver)
    assert mongo_saver.path == "mongo://mongoaddress"