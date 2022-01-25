from multiprocessing import Process
from typing import Dict
import pytest
import requests

from dff_node_stats.api import api_run


@pytest.fixture(scope="module")
def host():
    yield "localhost"


@pytest.fixture(scope="module")
def port():
    yield 8000


@pytest.fixture(scope="module")
def endpoint():
    yield "customendpoint"


def customize(app, endpoint):
    @app.get(f"/{endpoint}", response_model=Dict[str, str])
    async def custom_route():
        return [{"foo": "bar"}, {"baz": "qux"}]

    return app


@pytest.fixture(scope="function")
def custom_API(host, port):
    proc = Process(target=api_run, args=(), daemon=True)
    yield
    proc.kill()


@pytest.fixture(scope="function")
def default_API(host, port):
    proc = Process(target=api_run, args=(), daemon=True)
    yield
    proc.kill()


def test_custom(default_API, host, port, endpoint):
    response = requests.get(f"http://{host}:{port}/{endpoint}")
    code = response.code
    data = response.json()
    assert code == 200
    assert len(data) > 0
    first = data[0]
    assert len(first) > 0
    assert isinstance(first, dict)
    assert first["foo"] == "bar"


def test_default_transition_counts(default_API, host, port):
    response = requests.get(f"http://{host}:{port}/api/v1/stats/transition-counts")
    code = response.code
    data = response.json()
    assert code == 200
    assert len(data) > 0
    first = data[0]
    assert len(first) > 0
    assert isinstance(first, dict)


def test_default_transition_probs(default_API, host, port):
    response = requests.get(f"http://{host}:{port}/api/v1/stats/transition-probs")
    code = response.code
    data = response.json()
    assert code == 200
    assert len(data) > 0
    first = data[0]
    assert len(first) > 0
    assert isinstance(first, dict)
