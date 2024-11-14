import duckdb
import pytest


@pytest.fixture
def db():
    return duckdb.connect(":memory:")
