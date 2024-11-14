from datetime import datetime
import pytest
from freezegun import freeze_time
from purrr.client.main import DuckDBCache


@pytest.fixture
def db_cache(tmp_path):
    """Create a temporary DuckDBCache instance for testing."""
    db_path = str(tmp_path / "test.db")
    return DuckDBCache(db_path)


@freeze_time("2024-01-01 12:00:00")
def test_log_execution_creates_new_entry(db_cache):
    """Test that logging a new function execution creates an entry."""
    db_cache.log_execution("test_function", True)

    result = db_cache.db.execute("""
        SELECT * FROM purrr_metadata
        WHERE function_name = 'test_function'
    """).fetchone()

    assert result is not None
    assert result[0] == "test_function"
    assert result[1] == datetime(2024, 1, 1, 12, 0, 0)
    assert result[2] is True


def test_log_execution_updates_existing_entry(db_cache):
    """Test that logging the same function multiple times updates the existing entry."""
    # First execution
    with freeze_time("2024-01-01 12:00:00"):
        db_cache.log_execution("test_function", True)
        first_result = db_cache.db.execute("""
            SELECT * FROM purrr_metadata
            WHERE function_name = 'test_function'
        """).fetchone()

    # Second execution
    with freeze_time("2024-01-01 12:01:00"):
        db_cache.log_execution("test_function", False)
        second_result = db_cache.db.execute("""
            SELECT * FROM purrr_metadata
            WHERE function_name = 'test_function'
        """).fetchone()

    # Check first execution
    assert first_result[0] == "test_function"
    assert first_result[1] == datetime(2024, 1, 1, 12, 0, 0)
    assert first_result[2] is True

    # Check second execution
    assert second_result[0] == "test_function"
    assert second_result[1] == datetime(2024, 1, 1, 12, 1, 0)
    assert second_result[2] is False


@freeze_time("2024-01-01 12:00:00")
def test_log_execution_multiple_functions(db_cache):
    """Test that logging different functions creates separate entries."""
    db_cache.log_execution("function1", True)
    db_cache.log_execution("function2", False)

    results = db_cache.db.execute("""
        SELECT function_name, time_executed, success
        FROM purrr_metadata
        ORDER BY function_name
    """).fetchall()

    assert len(results) == 2

    assert results[0][0] == "function1"
    assert results[0][1] == datetime(2024, 1, 1, 12, 0, 0)
    assert results[0][2] is True

    assert results[1][0] == "function2"
    assert results[1][1] == datetime(2024, 1, 1, 12, 0, 0)
    assert results[1][2] is False


@freeze_time("2024-01-01 12:00:00")
def test_log_execution_exact_timestamp(db_cache):
    """Test that the timestamp is exactly what we expect."""
    db_cache.log_execution("test_function", True)

    result = db_cache.db.execute("""
        SELECT time_executed FROM purrr_metadata
        WHERE function_name = 'test_function'
    """).fetchone()

    assert result[0] == datetime(2024, 1, 1, 12, 0, 0)
