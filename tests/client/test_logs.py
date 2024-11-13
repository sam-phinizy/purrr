import pytest
from prefect.client.schemas.objects import Log
from uuid import UUID
import duckdb
from pendulum import DateTime

from purrr.client.logs import LogsCache


@pytest.fixture
def db():
    """Create an in-memory DuckDB database for testing"""
    return duckdb.connect(database=":memory:")


@pytest.fixture
def logs_cache(db):
    """Create a LogsCache instance with the test database"""
    return LogsCache(db)


def test_upsert_new_log(logs_cache):
    """Test inserting a new log entry"""
    # Use pendulum for timestamp creation to match Prefect's Log schema
    timestamp = DateTime.now()
    flow_run_id = UUID("12345678-1234-5678-1234-567812345678")
    log = Log(
        name="test_flow",
        level=20,  # INFO level
        message="Test message",
        timestamp=timestamp,  # type: ignore
        flow_run_id=flow_run_id,
    )

    logs_cache.upsert([log])

    result = logs_cache.db.execute(
        "SELECT * FROM logs WHERE flow_run_id = ?", [str(flow_run_id)]
    ).fetchone()

    assert result[0] == "test_flow"  # name
    assert result[1] == 20  # level
    assert result[2] == "Test message"  # message
    assert result[4] == str(flow_run_id)  # flow_run_id


def test_upsert_duplicate_log(logs_cache):
    """Test that upserting a log with the same timestamp and run IDs replaces the existing log"""
    timestamp = DateTime.now()
    flow_run_id = UUID("12345678-1234-5678-1234-567812345678")

    # Insert initial log
    initial_log = Log(
        name="test_flow",
        level=20,
        message="Initial message",
        timestamp=timestamp,  # type: ignore
        flow_run_id=flow_run_id,
    )
    logs_cache.upsert([initial_log])

    # Insert updated log with same timestamp and flow_run_id
    updated_log = Log(
        name="test_flow",
        level=30,  # WARNING level
        message="Updated message",
        timestamp=timestamp,  # type: ignore
        flow_run_id=flow_run_id,
    )
    logs_cache.upsert([updated_log])

    # Check that only one log exists and it's the updated one
    results = logs_cache.db.execute(
        """
        SELECT COUNT(*) as count,
               MAX(message) as message,
               MAX(level) as level
        FROM logs
        WHERE flow_run_id = ?
        """,
        [str(flow_run_id)],
    ).fetchone()

    assert results[0] == 1  # Only one log should exist
    assert results[1] == "Updated message"
    assert results[2] == 30


def test_upsert_multiple_logs(logs_cache):
    """Test inserting multiple logs at once"""
    base_timestamp = DateTime.now()
    flow_run_id = UUID("12345678-1234-5678-1234-567812345678")

    # Use different timestamps for each log
    logs = [
        Log(
            name="test_flow",
            level=20,
            message=f"Message {i}",
            timestamp=base_timestamp.add(seconds=i),  # type: ignore
            flow_run_id=flow_run_id,
        )
        for i in range(3)
    ]

    logs_cache.upsert(logs)

    result = logs_cache.db.execute(
        "SELECT COUNT(*) FROM logs WHERE flow_run_id = ?", [str(flow_run_id)]
    ).fetchone()

    assert result[0] == 3


def test_upsert_log_without_run_ids(logs_cache):
    """Test inserting a log without flow_run_id and task_run_id"""
    timestamp = DateTime.now()
    log = Log(
        name="test_flow",
        level=20,
        message="Test message",
        timestamp=timestamp,  # type: ignore
    )

    logs_cache.upsert([log])

    result = logs_cache.db.execute(
        "SELECT * FROM logs WHERE flow_run_id IS NULL AND task_run_id IS NULL"
    ).fetchone()

    assert result[0] == "test_flow"
    assert result[1] == 20
    assert result[2] == "Test message"
    # Convert DuckDB datetime to pendulum datetime for comparison
    assert result[4] is None  # flow_run_id
    assert result[5] is None  # task_run_id
