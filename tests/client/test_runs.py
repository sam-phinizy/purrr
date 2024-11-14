import pytest
from datetime import datetime
from uuid import UUID
from prefect.client.schemas.objects import FlowRun

from purrr.client.runs import RunsCache


@pytest.fixture
def runs_cache(db):
    return RunsCache(db)


@pytest.fixture
def sample_flow_run():
    return FlowRun(
        id=UUID("12345678-1234-5678-1234-567812345678"),
        name="test_flow",
        created=datetime.now(),  # type: ignore
        updated=datetime.now(),  # type: ignore
        deployment_id=UUID("87654321-4321-8765-4321-876543210987"),
        flow_id=UUID("11111111-2222-3333-4444-555555555555"),
        state_name="Running",
        work_pool_name="default",
    )


def test_create_table(runs_cache):
    result = runs_cache.db.execute("PRAGMA table_info(flow_runs)").fetchall()
    column_names = [col[1] for col in result]
    expected_columns = [
        "raw_json",
        "id",
        "name",
        "created",
        "updated",
        "deployment_id",
        "flow_id",
        "state_name",
        "work_pool_name",
    ]
    assert all(col in column_names for col in expected_columns)


def test_upsert_flow_run(runs_cache, sample_flow_run):
    runs_cache.upsert([sample_flow_run])
    result = runs_cache.db.execute("SELECT * FROM flow_runs").fetchall()
    assert len(result) == 1
    assert result[0][1] == str(sample_flow_run.id)
    assert result[0][2] == sample_flow_run.name


def test_filter_valid_query(runs_cache, sample_flow_run):
    runs_cache.upsert([sample_flow_run])
    filtered_runs = runs_cache.filter("state_name = 'Running'")
    assert len(filtered_runs) == 1
    assert filtered_runs[0].id == sample_flow_run.id


def test_filter_invalid_query(runs_cache, sample_flow_run, caplog):
    runs_cache.upsert([sample_flow_run])
    filtered_runs = runs_cache.filter("invalid query syntax")
    assert len(filtered_runs) == 0
    assert "SQLite error:" in caplog.text


def test_filter_no_results(runs_cache, sample_flow_run):
    runs_cache.upsert([sample_flow_run])
    filtered_runs = runs_cache.filter("state_name = 'Completed'")
    assert len(filtered_runs) == 0


def test_read_flow_run(runs_cache, sample_flow_run):
    runs_cache.upsert([sample_flow_run])
    result = runs_cache.read(sample_flow_run.id)
    assert result is not None
    assert result.id == sample_flow_run.id
    assert result.name == sample_flow_run.name


def test_read_nonexistent_flow_run(runs_cache):
    result = runs_cache.read(UUID("00000000-0000-0000-0000-000000000000"))
    assert result is None
