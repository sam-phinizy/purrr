import pytest
from datetime import datetime
from uuid import UUID
from purrr.client.runs import RunsCache, FlowRun


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
    result = runs_cache.db.execute("SELECT * FROM flow_runs").description
    column_names = [col[0] for col in result]
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


def test_read_existing_run(runs_cache, sample_flow_run):
    runs_cache.upsert([sample_flow_run])
    retrieved_run = runs_cache.read(sample_flow_run.id)
    assert retrieved_run is not None
    assert retrieved_run.id == sample_flow_run.id
    assert retrieved_run.name == sample_flow_run.name


def test_read_nonexistent_run(runs_cache):
    nonexistent_id = UUID("99999999-9999-9999-9999-999999999999")
    retrieved_run = runs_cache.read(nonexistent_id)
    assert retrieved_run is None


def test_filter_flow_runs(runs_cache, sample_flow_run):
    runs_cache.upsert([sample_flow_run])
    filtered_runs = runs_cache.filter("state_name = 'Running'")
    assert len(filtered_runs) == 1
    assert filtered_runs[0].id == sample_flow_run.id


def test_filter_invalid_query(runs_cache, sample_flow_run, caplog):
    runs_cache.upsert([sample_flow_run])
    filtered_runs = runs_cache.filter("invalid query syntax")
    assert len(filtered_runs) == 0


def test_upsert_multiple_runs(runs_cache, sample_flow_run):
    second_flow_run = FlowRun(
        id=UUID("98765432-9876-5432-9876-987654321098"),
        name="test_flow_2",
        created=datetime.now(),  # type: ignore
        updated=datetime.now(),  # type: ignore
        deployment_id=None,
        flow_id=UUID("11111111-2222-3333-4444-555555555555"),
        state_name="Completed",
        work_pool_name="default",
    )
    runs_cache.upsert([sample_flow_run, second_flow_run])
    result = runs_cache.db.execute("SELECT * FROM flow_runs").fetchall()
    assert len(result) == 2


def test_upsert_update_existing(runs_cache, sample_flow_run):
    runs_cache.upsert([sample_flow_run])
    updated_run = sample_flow_run.copy()
    updated_run.state_name = "Completed"
    runs_cache.upsert([updated_run])
    result = runs_cache.db.execute("SELECT * FROM flow_runs").fetchall()
    assert len(result) == 1
    assert result[0][7] == "Completed"
