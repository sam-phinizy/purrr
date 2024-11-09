import pytest
from datetime import datetime
import uuid
from prefect.client.schemas.objects import FlowRun, Log, State, StateType
from purrr.client import LocalDuckDB

@pytest.fixture
def db():
    return LocalDuckDB(":memory:")

@pytest.fixture
def sample_flow_run():
    return FlowRun(
        id=uuid.uuid4(),
        name="test-flow-run",
        flow_id=uuid.uuid4(),
        created=datetime.utcnow(),
        updated=datetime.utcnow(),
        deployment_id=uuid.uuid4(),
        work_pool_name="test-pool",
        state=State(type=StateType.COMPLETED, name="Completed"),
        state_name="Completed"
    )

@pytest.fixture
def sample_flow_runs():
    return [
        FlowRun(
            id=uuid.uuid4(),
            name=f"test-flow-run-{i}",
            flow_id=uuid.uuid4(),
            created=datetime.utcnow(),
            updated=datetime.utcnow(),
            deployment_id=uuid.uuid4(),
            work_pool_name="test-pool",
            state=State(type=StateType.COMPLETED, name="Completed"),
            state_name="Completed"
        )
        for i in range(3)
    ]

def test_create_flow_runs_table(db):
    result = db.db.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'flow_runs'
    """).fetchall()
    
    expected_columns = {
        'raw_json': 'JSON',
        'id': 'VARCHAR',
        'name': 'VARCHAR',
        'created': 'TIMESTAMP',
        'updated': 'TIMESTAMP',
        'deployment_id': 'VARCHAR',
        'flow_id': 'VARCHAR',
        'state_name': 'VARCHAR',
        'work_pool_name': 'VARCHAR'
    }
    
    actual_columns = {row[0]: row[1] for row in result}
    assert actual_columns == expected_columns

def test_upsert_flow_run(db, sample_flow_run):
    # Insert a flow run
    db.runs.upsert(sample_flow_run)

    # Verify the flow run was inserted
    result = db.db.execute("SELECT * FROM flow_runs").fetchone()
    assert result is not None
    assert result[1] == str(sample_flow_run.id)
    assert result[2] == sample_flow_run.name

    # Test upsert (update) with same ID
    updated_flow_run = sample_flow_run.model_copy()
    updated_flow_run.name = "updated-name"
    db.runs.upsert(updated_flow_run)

    # Verify there's still only one record but with updated name
    result = db.db.execute("SELECT COUNT(*), name FROM flow_runs GROUP BY name").fetchall()
    assert len(result) == 1
    assert result[0][0] == 1
    assert result[0][1] == "updated-name"

def test_upsert_flow_runs(db, sample_flow_runs):
    # Insert multiple flow runs
    db.runs.upsert_many(sample_flow_runs)

    # Verify all flow runs were inserted
    result = db.db.execute("SELECT COUNT(*) FROM flow_runs").fetchone()
    assert result[0] == len(sample_flow_runs)
    
    # Verify each flow run's data
    for flow_run in sample_flow_runs:
        result = db.db.execute(
            "SELECT * FROM flow_runs WHERE id = ?", 
            [str(flow_run.id)]
        ).fetchone()
        assert result is not None
        assert result[2] == flow_run.name

def test_null_optional_fields(db):
    flow_run = FlowRun(
        id=uuid.uuid4(),
        name="test-flow-run",
        flow_id=uuid.uuid4(),
        created=datetime.utcnow(),
        updated=datetime.utcnow(),
        deployment_id=None,
        work_pool_name=None,
        state_name=None
    )
    
    db.runs.upsert(flow_run)

    result = db.db.execute("""
        SELECT deployment_id, work_pool_name, state_name 
        FROM flow_runs
    """).fetchone()
    
    assert result[0] is None
    assert result[1] is None
    assert result[2] == "Unknown"

def test_logs_crud(db):
    flow_run_id = uuid.uuid4()
    log = Log(
        name="test-log",
        level=20,
        message="Test message",
        timestamp=datetime.utcnow(),
        flow_run_id=flow_run_id
    )

    # Test single log insert
    db.logs.upsert(log)

    # Test reading logs
    logs = db.logs.read_by_flow_run(flow_run_id)
    assert len(logs) == 1
    assert logs[0]['message'] == "Test message"

    # Test multiple logs
    more_logs = [
        Log(
            name="test-log",
            level=20,
            message=f"Test message {i}",
            timestamp=datetime.utcnow(),
            flow_run_id=flow_run_id
        )
        for i in range(3)
    ]

    db.logs.upsert_many(more_logs)
    all_logs = db.logs.read_by_flow_run(flow_run_id)
    assert len(all_logs) == 4
