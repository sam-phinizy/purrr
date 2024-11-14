import pytest
import uuid
from prefect.client.schemas.objects import FlowRun, State, StateType
from purrr.client.main import SQLiteCache
import pendulum


@pytest.fixture
def db():
    return SQLiteCache(":memory:")


@pytest.fixture
def generate_flow_runs():
    def _generate_flow_runs(count: int) -> list[FlowRun]:
        return [
            FlowRun(
                id=uuid.uuid4(),
                name=f"test-flow-run-{i}",
                flow_id=uuid.uuid4(),
                created=pendulum.now(),  # type: ignore
                updated=pendulum.now(),  # type: ignore
                deployment_id=uuid.uuid4(),
                work_pool_name="test-pool",
                state=State(type=StateType.COMPLETED, name="Completed"),
                state_name="Completed",
            )
            for i in range(count)
        ]

    return _generate_flow_runs


def test_create_flow_runs_table(db):
    result = db.db.execute("PRAGMA table_info(flow_runs)").fetchall()

    # PRAGMA returns: (id, name, type, notnull, dflt_value, pk)
    column_info = {row[1]: row[2] for row in result}

    expected_columns = {
        "raw_json": "JSON",
        "id": "TEXT",
        "name": "TEXT",
        "created": "TIMESTAMP",
        "updated": "TIMESTAMP",
        "deployment_id": "TEXT",
        "flow_id": "TEXT",
        "state_name": "TEXT",
        "work_pool_name": "TEXT",
    }

    assert column_info == expected_columns


def test_create_flow_runs_primary_key(db):
    result = db.db.execute("PRAGMA table_info(flow_runs)").fetchall()
    # Find the primary key column
    pk_column = next(row[1] for row in result if row[5] == 1)  # row[5] is the pk flag
    assert pk_column == "id"


def test_upsert_flow_run(db, generate_flow_runs):
    # Insert a flow run
    flow_runs = generate_flow_runs(1)
    db.runs.upsert(flow_runs)

    # Verify the flow run was inserted
    result = db.db.execute("SELECT * FROM flow_runs").fetchone()
    assert result is not None
    assert result[1] == str(flow_runs[0].id)
    assert result[2] == flow_runs[0].name

    # Test upsert (update) with same ID
    updated_flow_run = flow_runs[0].model_copy()
    updated_flow_run.name = "updated-name"
    db.runs.upsert([updated_flow_run])

    # Verify there's still only one record but with updated name
    result = db.db.execute(
        "SELECT COUNT(*), name FROM flow_runs GROUP BY name"
    ).fetchall()
    assert len(result) == 1
    assert result[0][0] == 1
    assert result[0][1] == "updated-name"


def test_upsert_flow_runs(db, generate_flow_runs):
    # Insert multiple flow runs
    flow_runs = generate_flow_runs(3)
    db.runs.upsert(flow_runs)

    # Verify all flow runs were inserted
    result = db.db.execute("SELECT COUNT(*) FROM flow_runs").fetchone()
    assert result[0] == len(flow_runs)

    # Verify each flow run's data
    for flow_run in flow_runs:
        result = db.db.execute(
            "SELECT * FROM flow_runs WHERE id = ?", [str(flow_run.id)]
        ).fetchone()
        assert result is not None
        assert result[2] == flow_run.name


def test_null_optional_fields(db):
    flow_run = FlowRun(
        id=uuid.uuid4(),
        name="test-flow-run",
        flow_id=uuid.uuid4(),
        created=pendulum.now(),  # type: ignore
        updated=pendulum.now(),  # type: ignore
        deployment_id=None,
        work_pool_name=None,
        state_name=None,
    )

    db.runs.upsert([flow_run])

    result = db.db.execute("""
        SELECT deployment_id, work_pool_name, state_name
        FROM flow_runs
    """).fetchone()

    assert result[0] is None
    assert result[1] is None
    assert result[2] == "Unknown"
