import uuid

import duckdb
import pytest

from purrr.client.deployments import DeploymentCache
from prefect.client.schemas.responses import DeploymentResponse
from pendulum import DateTime


@pytest.fixture
def db():
    return duckdb.connect(":memory:")


@pytest.fixture
def deployment_cache(db):
    return DeploymentCache(db)


@pytest.fixture
def sample_deployment():
    return DeploymentResponse(
        id=uuid.uuid4(),
        created=DateTime.now(),  # type: ignore
        updated=DateTime.now(),  # type: ignore
        name="test-deployment",
        version="1.0",
        flow_id=uuid.uuid4(),
        paused=False,
        work_pool_name="test-pool",
        work_queue_name="test-queue",
        work_queue_id=uuid.uuid4(),
    )


def test_init_creates_table(db):
    DeploymentCache(db)
    result = db.execute("SELECT * FROM deployments").description
    expected_columns = {
        "id",
        "name",
        "flow_id",
        "paused",
        "work_pool_name",
        "work_queue_name",
        "data",
    }
    assert {col[0] for col in result} == expected_columns


def test_upsert_single_deployment(deployment_cache, sample_deployment):
    deployment_cache.upsert([sample_deployment])
    result = deployment_cache.read(sample_deployment.id)
    assert result.id == sample_deployment.id
    assert result.name == sample_deployment.name
    assert result.flow_id == sample_deployment.flow_id


def test_upsert_multiple_deployments(deployment_cache):
    deployments = [
        DeploymentResponse(
            id=uuid.uuid4(),
            created=DateTime.now(),  # type: ignore
            updated=DateTime.now(),  # type: ignore
            name=f"test-deployment-{i}",
            version="1.0",
            flow_id=uuid.uuid4(),
            paused=False,
            work_pool_name="test-pool",
            work_queue_name="test-queue",
            work_queue_id=uuid.uuid4(),
        )
        for i in range(3)
    ]
    deployment_cache.upsert(deployments)

    for deployment in deployments:
        result = deployment_cache.read(deployment.id)
        assert result.id == deployment.id
        assert result.name == deployment.name


def test_read_nonexistent_deployment(deployment_cache):
    result = deployment_cache.read(uuid.uuid4())
    assert result is None


def test_upsert_updates_existing_deployment(deployment_cache, sample_deployment):
    deployment_cache.upsert([sample_deployment])

    updated_deployment = DeploymentResponse(
        id=sample_deployment.id,
        created=sample_deployment.created,
        updated=DateTime.now(),  # type: ignore
        name="updated-name",
        version="2.0",
        flow_id=sample_deployment.flow_id,
        paused=True,
        work_pool_name="new-pool",
        work_queue_name="new-queue",
        work_queue_id=uuid.uuid4(),
    )

    deployment_cache.upsert([updated_deployment])
    result = deployment_cache.read(sample_deployment.id)

    assert result.name == "updated-name"
    assert result.paused is True
    assert result.work_pool_name == "new-pool"
