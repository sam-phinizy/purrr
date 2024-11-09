from purrr.client import CachedPrefectClient
import vcr
import pytest


@pytest.mark.asyncio
@vcr.use_cassette("fixtures/vcr_cassettes/flow_run_data.yaml")
async def test_get_runs():
    client = CachedPrefectClient()
    runs = await client.get_runs()
    assert len(runs) == 13


@pytest.mark.asyncio
@vcr.use_cassette("fixtures/vcr_cassettes/get_deployment.yaml")
async def test_get_deployment():
    client = CachedPrefectClient()
    runs = await client.get_runs()

    deployments = await client.get_deployment_by_id(runs[0].id)
