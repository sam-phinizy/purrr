from typing import List

import anyio
import httpx
import prefect
from plumbum import local
from prefect import flow, task
from prefect import get_client
from prefect.client.schemas.actions import WorkPoolCreate

prefect_cli = local["prefect"]

prefect_client = get_client()


async def main():
    try:
        await get_client().create_work_pool(
            WorkPoolCreate(
                name="test-process-pool",
                type="process",
            )
        )
    except prefect.exceptions.ObjectAlreadyExists:
        pass

if __name__ == "__main__":
    anyio.run(main)
