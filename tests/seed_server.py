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


@task(log_prints=True)
def get_stars(repo: str):
    url = f"https://api.github.com/repos/{repo}"
    count = httpx.get(url).json()["stargazers_count"]
    print(f"{repo} has {count} stars!")


@flow(name="GitHub Stars")
def github_stars(repos: List[str] = ["prefecthq/prefect", "microsoft/playwright"]):
    for repo in repos:
        get_stars(repo)


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

    d = await github_stars.from_source(
        source="/Users/sphinizy/src/github.com/sam-phinizy/purrr",
        entrypoint="tests/seed_server.py:github_stars",
    )

    await d.deploy(
        name="github-stars",
        work_pool_name="test-process-pool",
        cron="5 4 * * *",
    )


# run the flow!
if __name__ == "__main__":
    anyio.run(main)
