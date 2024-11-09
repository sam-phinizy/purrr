import httpx
from prefect import task, flow


@task(log_prints=True)
def get_stars(repo: str):
    url = f"https://api.github.com/repos/{repo}"
    count = httpx.get(url).json()["stargazers_count"]
    print(f"{repo} has {count} stars!")


@flow(name="GitHub Stars")
def github_stars(repos: list[str] = ["prefecthq/prefect", "microsoft/playwright"]):
    for repo in repos:
        get_stars(repo)


if __name__ == "__main__":
    d = github_stars.from_source(
        source="/Users/sphinizy/src/github.com/sam-phinizy/purrr",
        entrypoint="./tests/fixtures/flows/github_stars.py:github_stars",
    )

    d.deploy(
        name="github-stars",
        work_pool_name="test-process-pool",
        cron="5 4 * * *",
    )