import pathlib

from precis.pytest import run_pytest
from precis.pre_commit import run_pre_commit
from prefect import flow

CWD = pathlib.Path()


@flow
def run_ci_flow():
    results = []
    for folder in ["client", "app"]:
        results.append(run_pytest.submit(CWD, f"tests/{folder}"))

    results.append(run_pre_commit.submit())

    return results


if __name__ == "__main__":
    run_ci_flow()
