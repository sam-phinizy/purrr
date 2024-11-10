import pathlib

from precis.pytest import run_pytest
from prefect import flow

CWD = pathlib.Path()


@flow
def run_ci_flow():
    results = []
    for folder in ["client"]:
        results.append(run_pytest.submit(CWD, f"tests/{folder}"))

    return results


if __name__ == "__main__":
    run_ci_flow()
