import pathlib

from precis.pytest import run_pytest
from prefect import flow

CWD = pathlib.Path()


@flow
def run_ci_flow():
    run_pytest(CWD)


if __name__ == "__main__":
    run_ci_flow()
