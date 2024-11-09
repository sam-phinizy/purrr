import pathlib

from prefect import task, Task
from prefect_shell import ShellOperation


@task
def run_pytest(directory: pathlib.Path, pytest_args: str | None = None) -> None:
    """
    Run pytest in a given directory with specified arguments.
    """

    shell_operation = ShellOperation(
        commands=[f"cd {directory}", f"pytest"],
        stream_output=True,
        working_dir=directory,
    )

    with shell_operation:
        shell_process = shell_operation.trigger()
        shell_process.wait_for_completion()
        shell_output = shell_process.fetch_result()

    return shell_output
