from prefect import task, flow
from prefect_shell import ShellOperation


@task(task_run_name="run-pre-commit")
def run_pre_commit():
    shell_operation = ShellOperation(
        commands=["pre-commit run --all-files"],
        stream_output=True,
    )
    with shell_operation:
        shell_process = shell_operation.trigger()
        shell_process.wait_for_completion()
        shell_output = shell_process.fetch_result()

    return shell_output


@flow(name="run-pre-commit-flow")
def run_pre_commit_flow():
    return run_pre_commit.submit()
