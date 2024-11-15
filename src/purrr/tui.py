import enum

from textual.app import App

from purrr.client import CachingPrefectClient
from purrr.screens.deployments import DeploymentsScreen
from purrr.screens.flows import FlowsScreen
from purrr.screens.runs import RunsScreen
from textual.command import Hit, Provider


class Screens(str, enum.Enum):
    DEPLOYMENTS = "deployments"
    FLOWS = "flows"
    RUNS = "runs"


class PrefectAppCommands(Provider):
    def show_runs_screen(self) -> None:
        """Show the runs screen"""
        self.app.switch_screen(Screens.RUNS)

    def discover_commands(self):
        yield Hit(1, "Show Runs", self.show_runs_screen)

    def search(self, query: str):
        yield Hit(1, "Show Runs", self.show_runs_screen)


class PrefectApp(App):
    """A Textual app to display Prefect deployments, flows, and flow runs."""

    BINDINGS = [
        ("d", "show_deployments", "Show Deployments"),
        ("f", "show_flows", "Show Flows"),
        ("r", "show_flow_runs", "Show Runs"),
        ("q", "quit", "Quit"),
        ("ctrl-w", "switch_workspace", "Switch Workspace"),
    ]

    SCREENS = {
        Screens.DEPLOYMENTS: DeploymentsScreen,
        Screens.FLOWS: FlowsScreen,
        Screens.RUNS: RunsScreen,
    }

    COMMANDS = App.COMMANDS | {PrefectAppCommands}

    CSS_PATH = "purrr.tcss"
    _client: CachingPrefectClient

    def __init__(self, client=None) -> None:
        super().__init__()
        self._client = client or CachingPrefectClient()

    def on_mount(self) -> None:
        self.push_screen(Screens.RUNS)

    def switch_workspace(self) -> None:
        self.push_screen(Screens.RUNS)

    def action_show_deployments(self) -> None:
        self.switch_screen(Screens.DEPLOYMENTS)

    def action_show_flows(self) -> None:
        self.switch_screen(Screens.FLOWS)

    def action_show_flow_runs(self) -> None:
        self.switch_screen(Screens.RUNS)


def entrypoint():
    app = PrefectApp()
    app.run()


if __name__ == "__main__":
    entrypoint()
