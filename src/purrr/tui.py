import enum

from textual.app import App

from purrr.client import CachedPrefectClient
from purrr.deployments import DeploymentsScreen
from purrr.flows import FlowsScreen
from purrr.runs import RunsScreen


class Screens(str, enum.Enum):
    DEPLOYMENTS = "deployments"
    FLOWS = "flows"
    RUNS = "runs"


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

    CSS_PATH = "purrr.tcss"
    _client: CachedPrefectClient

    def __init__(self, client=None) -> None:
        super().__init__()
        self._client = client or CachedPrefectClient()

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
