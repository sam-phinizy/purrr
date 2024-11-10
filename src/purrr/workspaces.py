from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Header, Footer, RadioSet, RadioButton, Button

from purrr.base import BaseDetailView


class WorkspaceScreen(Screen):
    def compose(self) -> ComposeResult:
        yield Header()
        with RadioSet():
            yield RadioButton("Battlestar Galactica")
            yield RadioButton("Dune 1984")
            yield RadioButton("Dune 2021", id="focus_me")
            yield RadioButton("Serenity", value=True)
            yield RadioButton("Star Trek: The Motion Picture")
            yield RadioButton("Star Wars: A New Hope")
            yield RadioButton("The Last Starfighter")
            yield RadioButton(
                "Total Recall :backhand_index_pointing_right: :red_circle:"
            )
            yield RadioButton("Wing Commander")
        yield Button("Select Workspace", variant="primary")
        yield Footer()

    def on_mount(self) -> None:
        self.query_one(RadioSet).focus()


class WorkspaceDetailView(BaseDetailView):
    pass
