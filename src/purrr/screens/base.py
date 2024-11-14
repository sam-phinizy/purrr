from typing import Type, TYPE_CHECKING

from textual import on
from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Header, DataTable, Footer, Input
from textual.message import Message

if TYPE_CHECKING:
    from purrr.tui import PrefectApp


class BaseDetailView(Screen):
    BINDINGS = [
        ("R", "refresh_data()", "Refresh"),
        ("escape", "app.pop_screen()", "Back"),
    ]
    app: "PrefectApp"

    def __init__(self, lookup_value, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.lookup_value = lookup_value

    async def action_refresh_data(self):
        await self.load_data()

    async def load_data(self) -> None:
        raise NotImplementedError


class CustomInput(Input):
    BINDINGS = [
        ("escape", "loose_focus()", "Reset Focus"),
    ]

    class ResetFocus(Message):
        pass

    def action_loose_focus(self):
        self.post_message(self.ResetFocus())


class BaseTableScreen(Screen):
    detail_screen: Type[BaseDetailView]
    app: "PrefectApp"

    BINDINGS = [
        ("R", "refresh_data()", "Refresh"),
        ("D", "open_detail()", "Open Detail"),
        ("S", "sort_by_column()", "Sort By"),
        ("F", "filter_table()", "Filter Table"),
    ]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._sorted_col = None
        self._reverse_sort = False

    def compose(self) -> ComposeResult:
        yield Header()
        yield CustomInput(
            placeholder="Enter clause like `state_name ='Scheduled'`",
            id="filterInput",
            classes="hidden",
        )
        yield DataTable()
        yield Footer()

    async def on_mount(self) -> None:
        table = self.query_one(DataTable)
        self.add_columns(table)
        await self.load_data(table)
        table.focus()

    def add_columns(self, table: DataTable) -> None:
        raise NotImplementedError

    async def load_data(self, table: DataTable) -> None:
        raise NotImplementedError

    async def action_refresh_data(self):
        table = self.query_one(DataTable)
        table.clear()
        await self.load_data(table)

    async def action_filter_table(self):
        filter_input = self.query_one("#filterInput")
        filter_input.toggle_class("hidden")
        if not filter_input.has_class("hidden"):
            filter_input.focus()

    async def action_sort_by_column(self):
        my_table = self.query_one(DataTable)
        _, col_key = my_table.coordinate_to_cell_key(my_table.cursor_coordinate)

        if self._sorted_col == col_key:
            my_table.sort(col_key, reverse=not self._reverse_sort)
            self._reverse_sort = not self._reverse_sort
        else:
            self._sorted_col = col_key
            my_table.sort(col_key)

    async def get_value(self, row_key: str, column_key: str) -> str:
        raise NotImplementedError

    @on(CustomInput.ResetFocus)
    def reset_focus(self):
        self.query_one(DataTable).focus()
