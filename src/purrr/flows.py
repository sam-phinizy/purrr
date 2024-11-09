from __future__ import annotations

from typing import Generator

from prefect import get_client
from prefect.client.orchestration import PrefectClient
from prefect.client.schemas import FlowRun
from prefect.client.schemas.objects import Flow
from sqlitedict import SqliteDict
from textual.app import ComposeResult
from textual.widgets import DataTable, Label, Footer

from purrr.base import BaseTableScreen, BaseDetailView


async def get_flows(
    prefect_client: PrefectClient,
) -> Generator[Flow, None, None]:
    flows = await prefect_client.read_flows()

    for flow in flows:
        yield flow


class FlowDetail(BaseDetailView):
    def compose(self) -> ComposeResult:
        yield Label("")
        yield Footer()

    async def on_mount(self) -> None:
        label = self.query_one(Label)
        row = await self.load_data()
        label.update(f"Flow Name: {row.name}")

    async def load_data(self) -> FlowRun:
        async with get_client() as client:
            return await client.read_flow_run(self.lookup_value)


class FlowsScreen(BaseTableScreen):
    detail_screen = FlowDetail

    def add_columns(self, table: DataTable) -> None:
        table.add_column("Flow ID", width=36)
        table.add_column("Name", width=30)
        table.add_column("Created", width=20)
        table.add_column("Tags", width=20)

    async def get_value(self, selected: DataTable.CellSelected) -> str:
        table = self.query_one(DataTable)
        return table.get_cell(selected.cell_key.row_key, selected.cell_key.column_key)

    async def load_data(self, table: DataTable) -> None:
        async for flow in get_flows(get_client()):
            table.add_row(
                str(flow.id),
                flow.name,
                str(flow.created),
                ", ".join(flow.tags) if flow.tags else "N/A",
            )
