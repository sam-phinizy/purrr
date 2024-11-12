from __future__ import annotations

import enum
from typing import Any

from prefect import get_client
from prefect.client.schemas.objects import FlowRun
from textual import on
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import DataTable, Label, Footer, Log, Header, Static, Input

from purrr.screens.base import BaseTableScreen, BaseDetailView
from purrr.screens.deployments import DeploymentDetail


class RunsColumnKeys(str, enum.Enum):
    ID = "ID"
    NAME = "NAME"
    STATE = "STATE"
    START_TIME = "START_TIME"
    END_TIME = "END_TIME"
    DURATION = "DURATION"
    DEPLOYMENT = "DEPLOYMENT"


class RunDetail(BaseDetailView):
    def compose(self) -> ComposeResult:
        yield Header()
        yield Horizontal(
            Vertical(
                Label("Name:", id="flowNameLabel", classes="formLabel"),
                Static(id="flowNameVal"),
                Label("ID", id="flowId", classes="formLabel"),
                Static(id="flowIdVal"),
                Label("State", id="flowState", classes="formLabel"),
                Static(id="flowStateVal"),
                Label("Deployment", id="flowDeployment", classes="formLabel"),
                Static(id="flowDeploymentVal"),
                id="flowDetails",
            ),
            Vertical(Log(highlight=False, id="flowLog")),
        )

        yield Footer()

    async def on_mount(self) -> None:
        label = self.query_one("#flowNameVal")
        row = await self.load_data()
        label = self.query_one("#flowNameVal", expect_type=Static)
        label.update(row.name)
        label = self.query_one("#flowIdVal", expect_type=Static)
        label.update(str(row.id))
        label = self.query_one("#flowStateVal", expect_type=Static)
        label.update(row.state_name or "Unknown")

        if row.deployment_id:
            try:
                deployment = await self.app._client.get_deployment_by_id(
                    row.deployment_id
                )
                label = self.query_one("#flowDeploymentVal", expect_type=Static)
                label.update(deployment.name)
            except ValueError as ve:
                if "Invalid deployment ID" in str(ve):
                    label = self.query_one("#flowDeploymentVal", expect_type=Static)
                    label.update("No Deployment")
                else:
                    raise ve
        else:
            label = self.query_one("#flowDeploymentVal", expect_type=Static)
            label.update("No Deployment")

        logs = await self.app._client.get_logs(self.lookup_value)
        log_widget: Log = self.query_one("#flowLog", expect_type=Log)
        log_widget.write_line(logs)

    async def load_data(self) -> FlowRun:
        async with get_client() as client:
            return await client.read_flow_run(self.lookup_value)


class RunsScreen(BaseTableScreen):
    detail_screen = RunDetail

    def compose(self) -> ComposeResult:
        yield Input(placeholder="Enter SQL filter...", id="filterInput")
        yield from super().compose()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id == "filterInput" and event.input.value:
            self.app.log("filterInput", event.value)
            self.action_filter_data(event.value)

    def action_filter_data(self, filter_query: str) -> None:
        table = self.query_one(DataTable)
        table.clear()
        self.app.log("filter_query", filter_query)

        data = self.app._client.cache.runs.filter(filter_query)
        self.app.log(data)
        for run in data:
            deployment = self._get_deployment_for_run(run)
            self._add_run_to_table(table, run, deployment)

    def add_columns(self, table: DataTable) -> None:
        table.add_column("Name", width=30, key=RunsColumnKeys.NAME)
        table.add_column("Deployment", width=30, key=RunsColumnKeys.DEPLOYMENT)
        table.add_column("Run ID", width=36, key=RunsColumnKeys.ID)
        table.add_column("State", width=20, key=RunsColumnKeys.STATE)
        table.add_column("Start Time", width=20, key=RunsColumnKeys.START_TIME)
        table.add_column("End Time", width=20, key=RunsColumnKeys.END_TIME)

    async def get_value(self, row_key: str, column_key: str) -> str:
        table = self.query_one(DataTable)
        return table.get_cell(row_key, column_key)

    @on(DataTable.CellSelected)
    async def cell_selected(self, selected: DataTable.CellSelected) -> None:
        if selected.cell_key.row_key is None:
            return
        if selected.cell_key.column_key in (RunsColumnKeys.ID, RunsColumnKeys.NAME):
            screen_to_push = RunDetail
        elif (
            selected.value != "-"
            and selected.cell_key.column_key == RunsColumnKeys.DEPLOYMENT
        ):
            screen_to_push = DeploymentDetail
        else:
            screen_to_push = RunDetail

        if selected.cell_key.column_key != RunsColumnKeys.ID:
            lookup_value = await self.get_value(
                selected.cell_key.row_key.value, RunsColumnKeys.ID
            )
        else:
            lookup_value = selected.value

        await self.app.push_screen(screen_to_push(lookup_value))

    async def _get_deployment_for_run(self, run) -> Any | None:
        """Helper method to fetch deployment information for a run."""
        if run.deployment_id:
            try:
                return await self.app._client.get_deployment_by_id(run.deployment_id)
            except ValueError:
                return None
        return None

    async def load_data(self, table: DataTable) -> None:
        runs = await self.app._client.get_runs()
        if runs:
            for run in runs:
                deployment = await self._get_deployment_for_run(run)
                self._add_run_to_table(table, run, deployment)

    def _add_run_to_table(self, table: DataTable, run, deployment=None) -> None:
        """Helper method to add a run to the data table with consistent formatting."""
        table.add_row(
            run.name,
            deployment.name if deployment else "-",
            str(run.id),
            run.state_name if hasattr(run, "state_name") else run.state.type.value,
            str(run.start_time) if run.start_time else "-",
            str(run.end_time) if run.end_time else "-",
            key=str(run.id),
        )
