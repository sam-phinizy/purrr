from typing import AsyncGenerator
from uuid import UUID

from prefect import get_client
from prefect.client.orchestration import PrefectClient
from prefect.client.schemas.responses import DeploymentResponse
from textual.app import ComposeResult
from textual.widgets import Label, Footer, DataTable

from purrr.base import BaseDetailView, BaseTableScreen


async def get_deployment(
    prefect_client: PrefectClient, deployment_id: UUID
) -> DeploymentResponse:
    return await prefect_client.read_deployment(deployment_id)


async def get_deployments(
    prefect_client: PrefectClient,
) -> AsyncGenerator[DeploymentResponse, None, None]:
    deployments = await prefect_client.read_deployments()
    for deployment in deployments:
        yield deployment


class DeploymentDetail(BaseDetailView):
    def compose(self) -> ComposeResult:
        yield Label("")
        yield Footer()

    async def load_data(self) -> None:
        deployment = await get_deployment(self.lookup_value)
        self.query_one(Label).update(deployment.name)


class DeploymentsScreen(BaseTableScreen):
    def add_columns(self, table: DataTable) -> None:
        table.add_column("Name", width=30)
        table.add_column("Flow Name", width=30)
        table.add_column("Status", width=20)
        table.add_column("Schedule", width=20)
        table.add_column("Tags", width=20)

    async def load_data(self, table: DataTable) -> None:
        table.clear()
        async for deployment in get_deployments(get_client()):
            table.add_row(
                deployment.name,
                deployment.flow_id,
                str(deployment.status),
                str(deployment.schedules),
                ", ".join(deployment.tags) if deployment.tags else "N/A",
            )
