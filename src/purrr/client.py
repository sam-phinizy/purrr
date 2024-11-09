from uuid import UUID
import duckdb
import json

from prefect import get_client
from prefect.client.orchestration import PrefectClient
from prefect.client.schemas.filters import (
    FlowRunFilterState,
    FlowRunFilterStateType,
    FlowRunFilter,
    LogFilterFlowRunId,
    LogFilter,
)
from prefect.client.schemas.responses import DeploymentResponse
from prefect.client.schemas.sorting import FlowRunSort
from prefect.client.schemas import FlowRun, StateType as FlowRunStates
from prefect.exceptions import ObjectNotFound

class CachedPrefectClient:
    def __init__(self):
        self.client = get_client()

    def reset(self):
        self.client = PrefectClient()

    async def get_runs(
        self,
        sort: FlowRunSort = FlowRunSort.START_TIME_DESC,
        state_types: list[FlowRunStates] | None = None,
    ) -> list[FlowRun]:
        if state_types:
            flow_run_filter = FlowRunFilter(
                state=FlowRunFilterState(type=FlowRunFilterStateType(any_=state_types))
            )
        else:
            flow_run_filter = None
        offset = 0
        while True:
            flow_runs = await self.client.read_flow_runs(
                sort=sort, offset=offset, flow_run_filter=flow_run_filter
            )
            offset += 25
            if not flow_runs:
                break

            return flow_runs

    async def get_run_logs(self, run_id: UUID | str) -> str:
        log_filter = LogFilter(flow_run_id=LogFilterFlowRunId(any_=[run_id]))
        logs = await self.client.read_logs(log_filter=log_filter)

        return "\n".join([log.message for log in logs])

    async def get_deployment_by_id(
        self, deployment_id: UUID,
    ) -> DeploymentResponse | None:
        try:
            deployment = await self.client.read_deployment(deployment_id)
        except ObjectNotFound:
            return None

        return deployment


class LocalDuckDB:
    def __init__(self, db_path: str = ":memory:"):
        self.db = duckdb.connect(database=db_path)
        self.create_flow_runs_table()

    def upsert_data(self, table: str, data: dict):
        # Create table if not exists with dynamic columns based on data
        columns = ", ".join([f"{k} VARCHAR" for k in data.keys()])
        self.db.execute(f"CREATE TABLE IF NOT EXISTS {table} ({columns})")

        # Prepare column names and values for upsert
        columns = ", ".join(data.keys())
        placeholders = ", ".join(["?" for _ in data.keys()])
        values = list(data.values())

        # Perform the upsert operation
        self.db.execute(
            f"""
            INSERT OR REPLACE INTO {table} ({columns})
            VALUES ({placeholders})
            """,
            values,
        )

    def upsert_flow_runs(self, flow_runs: list[FlowRun]):
        """
        Upsert multiple Prefect flow runs into the database.
        
        Args:
            flow_runs: A list of Prefect FlowRun objects from prefect.client.schemas.objects
        """

        for flow_run in flow_runs:
            self.upsert_flow_run(flow_run)

    def upsert_flow_run(self, flow_run: FlowRun):
        """
        Upsert a Prefect flow run into the database.
        
        Args:
            flow_run: A Prefect FlowRun object from prefect.client.schemas.objects
        """
        # Convert FlowRun to dict for storage
        flow_run_dict = json.loads(flow_run.model_dump_json())

        
        self.db.execute("""
            INSERT OR REPLACE INTO flow_runs 
            (raw_json, id, name, created, updated, deployment_id, flow_id, state_name, work_pool_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            json.dumps(flow_run_dict),  # raw_json
            str(flow_run.id),  # Convert UUID to string
            flow_run.name,
            flow_run.created,
            flow_run.updated,
            str(flow_run.deployment_id) if flow_run.deployment_id else None,  # Handle optional UUID
            str(flow_run.flow_id),  # Convert UUID to string
            flow_run.state_name or 'Unknown',
            flow_run.work_pool_name
        ])
        

    def create_flow_runs_table(self):    
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS flow_runs (
                raw_json JSON,
                id VARCHAR PRIMARY KEY,  
                name VARCHAR,
                created TIMESTAMP,
                updated TIMESTAMP,
                deployment_id VARCHAR,
                flow_id VARCHAR,
                state_name VARCHAR,
                work_pool_name VARCHAR
            )
        """)

    