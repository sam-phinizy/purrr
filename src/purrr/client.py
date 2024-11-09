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
from prefect.client.schemas.objects import TERMINAL_STATES, Log, FlowRun, StateType as FlowRunStates
from prefect.exceptions import ObjectNotFound

class CachedPrefectClient:
    def __init__(self):
        self.client = get_client()
        self.db = LocalDuckDB("test.db")

    def reset(self):
        self.client = PrefectClient()
    
    async def get_runs(
        self,
        sort: FlowRunSort = FlowRunSort.START_TIME_DESC,
        state_types: list[FlowRunStates] | None = None,
    ) -> list[FlowRun]:
        """Get all flow runs, optionally filtered by state types.
        
        Args:
            sort: How to sort the flow runs
            state_types: Optional list of state types to filter by
            
        Returns:
            List of flow runs
        """
        if state_types:
            flow_run_filter = FlowRunFilter(
                state=FlowRunFilterState(type=FlowRunFilterStateType(any_=state_types))
            )
        else:
            flow_run_filter = None
            
        all_flow_runs = []
        offset = 0
        
        while True:
            flow_runs = await self.client.read_flow_runs(
                sort=sort, offset=offset, flow_run_filter=flow_run_filter
            )
            if not flow_runs:
                break
                
            for flow_run in flow_runs:
                self.db.upsert_flow_run(flow_run)
                
            all_flow_runs.extend(flow_runs)
            offset += len(flow_runs)
            
        return all_flow_runs
        
    async def get_run_by_id(self, run_id: UUID | str, force_refresh: bool = False) -> FlowRun | None:
        """Get a flow run by ID, optionally forcing a refresh from the remote server.
        
        Args:
            run_id: The ID of the flow run to retrieve
            force_refresh: If True, skip cache and fetch from server. Defaults to False.
            
        Returns:
            The flow run if found, None if not found
        """
        try:
            # If force refresh requested, bypass cache and fetch from server
            if force_refresh:
                flow_run = await self._fetch_and_cache_flow_run(run_id)
                return flow_run

            # Try to get from cache first
            cached_run = self.db.get_flow_run(run_id)
            if cached_run and cached_run.state_name in TERMINAL_STATES:
                return cached_run

            # Cache miss or non-terminal state, fetch from server
            flow_run = await self._fetch_and_cache_flow_run(run_id)
            return flow_run

        except ObjectNotFound:
            return None

    async def _fetch_and_cache_flow_run(self, run_id: UUID | str) -> FlowRun:
        """Helper to fetch a flow run from server and cache it."""
        flow_run = await self.client.read_flow_run(run_id)
        self.db.upsert_flow_run(flow_run)
        return flow_run

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
        self.create_logs_table()

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

    def get_flow_run(self, run_id: UUID | str) -> FlowRun | None:
        result = self.db.execute(f"SELECT * FROM flow_runs WHERE id = '{run_id}'").fetchall()
        if result:
            return FlowRun(**json.loads(result[0][0]))
        return None

    def create_logs_table(self):
        """Create the logs table if it doesn't exist."""
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                name VARCHAR,
                level INTEGER,
                message VARCHAR,
                timestamp TIMESTAMP,
                flow_run_id VARCHAR,  -- Store UUID as string
                task_run_id VARCHAR,  -- Store UUID as string
                PRIMARY KEY (flow_run_id, timestamp)  -- Composite key to allow multiple logs per flow
            )
        """)

    def upsert_log(self, log: Log):
        """
        Upsert a log entry into the database.
        
        Args:
            log: A Prefect Log object
        """
        self.db.execute("""
            INSERT OR REPLACE INTO logs 
            (name, level, message, timestamp, flow_run_id, task_run_id)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [
            log.name,
            log.level,
            log.message,
            log.timestamp,
            str(log.flow_run_id) if log.flow_run_id else None,
            str(log.task_run_id) if log.task_run_id else None
        ])

    def upsert_logs(self, logs: list[Log]):
        """
        Upsert multiple log entries into the database.
        
        Args:
            logs: A list of Prefect Log objects
        """
        for log in logs:
            self.upsert_log(log)

    def get_logs_for_flow_run(self, flow_run_id: UUID | str) -> list[dict]:
        """
        Get all logs for a specific flow run.
        
        Args:
            flow_run_id: The ID of the flow run
            
        Returns:
            List of log entries as dictionaries
        """
        result = self.db.execute(
            "SELECT * FROM logs WHERE flow_run_id = ? ORDER BY timestamp",
            [str(flow_run_id)]
        ).fetchall()
        
        # Convert to list of dicts with column names
        columns = ['name', 'level', 'message', 'timestamp', 'flow_run_id', 'task_run_id']
        return [dict(zip(columns, row)) for row in result]
    