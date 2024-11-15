from uuid import UUID
from datetime import datetime
import sqlite3

from prefect import get_client
from prefect.client.schemas.filters import (
    FlowRunFilterState,
    FlowRunFilterStateType,
    FlowRunFilter,
    LogFilterFlowRunId,
    LogFilterTaskRunId,
    LogFilter,
)
from prefect.client.schemas.objects import (
    TERMINAL_STATES,
    FlowRun,
    StateType as FlowRunStates,
)
from prefect.client.schemas.responses import DeploymentResponse
from prefect.client.schemas.sorting import FlowRunSort
from prefect.exceptions import ObjectNotFound

from purrr.client.logs import LogsCache
from purrr.client.runs import RunsCache
from purrr.client.deployments import DeploymentCache


class CachingPrefectClient:
    def __init__(self, db_name: str = "test.db"):
        self.client = get_client()
        self.cache = SQLiteCache(db_name)

    async def get_runs(
        self,
        sort: FlowRunSort = FlowRunSort.START_TIME_DESC,
        state_types: list[FlowRunStates] | None = None,
    ) -> list[FlowRun]:
        """Get all flow runs from Prefect.

        Args:
            sort (FlowRunSort, optional): Sort order. Defaults to FlowRunSort.START_TIME_DESC.
            state_types (list[FlowRunStates] | None, optional): State types to filter by. Defaults to None.

        Returns:
            list[FlowRun]: List of flow runs.
        """
        try:
            args = {}
            args["sort"] = sort
            args["offset"] = 0

            if state_types:
                args["flow_run_filter"] = FlowRunFilter(
                    state=FlowRunFilterState(
                        type=FlowRunFilterStateType(any_=state_types)
                    )
                )
            else:
                args["flow_run_filter"] = None

            all_flow_runs = []

            while True:
                flow_runs: list[FlowRun] = await self.client.read_flow_runs(**args)
                if not flow_runs:
                    break

                self.cache.runs.upsert(flow_runs)

                all_flow_runs.extend(flow_runs)
                args["offset"] += len(flow_runs)

            self.cache.log_execution("get_runs", True)
            return all_flow_runs
        except Exception as e:
            self.cache.log_execution("get_runs", False)
            raise e

    async def get_run(
        self, run_id: UUID | str, force_refresh: bool = False
    ) -> FlowRun | None:
        if isinstance(run_id, str):
            run_id = UUID(run_id)

        try:
            if force_refresh:
                flow_run = await self._fetch_and_cache_flow_run(run_id)
                return flow_run

            cached_run = self.cache.runs.read(run_id)
            if cached_run and cached_run.state_name in TERMINAL_STATES:
                return cached_run

            flow_run = await self._fetch_and_cache_flow_run(run_id)
            return flow_run

        except ObjectNotFound:
            return None

    async def _fetch_and_cache_flow_run(self, run_id: UUID) -> FlowRun:
        flow_run = await self.client.read_flow_run(run_id)
        self.cache.runs.upsert([flow_run])
        return flow_run

    async def get_logs(
        self, run_id: UUID | str | None = None, task_run_id: UUID | str | None = None
    ) -> str:
        if isinstance(run_id, str):
            run_id = UUID(run_id)
        if isinstance(task_run_id, str):
            task_run_id = UUID(task_run_id)

        if run_id and task_run_id:
            raise ValueError("Cannot filter by both run_id and task_run_id")

        if run_id:
            flow_run_filter = LogFilterFlowRunId(any_=[run_id])
        else:
            flow_run_filter = None

        if task_run_id:
            task_run_filter = LogFilterTaskRunId(any_=[task_run_id])
        else:
            task_run_filter = None

        log_filter = LogFilter(
            flow_run_id=flow_run_filter,
            task_run_id=task_run_filter,
        )

        logs = await self.client.read_logs(log_filter=log_filter)
        return "\n".join([log.message for log in logs])

    async def get_deployment_by_id(
        self, deployment_id: UUID, force_refresh: bool = True
    ) -> DeploymentResponse:
        if not force_refresh:
            cached_deployment = self.cache.deployments.read(deployment_id)
            if cached_deployment:
                return cached_deployment
        else:
            cached_deployment = None

        # If not in cache, fetch from API and cache it
        deployment = await self.client.read_deployment(deployment_id)
        self.cache.deployments.upsert([deployment])

        return deployment


class SQLiteCache:
    def __init__(
        self,
        db_path: str = "sqlite.db",
        logs_client_class: type[LogsCache] = LogsCache,
        runs_client_class: type[RunsCache] = RunsCache,
        deployments_client_class: type[DeploymentCache] = DeploymentCache,
    ):
        self.db_path = db_path
        self.db = self._get_connection()
        self.logs = logs_client_class(self.db)
        self.runs = runs_client_class(self.db)
        self.deployments = deployments_client_class(self.db)

        # Initialize metadata table with function_name as primary key
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS purrr_metadata (
                function_name TEXT PRIMARY KEY,
                time_executed TIMESTAMP,
                success BOOLEAN
            )
        """)
        self.db.commit()

    def _get_connection(self) -> sqlite3.Connection:
        """Create a new SQLite connection with proper settings."""
        conn = sqlite3.connect(
            self.db_path,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )
        # Make sqlite3 return Row objects that support both index and key-based access
        conn.row_factory = sqlite3.Row
        return conn

    def log_execution(self, function_name: str, success: bool) -> None:
        """Log function execution with timestamp and success status.
        Will update existing record if function has been called before."""
        self.db.execute(
            """
            INSERT OR REPLACE INTO purrr_metadata (function_name, time_executed, success)
            VALUES (?, ?, ?)
        """,
            [function_name, datetime.now(), success],
        )
        self.db.commit()
