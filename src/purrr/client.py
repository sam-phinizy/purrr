import json
from uuid import UUID

import duckdb
from prefect import get_client
from prefect.client.orchestration import PrefectClient
from prefect.client.schemas.filters import (
    FlowRunFilterState,
    FlowRunFilterStateType,
    FlowRunFilter,
    LogFilterFlowRunId,
    LogFilter,
)
from prefect.client.schemas.objects import (
    TERMINAL_STATES,
    Log,
    FlowRun,
    StateType as FlowRunStates,
)
from prefect.client.schemas.responses import DeploymentResponse
from prefect.client.schemas.sorting import FlowRunSort
from prefect.exceptions import ObjectNotFound


class CachedPrefectClient:
    def __init__(self):
        self.client = get_client()
        self.db = DuckDBCache("test.db")

    def reset(self):
        self.client = PrefectClient()

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
        if state_types:
            flow_run_filter = FlowRunFilter(
                state=FlowRunFilterState(type=FlowRunFilterStateType(any_=state_types))
            )
        else:
            flow_run_filter = None

        all_flow_runs = []
        offset = 0

        while True:
            flow_runs: list[FlowRun] = await self.client.read_flow_runs(
                sort=sort, offset=offset, flow_run_filter=flow_run_filter
            )
            if not flow_runs:
                break

            self.db.runs.upsert(flow_runs)

            all_flow_runs.extend(flow_runs)
            offset += len(flow_runs)

        return all_flow_runs

    async def get_run_by_id(
        self, run_id: UUID | str, force_refresh: bool = False
    ) -> FlowRun | None:
        try:
            if force_refresh:
                flow_run = await self._fetch_and_cache_flow_run(run_id)
                return flow_run

            cached_run = self.db.runs.read(run_id)
            if cached_run and cached_run.state_name in TERMINAL_STATES:
                return cached_run

            flow_run = await self._fetch_and_cache_flow_run(run_id)
            return flow_run

        except ObjectNotFound:
            return None

    async def _fetch_and_cache_flow_run(self, run_id: UUID | str) -> FlowRun:
        flow_run = await self.client.read_flow_run(run_id)
        self.db.runs.upsert([flow_run])
        return flow_run

    async def get_run_logs(self, run_id: UUID | str) -> str:
        log_filter = LogFilter(flow_run_id=LogFilterFlowRunId(any_=[run_id]))
        logs = await self.client.read_logs(log_filter=log_filter)
        return "\n".join([log.message for log in logs])

    async def get_deployment_by_id(
        self, deployment_id: UUID
    ) -> DeploymentResponse | None:
        try:
            deployment = await self.client.read_deployment(deployment_id)
        except ObjectNotFound:
            return None
        return deployment


class RunsClient:
    def __init__(self, db: duckdb.DuckDBPyConnection):
        self.db = db
        self._create_table()

    def _create_table(self):
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

    def upsert(self, flow_runs: list[FlowRun]):
        for flow_run in flow_runs:
            flow_run_dict = json.loads(flow_run.model_dump_json())
            self.db.execute(
                """
                INSERT OR REPLACE INTO flow_runs
                (raw_json, id, name, created, updated, deployment_id, flow_id, state_name, work_pool_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                [
                    json.dumps(flow_run_dict),
                    str(flow_run.id),
                    flow_run.name,
                    flow_run.created,
                    flow_run.updated,
                    str(flow_run.deployment_id) if flow_run.deployment_id else None,
                    str(flow_run.flow_id),
                    flow_run.state_name or "Unknown",
                    flow_run.work_pool_name,
                ],
            )

    def read(self, run_id: UUID | str) -> FlowRun | None:
        result = self.db.execute(
            f"SELECT * FROM flow_runs WHERE id = '{run_id}'"
        ).fetchall()
        if result:
            return FlowRun(**json.loads(result[0][0]))
        return None


class LogsClient:
    def __init__(self, db: duckdb.DuckDBPyConnection):
        self.db = db
        self._create_table()

    def _create_table(self):
        self.db.execute("""
                CREATE TABLE IF NOT EXISTS logs (
                    name VARCHAR,
                    level INTEGER,
                    message VARCHAR,
                    timestamp TIMESTAMP,
                    flow_run_id VARCHAR,
                    task_run_id VARCHAR,
                    PRIMARY KEY (flow_run_id, timestamp)
                )
            """)

    def upsert(self, logs: list[Log]):
        for log in logs:
            self.db.execute(
                """
                INSERT OR REPLACE INTO logs
                (name, level, message, timestamp, flow_run_id, task_run_id)
                VALUES (?, ?, ?, ?, ?, ?)
            """,
                [
                    log.name,
                    log.level,
                    log.message,
                    log.timestamp,
                    str(log.flow_run_id) if log.flow_run_id else None,
                    str(log.task_run_id) if log.task_run_id else None,
                ],
            )

    def flow_run(self, flow_run_id: UUID | str) -> list[dict]:
        result = self.db.execute(
            "SELECT * FROM logs WHERE flow_run_id = ? ORDER BY timestamp",
            [str(flow_run_id)],
        ).fetchall()
        columns = [
            "name",
            "level",
            "message",
            "timestamp",
            "flow_run_id",
            "task_run_id",
        ]
        return [dict(zip(columns, row)) for row in result]


class DuckDBCache:
    def __init__(
        self,
        db_path: str = "duckdb.db",
        logs_client_class: type[LogsClient] = LogsClient,
        runs_client_class: type[RunsClient] = RunsClient,
    ):
        self.db = duckdb.connect(db_path)
        self.logs = logs_client_class(self.db)
        self.runs = runs_client_class(self.db)
