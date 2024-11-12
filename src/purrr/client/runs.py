import json
from uuid import UUID
import logging
import duckdb
from prefect.client.schemas.objects import FlowRun
from textual.logging import TextualHandler

logging.basicConfig(
    level="NOTSET",
    handlers=[TextualHandler()],
)


class RunsCache:
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

    def filter(self, query: str):
        query = f"SELECT * FROM flow_runs WHERE {query}"
        logging.info("query", query)
        try:
            result = self.db.execute(query).fetchall()
        except duckdb.ParserException as e:
            logging.error(e)
        return [FlowRun(**json.loads(row[0])) for row in result]
