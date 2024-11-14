import json
from uuid import UUID
import logging
import sqlite3
from datetime import datetime
from prefect.client.schemas.objects import FlowRun
from textual.logging import TextualHandler

logging.basicConfig(
    level="NOTSET",
    handlers=[TextualHandler()],
)


class RunsCache:
    def __init__(self, db: sqlite3.Connection):
        self.db = db
        self._create_table()

    def _create_table(self):
        self.db.execute("""
            CREATE TABLE IF NOT EXISTS flow_runs (
                raw_json JSON,
                id TEXT PRIMARY KEY,
                name TEXT,
                created TIMESTAMP,
                updated TIMESTAMP,
                deployment_id TEXT,
                flow_id TEXT,
                state_name TEXT,
                work_pool_name TEXT
            )
        """)
        self.db.commit()

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
                    datetime.fromisoformat(str(flow_run.created)),
                    datetime.fromisoformat(str(flow_run.updated)),
                    str(flow_run.deployment_id) if flow_run.deployment_id else None,
                    str(flow_run.flow_id),
                    flow_run.state_name or "Unknown",
                    flow_run.work_pool_name,
                ],
            )
        self.db.commit()

    def read(self, run_id: UUID | str) -> FlowRun | None:
        cursor = self.db.cursor()
        result = cursor.execute(
            "SELECT raw_json FROM flow_runs WHERE id = ?", [str(run_id)]
        ).fetchone()
        if result:
            return FlowRun.parse_raw(result[0])
        return None

    def filter(self, query: str):
        """Filter flow runs based on a WHERE clause.

        Args:
            query: SQL WHERE clause to filter flow runs

        Returns:
            list[FlowRun]: List of matching flow runs
        """
        sql = f"SELECT raw_json FROM flow_runs WHERE {query}"
        logging.info("Executing query: %s", sql)
        try:
            cursor = self.db.cursor()
            result = cursor.execute(sql).fetchall()
            return [FlowRun.parse_raw(row[0]) for row in result]
        except sqlite3.Error as e:
            logging.error("SQLite error: %s", str(e))
            return []
