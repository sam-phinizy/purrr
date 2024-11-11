import duckdb
from prefect.client.schemas.objects import Log


from uuid import UUID


class LogsCache:
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
