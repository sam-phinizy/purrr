from typing import Sequence
from uuid import UUID
import sqlite3

from prefect.client.schemas.responses import DeploymentResponse


class DeploymentCache:
    """Client for managing deployment data in SQLite cache."""

    def __init__(self, db: sqlite3.Connection):
        self.db = db
        self._init_table()

    def _init_table(self):
        """Initialize the deployments table if it doesn't exist."""
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS deployments (
                id TEXT PRIMARY KEY,
                name TEXT,
                flow_id TEXT,
                paused BOOLEAN,
                work_pool_name TEXT,
                work_queue_name TEXT,
                data JSON
            )
            """
        )
        self.db.commit()

    def upsert(self, deployments: Sequence[DeploymentResponse]):
        """Insert or update deployment records in the cache.

        Args:
            deployments: Sequence of DeploymentResponse objects to upsert
        """
        if not deployments:
            return

        values = [
            (
                str(d.id),
                d.name,
                str(d.flow_id) if d.flow_id else None,
                1 if d.paused else 0,  # Convert boolean to integer for SQLite
                d.work_pool_name,
                d.work_queue_name,
                d.json(),
            )
            for d in deployments
        ]

        # Perform upsert operation
        cursor = self.db.cursor()
        for value in values:
            cursor.execute(
                """
                INSERT OR REPLACE INTO deployments (
                id, name, flow_id, paused, work_pool_name, work_queue_name, data
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                value,
            )
        self.db.commit()

    def read(self, deployment_id: UUID | str) -> DeploymentResponse | None:
        """Read a deployment from the cache by ID.

        Args:
            deployment_id: UUID of the deployment to retrieve

        Returns:
            DeploymentResponse if found, None otherwise
        """
        cursor = self.db.cursor()
        result = cursor.execute(
            """
            SELECT data FROM deployments
            WHERE id = ?
            """,
            [str(deployment_id)],
        ).fetchone()

        if result:
            return DeploymentResponse.parse_raw(result[0])
        return None
