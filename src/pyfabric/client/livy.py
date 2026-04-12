"""
Client for the Fabric Livy API - execute Spark SQL against Lakehouse tables.

Uses FabricCredential for authentication. Implements context manager protocol
for automatic session cleanup.

Usage:
    from pyfabric.client.auth import FabricCredential
    from pyfabric.client.livy import LivyClient

    cred = FabricCredential(tenant="contoso")

    with LivyClient(cred, workspace_id, lakehouse_id) as livy:
        livy.sql("CREATE TABLE my_table (id STRING, name STRING) USING DELTA")
        livy.sql("INSERT INTO my_table VALUES ('1', 'test')")
        result = livy.sql("SELECT * FROM my_table")
        print(result)

API docs:
    https://learn.microsoft.com/en-us/fabric/data-engineering/get-started-api-livy-session
"""

import json
import logging
import time

import requests

from .auth import FabricCredential

log = logging.getLogger(__name__)


class LivyClient:
    """Spark session client using the Fabric Livy API."""

    def __init__(
        self,
        credential: FabricCredential,
        workspace_id: str,
        lakehouse_id: str,
    ):
        self._credential = credential
        self.workspace_id = workspace_id
        self.lakehouse_id = lakehouse_id
        self.base_url = (
            f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}"
            f"/lakehouses/{lakehouse_id}/livyapi/versions/2023-12-01/sessions"
        )
        self.session_id = None
        self.session_url = None
        self._session = requests.Session()

    def __enter__(self):
        self.create_session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_session()

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self._credential.fabric_token}",
            "Content-Type": "application/json",
        }

    def create_session(self, poll_interval: int = 5) -> int:
        """Create a new Spark session and wait for it to become idle."""
        log.info("Creating Livy session...")
        resp = self._session.post(self.base_url, headers=self._headers(), json={})
        if resp.status_code not in (200, 202):
            raise RuntimeError(
                f"Failed to create session: {resp.status_code} {resp.text}"
            )

        session = resp.json()
        self.session_id = session["id"]
        self.session_url = f"{self.base_url}/{self.session_id}"
        log.info(
            "Session %s created (state: %s)", self.session_id, session.get("state")
        )

        self._wait_for_session_idle(poll_interval)
        return self.session_id

    def close_session(self) -> None:
        """Delete the current Spark session."""
        if not self.session_url:
            return
        log.info("Closing Livy session %s", self.session_id)
        resp = self._session.delete(self.session_url, headers=self._headers())
        log.info("Session closed (status: %d)", resp.status_code)
        self.session_id = None
        self.session_url = None

    def sql(self, statement: str) -> str | None:
        """Execute a Spark SQL statement and return the text output."""
        return self.execute(
            f'spark.sql("{self._escape(statement)}").show()', kind="spark"
        )

    def execute(self, code: str, kind: str = "spark") -> str | None:
        """Submit arbitrary code and return the text output.

        Args:
            code: Code to execute (Spark Scala, PySpark, etc.).
            kind: Language kind - "spark" (Scala) or "pyspark".

        Returns:
            The text/plain output from the statement, or None.
        """
        if not self.session_url:
            raise RuntimeError("No active session. Call create_session() first.")

        statements_url = f"{self.session_url}/statements"
        resp = self._session.post(
            statements_url,
            headers=self._headers(),
            json={"code": code, "kind": kind},
        )

        if resp.status_code != 200:
            raise RuntimeError(
                f"Failed to submit statement: {resp.status_code} {resp.text}"
            )

        stmt = resp.json()
        stmt_id = stmt["id"]
        stmt_url = f"{statements_url}/{stmt_id}"

        # Poll until complete
        while stmt.get("state") not in ("available", "error", "cancelled"):
            time.sleep(3)
            stmt = self._session.get(stmt_url, headers=self._headers()).json()

        if stmt.get("state") == "error":
            error_info = stmt.get("output", {})
            raise RuntimeError(f"Statement failed: {json.dumps(error_info, indent=2)}")

        # Extract text output
        output = stmt.get("output", {})
        if output.get("status") == "error":
            raise RuntimeError(
                f"Spark error: {output.get('ename')}: {output.get('evalue')}"
            )

        data = output.get("data", {})
        return data.get("text/plain")

    def _wait_for_session_idle(self, poll_interval: int = 5) -> None:
        """Poll until session state is 'idle'."""
        log.debug("Waiting for session to become idle...")
        while True:
            resp = self._session.get(self.session_url, headers=self._headers())
            state = resp.json().get("state", "unknown")
            if state == "idle":
                log.info("Session is idle and ready")
                return
            if state in ("dead", "killed", "error"):
                raise RuntimeError(f"Session entered bad state: {state}")
            time.sleep(poll_interval)

    @staticmethod
    def _escape(s: str) -> str:
        """Escape a string for embedding in a Spark SQL call."""
        return s.replace("\\", "\\\\").replace('"', '\\"')
