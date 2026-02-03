"""
Airflow Trigger Service.

Provides functionality to trigger Airflow DAGs via REST API.
"""

import os
import logging
from datetime import datetime
from typing import Optional, Dict, Any

import httpx

logger = logging.getLogger(__name__)


class AirflowTrigger:
    """Service for triggering Airflow DAGs."""

    def __init__(self):
        """Initialize Airflow connection settings."""
        self.base_url = os.getenv('AIRFLOW_BASE_URL', 'http://airflow-webserver:8080')
        self.username = os.getenv('AIRFLOW_USERNAME', 'airflow')
        self.password = os.getenv('AIRFLOW_PASSWORD', 'airflow')

    async def trigger_dag(
        self,
        dag_id: str,
        conf: Optional[Dict[str, Any]] = None,
        run_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Trigger an Airflow DAG.

        Args:
            dag_id: DAG ID to trigger
            conf: Configuration to pass to the DAG
            run_id: Optional custom run ID

        Returns:
            Dict with trigger result
        """
        url = f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns"

        payload = {
            "conf": conf or {}
        }

        if run_id:
            payload["dag_run_id"] = run_id
        else:
            payload["dag_run_id"] = f"api_trigger_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    url,
                    json=payload,
                    auth=(self.username, self.password),
                    headers={"Content-Type": "application/json"},
                    timeout=30.0
                )

                if response.status_code == 200:
                    data = response.json()
                    return {
                        "success": True,
                        "dag_id": dag_id,
                        "run_id": data.get("dag_run_id"),
                        "message": "DAG triggered successfully"
                    }
                elif response.status_code == 409:
                    return {
                        "success": False,
                        "dag_id": dag_id,
                        "run_id": None,
                        "message": "DAG run already exists"
                    }
                else:
                    logger.error(f"Failed to trigger DAG: {response.status_code} - {response.text}")
                    return {
                        "success": False,
                        "dag_id": dag_id,
                        "run_id": None,
                        "message": f"Failed: {response.status_code}"
                    }

        except httpx.ConnectError as e:
            logger.error(f"Connection error to Airflow: {e}")
            return {
                "success": False,
                "dag_id": dag_id,
                "run_id": None,
                "message": "Cannot connect to Airflow"
            }
        except Exception as e:
            logger.error(f"Error triggering DAG: {e}")
            return {
                "success": False,
                "dag_id": dag_id,
                "run_id": None,
                "message": str(e)
            }

    async def get_dag_runs(self, dag_id: str, limit: int = 10) -> Dict[str, Any]:
        """Get recent DAG runs."""
        url = f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns"

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    url,
                    auth=(self.username, self.password),
                    params={"limit": limit, "order_by": "-execution_date"},
                    timeout=30.0
                )

                if response.status_code == 200:
                    return response.json()
                else:
                    return {"dag_runs": [], "error": response.status_code}

        except Exception as e:
            logger.error(f"Error getting DAG runs: {e}")
            return {"dag_runs": [], "error": str(e)}

    async def get_dag_run_status(self, dag_id: str, run_id: str) -> Dict[str, Any]:
        """Get status of a specific DAG run."""
        url = f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns/{run_id}"

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    url,
                    auth=(self.username, self.password),
                    timeout=30.0
                )

                if response.status_code == 200:
                    return response.json()
                else:
                    return {"error": response.status_code}

        except Exception as e:
            logger.error(f"Error getting DAG run status: {e}")
            return {"error": str(e)}
