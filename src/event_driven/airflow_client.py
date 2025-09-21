"""
Airflow REST API client for event-driven operations.

This module provides an async client for interacting with Airflow's REST API
to fetch logs and task information when failures occur.
"""

import logging
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)


class AirflowClient:
    """Async client for Airflow REST API operations."""
    
    def __init__(self, base_url: str, username: str, password: str) -> None:
        """Initialize the Airflow client.
        
        Args:
            base_url: Base URL of the Airflow instance.
            username: Username for authentication.
            password: Password for authentication.
        """
        self.base_url = base_url.rstrip("/")
        self.auth = (username, password)
    
    async def health_check(self) -> Dict[str, Any]:
        """Check Airflow health.
        
        Returns:
            Health status information.
            
        Raises:
            httpx.HTTPError: If the health check fails.
        """
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(
                f"{self.base_url}/api/v1/health",
                auth=self.auth
            )
            response.raise_for_status()
            return response.json()
    
    async def list_task_instances(
        self,
        dag_id: str,
        dag_run_id: str
    ) -> List[Dict[str, Any]]:
        """List task instances for a specific DAG run.
        
        Args:
            dag_id: ID of the DAG.
            dag_run_id: ID of the DAG run.
            
        Returns:
            List of task instance information.
            
        Raises:
            httpx.HTTPError: If the API request fails.
        """
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
                auth=self.auth
            )
            response.raise_for_status()
            return response.json().get("task_instances", [])
    
    async def get_task_log(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        try_number: int = 1
    ) -> str:
        """Get logs for a specific task instance.
        
        Args:
            dag_id: ID of the DAG.
            dag_run_id: ID of the DAG run.
            task_id: ID of the task.
            try_number: Try number of the task instance.
            
        Returns:
            Log content as string. Returns empty string if logs not available.
        """
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/logs/{try_number}",
                    auth=self.auth
                )
                
                if response.status_code >= 400:
                    logger.warning(f"Failed to get logs for {dag_id}.{task_id}: {response.status_code}")
                    return ""
                
                return response.text
                
        except Exception as e:
            logger.error(f"Error fetching logs for {dag_id}.{task_id}: {e}")
            return ""
    
    async def get_dag_run(
        self,
        dag_id: str,
        dag_run_id: str
    ) -> Optional[Dict[str, Any]]:
        """Get information about a specific DAG run.
        
        Args:
            dag_id: ID of the DAG.
            dag_run_id: ID of the DAG run.
            
        Returns:
            DAG run information or None if not found.
        """
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                response = await client.get(
                    f"{self.base_url}/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}",
                    auth=self.auth
                )
                
                if response.status_code == 404:
                    return None
                
                response.raise_for_status()
                return response.json()
                
        except Exception as e:
            logger.error(f"Error fetching DAG run {dag_id}.{dag_run_id}: {e}")
            return None
