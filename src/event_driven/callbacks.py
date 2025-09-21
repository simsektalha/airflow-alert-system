"""
Airflow failure callbacks for event-driven alert system.

This module provides callback functions that are triggered when DAGs or tasks fail,
enabling real-time event-driven alert processing.
"""

import asyncio
import logging
from typing import Any, Dict, Optional

from airflow.models import DagRun, TaskInstance
from airflow.utils.context import Context

from .agentic_processor import AgenticFailureProcessor
from .config import AlertConfig

logger = logging.getLogger(__name__)


class FailureCallbackHandler:
    """Handles failure callbacks and triggers agentic processing."""
    
    def __init__(self, config: Optional[AlertConfig] = None) -> None:
        """Initialize the failure callback handler.
        
        Args:
            config: Configuration for the alert system. If None, uses default config.
        """
        self.config = config or AlertConfig()
        self.processor = AgenticFailureProcessor(self.config)
    
    def dag_failure_callback(self, context: Context) -> None:
        """Callback triggered when a DAG run fails.
        
        Args:
            context: Airflow context containing DAG run information.
        """
        try:
            dag_run: DagRun = context.get("dag_run")
            if not dag_run:
                logger.warning("DAG failure callback triggered but no dag_run in context")
                return
            
            dag_id = dag_run.dag_id
            dag_run_id = dag_run.run_id
            
            logger.info(f"DAG failure detected: {dag_id} - {dag_run_id}")
            
            # Trigger async processing
            asyncio.run(
                self.processor.process_dag_failure(
                    dag_id=dag_id,
                    dag_run_id=dag_run_id,
                    context=context
                )
            )
            
        except Exception as e:
            logger.error(f"Error in DAG failure callback: {e}", exc_info=True)
    
    def task_failure_callback(self, context: Context) -> None:
        """Callback triggered when a task fails.
        
        Args:
            context: Airflow context containing task instance information.
        """
        try:
            task_instance: TaskInstance = context.get("task_instance")
            dag_run: DagRun = context.get("dag_run")
            
            if not task_instance or not dag_run:
                logger.warning("Task failure callback triggered but missing context data")
                return
            
            dag_id = dag_run.dag_id
            dag_run_id = dag_run.run_id
            task_id = task_instance.task_id
            try_number = task_instance.try_number
            
            logger.info(f"Task failure detected: {dag_id}.{task_id} (run: {dag_run_id}, try: {try_number})")
            
            # Trigger async processing
            asyncio.run(
                self.processor.process_task_failure(
                    dag_id=dag_id,
                    dag_run_id=dag_run_id,
                    task_id=task_id,
                    try_number=try_number,
                    context=context
                )
            )
            
        except Exception as e:
            logger.error(f"Error in task failure callback: {e}", exc_info=True)


def create_dag_failure_callback(config: Optional[AlertConfig] = None) -> callable:
    """Create a DAG failure callback function.
    
    Args:
        config: Configuration for the alert system.
        
    Returns:
        Callback function that can be used as on_failure_callback in DAGs.
    """
    handler = FailureCallbackHandler(config)
    return handler.dag_failure_callback


def create_task_failure_callback(config: Optional[AlertConfig] = None) -> callable:
    """Create a task failure callback function.
    
    Args:
        config: Configuration for the alert system.
        
    Returns:
        Callback function that can be used as on_failure_callback in tasks.
    """
    handler = FailureCallbackHandler(config)
    return handler.task_failure_callback
