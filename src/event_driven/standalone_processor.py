"""
Standalone processor for testing the event-driven alert system.

This script can be used to manually trigger failure analysis
for testing purposes without requiring actual Airflow failures.
"""

import asyncio
import logging
import sys
from typing import Optional

from .agentic_processor import AgenticFailureProcessor
from .config import AlertConfig

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_dag_failure_processing(
    dag_id: str,
    dag_run_id: str,
    config: Optional[AlertConfig] = None
) -> None:
    """Test DAG failure processing.
    
    Args:
        dag_id: ID of the DAG to test.
        dag_run_id: ID of the DAG run to test.
        config: Configuration for the alert system.
    """
    try:
        processor = AgenticFailureProcessor(config or AlertConfig())
        await processor.process_dag_failure(dag_id, dag_run_id)
        logger.info("DAG failure processing completed successfully")
    except Exception as e:
        logger.error(f"Error in DAG failure processing: {e}", exc_info=True)


async def test_task_failure_processing(
    dag_id: str,
    dag_run_id: str,
    task_id: str,
    try_number: int = 1,
    config: Optional[AlertConfig] = None
) -> None:
    """Test task failure processing.
    
    Args:
        dag_id: ID of the DAG containing the task.
        dag_run_id: ID of the DAG run.
        task_id: ID of the task to test.
        try_number: Try number of the task.
        config: Configuration for the alert system.
    """
    try:
        processor = AgenticFailureProcessor(config or AlertConfig())
        await processor.process_task_failure(dag_id, dag_run_id, task_id, try_number)
        logger.info("Task failure processing completed successfully")
    except Exception as e:
        logger.error(f"Error in task failure processing: {e}", exc_info=True)


async def main() -> None:
    """Main function for testing the event-driven system."""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python -m src.event_driven.standalone_processor dag <dag_id> <dag_run_id>")
        print("  python -m src.event_driven.standalone_processor task <dag_id> <dag_run_id> <task_id> [try_number]")
        sys.exit(1)
    
    mode = sys.argv[1]
    
    # Validate arguments based on mode
    if mode == "dag" and len(sys.argv) < 4:
        print("Error: 'dag' mode requires at least 4 arguments: script_name, mode, dag_id, dag_run_id")
        sys.exit(1)
    elif mode == "task" and len(sys.argv) < 5:
        print("Error: 'task' mode requires at least 5 arguments: script_name, mode, dag_id, dag_run_id, task_id")
        sys.exit(1)
    elif mode not in ["dag", "task"]:
        print(f"Error: Unknown mode '{mode}'. Supported modes: dag, task")
        sys.exit(1)
    
    try:
        config = AlertConfig()
        config.validate()
        
        if mode == "dag":
            dag_id = sys.argv[2]
            dag_run_id = sys.argv[3]
            await test_dag_failure_processing(dag_id, dag_run_id, config)
            
        elif mode == "task":
            dag_id = sys.argv[2]
            dag_run_id = sys.argv[3]
            task_id = sys.argv[4]
            try_number = int(sys.argv[5]) if len(sys.argv) > 5 else 1
            await test_task_failure_processing(dag_id, dag_run_id, task_id, try_number, config)
            
            
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
