"""
Usage example for the event-driven Airflow AI alert system.

This script demonstrates how to set up and use the event-driven alert system
with your Airflow DAGs.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from src.event_driven.callbacks import create_dag_failure_callback, create_task_failure_callback
from src.event_driven.config import AlertConfig

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Step 1: Configure the alert system
def setup_alert_system() -> tuple[callable, callable]:
    """Set up the alert system and return callback functions.
    
    Returns:
        Tuple of (dag_failure_callback, task_failure_callback) functions.
    """
    # Create configuration
    config = AlertConfig()
    
    # Validate configuration (will raise error if invalid)
    try:
        config.validate()
        logger.info("Alert system configuration validated successfully")
    except ValueError as e:
        logger.error(f"Configuration validation failed: {e}")
        raise
    
    # Create callback functions
    dag_callback = create_dag_failure_callback(config)
    task_callback = create_task_failure_callback(config)
    
    logger.info("Alert system callbacks created successfully")
    return dag_callback, task_callback

# Step 2: Create your DAG with callbacks
def create_example_dag() -> DAG:
    """Create an example DAG with failure callbacks.
    
    Returns:
        Configured DAG instance.
    """
    # Get callback functions
    dag_failure_callback, task_failure_callback = setup_alert_system()
    
    # Define default arguments with task-level callback
    default_args: Dict[str, Any] = {
        "owner": "data-team",
        "depends_on_past": False,
        "start_date": datetime(2024, 1, 1),
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "on_failure_callback": task_failure_callback,  # Task-level callback
    }
    
    # Create DAG with DAG-level callback
    dag = DAG(
        "example_ai_alert_dag",
        default_args=default_args,
        description="Example DAG with AI-powered failure analysis",
        schedule_interval=timedelta(hours=1),
        catchup=False,
        on_failure_callback=dag_failure_callback,  # DAG-level callback
        tags=["example", "ai-alerts"],
    )
    
    # Define tasks that might fail
    def failing_task() -> None:
        """A task that will fail to demonstrate the alert system."""
        import random
        
        # Randomly fail to simulate real-world scenarios
        if random.random() < 0.7:  # 70% chance of failure
            raise ValueError("Simulated task failure for AI analysis demonstration")
        
        logger.info("Task completed successfully!")
    
    def data_processing_task() -> None:
        """A data processing task that might fail."""
        import random
        
        # Simulate data processing that might fail
        if random.random() < 0.3:  # 30% chance of failure
            raise RuntimeError("Data processing failed - missing required columns")
        
        logger.info("Data processing completed successfully!")
    
    # Task 1: Data extraction (will fail)
    extract_data = BashOperator(
        task_id="extract_data",
        bash_command="echo 'Extracting data...' && sleep 2 && (exit 1)",  # This will fail
        dag=dag,
    )
    
    # Task 2: Data transformation (might fail)
    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=failing_task,
        dag=dag,
    )
    
    # Task 3: Data validation (might fail)
    validate_data = PythonOperator(
        task_id="validate_data",
        python_callable=data_processing_task,
        dag=dag,
    )
    
    # Task 4: Load data (depends on previous tasks)
    load_data = BashOperator(
        task_id="load_data",
        bash_command="echo 'Loading data to warehouse...' && sleep 1",
        dag=dag,
    )
    
    # Task 5: Send notification (always succeeds)
    send_notification = BashOperator(
        task_id="send_notification",
        bash_command="echo 'Pipeline completed successfully!'",
        dag=dag,
    )
    
    # Define task dependencies
    extract_data >> transform_data >> validate_data >> load_data >> send_notification
    
    return dag

# Step 3: Test the system
async def test_alert_system() -> None:
    """Test the alert system with a simulated failure."""
    from src.event_driven.agentic_processor import AgenticFailureProcessor
    from src.event_driven.config import AlertConfig
    
    logger.info("Testing alert system...")
    
    # Create configuration
    config = AlertConfig()
    config.validate()
    
    # Create processor
    processor = AgenticFailureProcessor(config)
    
    # Simulate a task failure
    await processor.process_task_failure(
        dag_id="test_dag",
        dag_run_id="test_run_123",
        task_id="test_task",
        try_number=1
    )
    
    logger.info("Alert system test completed")

if __name__ == "__main__":
    # Create the DAG
    dag = create_example_dag()
    logger.info(f"Created DAG: {dag.dag_id}")
    
    # Test the alert system
    asyncio.run(test_alert_system())
