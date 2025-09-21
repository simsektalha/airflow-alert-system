"""
Example DAG demonstrating event-driven failure callbacks.

This DAG shows how to integrate the AI alert system with Airflow DAGs
using failure callbacks that trigger agentic analysis.
"""

from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from src.event_driven.callbacks import create_dag_failure_callback, create_task_failure_callback
from src.event_driven.config import AlertConfig

# Configure the alert system
alert_config = AlertConfig()
alert_config.validate()

# Create callback functions
dag_failure_callback = create_dag_failure_callback(alert_config)
task_failure_callback = create_task_failure_callback(alert_config)

# DAG definition
default_args: Dict[str, Any] = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_failure_callback,  # Task-level failure callback
}

dag = DAG(
    "example_ai_alert_dag",
    default_args=default_args,
    description="Example DAG with AI-powered failure analysis",
    schedule_interval=timedelta(hours=1),
    catchup=False,
    on_failure_callback=dag_failure_callback,  # DAG-level failure callback
    tags=["example", "ai-alerts"],
)

def failing_task() -> None:
    """A task that will fail to demonstrate the alert system."""
    import random
    
    # Randomly fail to simulate real-world scenarios
    if random.random() < 0.7:  # 70% chance of failure
        raise ValueError("Simulated task failure for AI analysis demonstration")
    
    print("Task completed successfully!")

def data_processing_task() -> None:
    """A data processing task that might fail."""
    import random
    
    # Simulate data processing that might fail
    if random.random() < 0.3:  # 30% chance of failure
        raise RuntimeError("Data processing failed - missing required columns")
    
    print("Data processing completed successfully!")

# Task 1: Data extraction (might fail)
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
