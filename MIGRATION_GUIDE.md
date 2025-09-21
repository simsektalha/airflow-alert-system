# Migration Guide: From Polling to Event-Driven

This guide helps you migrate from the old polling-based approach (`airflow_ai_demo.py`) to the new event-driven system.

## Key Differences

### Old Approach (Polling)
- **Manual execution**: Run script manually or via cron
- **Polling**: Scans all DAGs and runs to find failures
- **Batch processing**: Processes multiple failures at once
- **Resource intensive**: Fetches data for all DAGs regardless of failures

### New Approach (Event-Driven)
- **Automatic execution**: Triggers immediately when failures occur
- **Event-driven**: Only processes actual failures
- **Real-time**: Analysis starts instantly when failure happens
- **Efficient**: Only fetches data for failed DAGs/tasks

## Migration Steps

### 1. Install New Dependencies

```bash
pip install -r requirements.txt
```

### 2. Update Environment Variables

The environment variables remain the same, but you can now use them in your DAGs:

```bash
export AIRFLOW_BASE_URL="http://localhost:8080"
export AIRFLOW_USERNAME="admin"
export AIRFLOW_PASSWORD="admin"
export LLM_PROVIDER="openailike"  # or "ollama"
export LLM_BASE_URL="https://your-llm-cluster.example.com/v1"
export LLM_API_KEY="YOUR_KEY"
export LLM_MODEL="gpt-4o-mini"
```

### 3. Update Your DAGs

#### Before (Old Approach)
```python
from airflow import DAG
from airflow.operators.bash import BashOperator

# No callbacks - failures handled by external script
dag = DAG(
    "my_dag",
    # ... other parameters
)

task = BashOperator(
    task_id="my_task",
    bash_command="echo 'Hello World'",
    dag=dag,
)
```

#### After (Event-Driven Approach)
```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from src.event_driven.callbacks import create_dag_failure_callback, create_task_failure_callback
from src.event_driven.config import AlertConfig

# Configure the alert system
config = AlertConfig()
config.validate()

# Create callback functions
dag_failure_callback = create_dag_failure_callback(config)
task_failure_callback = create_task_failure_callback(config)

# DAG with callbacks
dag = DAG(
    "my_dag",
    on_failure_callback=dag_failure_callback,  # DAG-level callback
    # ... other parameters
)

task = BashOperator(
    task_id="my_task",
    bash_command="echo 'Hello World'",
    on_failure_callback=task_failure_callback,  # Task-level callback
    dag=dag,
)
```

### 4. Remove Old Scripts

You can now remove or archive:
- `airflow_ai_demo.py` (old polling script)
- Any cron jobs that ran the old script

### 5. Test the New System

#### Test with Example DAG
```bash
# Copy the example DAG to your Airflow DAGs folder
cp examples/example_dag_with_callbacks.py $AIRFLOW_HOME/dags/

# Trigger the DAG and let it fail to test the callbacks
```

#### Test with Standalone Processor
```bash
# Test DAG failure processing
python -m src.event_driven.standalone_processor dag my_dag run_123

# Test task failure processing
python -m src.event_driven.standalone_processor task my_dag run_123 my_task 1
```

## Configuration Options

### Basic Configuration
```python
from src.event_driven.config import AlertConfig

config = AlertConfig()  # Uses environment variables
config.validate()
```

### Advanced Configuration
```python
from src.event_driven.config import AlertConfig

config = AlertConfig(
    airflow_base_url="http://custom-airflow:8080",
    llm_provider="ollama",
    llm_model="llama3.1",
    output_file="/var/log/airflow-analysis.log"
)
config.validate()
```

## Callback Types

### DAG-Level Callbacks
- Triggered when the entire DAG run fails
- Receives: `dag_id`, `dag_run_id`
- Use: `on_failure_callback` in DAG definition

### Task-Level Callbacks
- Triggered when individual tasks fail
- Receives: `dag_id`, `dag_run_id`, `task_id`, `try_number`
- Use: `on_failure_callback` in task definition

## Benefits of Migration

1. **Real-time Analysis**: Failures are analyzed immediately
2. **Reduced Resource Usage**: Only processes actual failures
3. **Better Integration**: Callbacks are part of your DAG definitions
4. **Scalable**: No need to manage external polling scripts
5. **Reliable**: No risk of missing failures between polling intervals

## Troubleshooting

### Callbacks Not Triggering
- Ensure callbacks are properly imported and configured
- Check that your DAGs are using the correct callback functions
- Verify Airflow configuration allows custom callbacks

### Configuration Errors
- Run `config.validate()` to check your configuration
- Ensure all required environment variables are set
- Check that your LLM provider is accessible

### Analysis Not Generated
- Check logs for errors in the agentic processor
- Verify Airflow API access and authentication
- Ensure your LLM provider is working correctly

## Rollback Plan

If you need to rollback to the old system:

1. Keep the old `airflow_ai_demo.py` script
2. Remove callback imports from your DAGs
3. Set up cron jobs to run the old script
4. Monitor the old system for any issues

## Support

For issues or questions:
1. Check the logs for error messages
2. Verify your configuration with `config.validate()`
3. Test with the standalone processor
4. Review the example DAG for reference
