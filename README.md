# Airflow AI Alert System – Event-Driven

An event-driven AI-powered alert system for Apache Airflow that automatically analyzes failures and provides intelligent recommendations. The system uses failure callbacks to trigger real-time analysis when DAGs or tasks fail.

## What it does
- **Event-Driven**: Uses Airflow failure callbacks to trigger analysis immediately when failures occur
- **Real-Time Processing**: No need to poll for failures - analysis starts instantly
- **AI-Powered Analysis**: Uses Agno agents with configurable LLM providers for intelligent failure analysis
- **Comprehensive Logging**: Fetches task logs and provides detailed failure context
- **Actionable Insights**: Provides root cause analysis, fix steps, and prevention tips

## Architecture

The system consists of several key components:

- **Event Callbacks** (`src/event_driven/callbacks.py`): Airflow callback functions that trigger on DAG/task failures
- **Agentic Processor** (`src/event_driven/agentic_processor.py`): AI-powered analysis engine that processes failure events
- **Airflow Client** (`src/event_driven/airflow_client.py`): Async client for fetching logs and task information
- **Configuration** (`src/event_driven/config.py`): Centralized configuration management
- **Example DAG** (`examples/example_dag_with_callbacks.py`): Demonstrates integration with Airflow DAGs

## Requirements
- Python 3.10+
- Apache Airflow 2.2.0+ with REST API enabled
- LLM provider (OpenAI-compatible API or Ollama)

Install dependencies:
```bash
pip install -r requirements.txt
```

Or install in development mode:
```bash
pip install -e ".[dev]"
```

## Quick Start

### 1. Set Environment Variables

**Airflow Configuration:**
```bash
export AIRFLOW_BASE_URL="http://localhost:8080"
export AIRFLOW_USERNAME="admin"
export AIRFLOW_PASSWORD="admin"
```

**LLM Configuration (choose one):**

**OpenAI-compatible API:**
```bash
export LLM_PROVIDER="openailike"
export LLM_BASE_URL="https://your-llm-cluster.example.com/v1"
export LLM_API_KEY="YOUR_KEY"
export LLM_MODEL="gpt-4o-mini"
```

**Ollama (local/remote):**
```bash
export LLM_PROVIDER="ollama"
export OLLAMA_BASE_URL="http://localhost:11434"
export LLM_MODEL="llama3.1"
```

### 2. Add Callbacks to Your DAGs

```python
from src.event_driven.callbacks import create_dag_failure_callback, create_task_failure_callback
from src.event_driven.config import AlertConfig

# Configure the alert system
config = AlertConfig()
config.validate()

# Create callback functions
dag_failure_callback = create_dag_failure_callback(config)
task_failure_callback = create_task_failure_callback(config)

# Use in your DAG
dag = DAG(
    "my_dag",
    on_failure_callback=dag_failure_callback,  # DAG-level callback
    # ... other DAG parameters
)

# Use in your tasks
task = BashOperator(
    task_id="my_task",
    bash_command="echo 'Hello World'",
    on_failure_callback=task_failure_callback,  # Task-level callback
    dag=dag,
)
```

### 3. Test the System

Use the example DAG or test with the standalone processor:

```bash
# Test with a specific DAG run
python -m src.event_driven.standalone_processor dag my_dag run_123

# Test with a specific task
python -m src.event_driven.standalone_processor task my_dag run_123 my_task 1
```

## How It Works

### Event-Driven Flow

1. **Failure Occurs**: A DAG or task fails in Airflow
2. **Callback Triggered**: The appropriate failure callback is automatically invoked
3. **Context Captured**: Callback receives `dag_id`, `dag_run_id`, `task_id`, and `try_number`
4. **Logs Fetched**: System retrieves relevant logs from Airflow API
5. **AI Analysis**: Agentic processor analyzes the failure and generates recommendations
6. **Results Logged**: Analysis is logged and can be sent to external systems

### Callback Types

- **DAG Failure Callback**: Triggered when an entire DAG run fails
- **Task Failure Callback**: Triggered when individual tasks fail

### Configuration Options

The system supports various configuration options:

```python
from src.event_driven.config import AlertConfig

config = AlertConfig(
    airflow_base_url="http://localhost:8080",
    airflow_username="admin",
    airflow_password="admin",
    llm_provider="openailike",  # or "ollama"
    llm_base_url="https://api.openai.com/v1",
    llm_api_key="your-key",
    llm_model="gpt-4o-mini",
    output_file="analysis.log"  # Optional: write results to file
)
```

## Example Output

When a failure occurs, the system generates analysis like this:

```
=== AI Failure Analysis ===
Root Cause: The task failed due to a missing environment variable 'DATABASE_URL' that is required for database connection.

Fix Steps:
1. Add the missing DATABASE_URL environment variable to your Airflow configuration
2. Verify the database connection string format
3. Test the connection before running the DAG
4. Add proper error handling for missing environment variables

Prevention Tips:
- Use Airflow's Variable or Connection features for sensitive configuration
- Add validation checks at the start of tasks that require specific environment variables
- Implement proper logging to catch configuration issues early
==========================
```

## Advanced Usage

### Custom Configuration

You can customize the system behavior by modifying the configuration:

```python
from src.event_driven.config import AlertConfig

# Custom configuration
config = AlertConfig(
    llm_model="gpt-4",  # Use a different model
    output_file="/var/log/airflow-analysis.log",  # Custom output location
)

# Validate configuration
config.validate()
```

### Integration with External Systems

The system can be extended to integrate with external notification systems:

```python
# In agentic_processor.py, modify _handle_analysis_result method
async def _handle_analysis_result(self, failure_info, analysis):
    # Send to Slack
    await self.send_slack_notification(analysis)
    
    # Store in database
    await self.store_in_database(failure_info, analysis)
    
    # Create JIRA ticket
    await self.create_jira_ticket(failure_info, analysis)
```

## Troubleshooting

- **401/403 from Airflow**: Check `AIRFLOW_USERNAME`/`AIRFLOW_PASSWORD` and API access
- **Connection error**: Verify `AIRFLOW_BASE_URL` and that Airflow is running
- **LLM auth errors**: Verify provider-specific variables (`LLM_API_KEY`, `LLM_BASE_URL`, etc.)
- **Callback not triggered**: Ensure callbacks are properly registered in your DAGs
- **No analysis generated**: Check logs for errors in the agentic processor

## Development

### Running Tests

```bash
pytest tests/
```

### Code Formatting

```bash
ruff check . --fix
black src/ tests/
```

### Type Checking

```bash
mypy src/
```

## Next Steps

- Add Slack/Email notifications
- Persist results in database
- Create web UI for viewing analysis
- Add metrics and monitoring
- Support for multiple Airflow instances
- Integration with incident management systems
