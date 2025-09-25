# Airflow AI-Powered Failure Callbacks - Production

This repository contains your production Airflow AI failure callback system.

## 📁 **Project Structure**

```
production/
├── airflow_failure_responder.py          # Main failure responder script
└── dags/
    ├── callbacks/
    │   └── trigger_failure_responder.py  # Airflow callback function
    └── example.py                        # Example DAG with failure scenarios
```

## 🚀 **Usage**

### 1. Deploy to Airflow
Copy the files to your Airflow environment:
```bash
# Copy responder script
cp production/airflow_failure_responder.py /path/to/airflow/dags/

# Copy callback
cp production/dags/callbacks/trigger_failure_responder.py /path/to/airflow/dags/callbacks/

# Copy example DAG
cp production/dags/example.py /path/to/airflow/dags/
```

### 2. Configure Airflow Variable
Set up the `v_callback_fetch_failed_task` Airflow Variable with your configuration:
```json
{
    "base_url": "https://your-airflow-instance:8080",
    "auth": {
        "basic": {
            "username": "your-username",
            "password": "your-password"
        }
    },
    "tls": {
        "verify": true
    },
    "timeouts": {
        "connect": 10.0,
        "total": 30.0
    },
    "llm": {
        "driver": "openai_like",
        "model": "gpt-4o-mini",
        "base_url": "https://api.openai.com/v1",
        "api_key": "your-api-key",
        "temperature": 0.1,
        "max_tokens": 800
    },
    "output": {
        "method": "stdout",
        "file_path": "/tmp/failed_task_log.json",
        "teams_webhook": "https://api.powerplatform.com/...",
        "teams_verify_ssl": false
    },
    "script_path": "/path/to/airflow/dags/airflow_failure_responder.py",
    "invoke": {
        "mode": "detach",
        "timeout_sec": 30
    }
}
```

### 3. Use in Your DAGs
```python
from callbacks.trigger_failure_responder import on_failure_trigger_fetcher

default_args = {
    "on_failure_callback": on_failure_trigger_fetcher,
}

with DAG(
    "my_dag",
    default_args=default_args,
    # ... other DAG parameters
) as dag:
    # Your tasks here
    pass
```

## 🔧 **Configuration Options**

### LLM Providers
- **OpenAI-like**: Set `driver: "openai_like"` with `base_url` and `api_key`
- **Ollama**: Set `driver: "ollama"` with `host: "http://localhost:11434"`
- **vLLM**: Set `driver: "vllm"` with `base_url`

### Output Methods
- **stdout**: Output analysis to console/logs (default)
  ```json
  "output": {
    "method": "stdout"
  }
  ```
- **file**: Save analysis to JSON file
  ```json
  "output": {
    "method": "file",
    "file_path": "/tmp/failed_task_log.json"
  }
  ```
- **teams**: Send Teams notification
  ```json
  "output": {
    "method": "teams",
    "teams_webhook": "https://api.powerplatform.com/...",
    "teams_verify_ssl": false
  }
  ```

### Script Execution
- **Mode**: `"detach"` (background) or `"run"` (blocking)
- **Timeout**: Maximum execution time in seconds
- **Python**: Custom Python interpreter path

## 📊 **Features**

- **Automatic Failure Detection**: Triggers on DAG/task failures
- **Log Analysis**: Fetches and analyzes Airflow task logs
- **AI Team Analysis**: Uses collaborative AI team for enhanced root cause analysis
  - **LogIngestor**: Specialized log analysis and summarization
  - **RootCauseAnalyst**: Expert root cause identification with confidence scoring
  - **FixPlanner**: Concrete fix steps and prevention measures
  - **Verifier**: Quality assurance and final report validation
- **Teams Integration**: Sends notifications to Microsoft Teams
- **Configurable**: Flexible configuration via Airflow Variables
- **Robust Error Handling**: Fallback mechanisms and retry logic
- **Collaborative Intelligence**: Multiple AI agents work together for better analysis

## 🧪 **Testing**

Use the example DAG to test the system:
```python
# The example DAG includes various failure scenarios
# Trigger it manually to test the failure callback system
```

## 📞 **Support**

- Check Airflow logs for callback execution
- Verify Airflow Variable configuration
- Test with the provided example DAG
- Monitor Teams notifications

---

**Production Ready**: This system is designed for production use with robust error handling and comprehensive logging.
