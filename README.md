# Airflow AI-Powered Failure Callbacks - Production

An enterprise-grade Airflow failure analysis system that leverages AI-powered teams to automatically diagnose, analyze, and provide actionable insights for failed DAG tasks. This production-ready solution integrates seamlessly with Airflow's callback system to deliver intelligent root cause analysis, automated fix recommendations, and multi-channel notifications.

## 📁 **Project Structure**

```
airflow-alert-system/
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
cp airflow_failure_responder.py /path/to/airflow/dags/

# Copy callback
cp dags/callbacks/trigger_failure_responder.py /path/to/airflow/dags/callbacks/

# Copy example DAG
cp dags/example.py /path/to/airflow/dags/
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
    "pdf_knowledge": {
        "path": "/path/to/Problem_Solutions.pdf",
        "vector_db": {
            "type": "chroma",
            "collection_name": "pdf_documents",
            "persist_directory": "./chroma_db"
        },
        "embedder": {
            "type": "openai",
            "api_key": "your-embedder-api-key",
            "base_url": "https://api.openai.com/v1",
            "model": "text-embedding-3-small"
        },
        "recreate": false,
        "upsert": true,
        "async_load": false
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

### PDF Knowledge Base
- **path**: Path to your Problem_Solutions.pdf file
- **vector_db**: Vector database configuration
  - **type**: `"chroma"` (default) or `"pgvector"`
  - **collection_name**: Collection name for document storage
  - **persist_directory**: Directory to persist Chroma database
- **embedder**: Embedding model configuration
  - **type**: `"openai"` (default) or `"ollama"`
  - **api_key**: API key for OpenAI embedder
  - **base_url**: Base URL for embedder API
  - **model**: Embedding model name
- **recreate**: Whether to recreate the knowledge base (default: false)
- **upsert**: Whether to update existing documents (default: true)
- **async_load**: Whether to load asynchronously (default: false)

**Example with PgVector:**
```json
"pdf_knowledge": {
  "path": "/path/to/Problem_Solutions.pdf",
  "vector_db": {
    "type": "pgvector",
    "table_name": "pdf_documents",
    "db_url": "postgresql+psycopg://ai:ai@localhost:5532/ai"
  },
  "embedder": {
    "type": "openai",
    "api_key": "your-api-key",
    "model": "text-embedding-3-small"
  },
  "recreate": false,
  "upsert": true
}
```

**Example with Ollama Embedder:**
```json
"pdf_knowledge": {
  "path": "/path/to/Problem_Solutions.pdf",
  "vector_db": {
    "type": "chroma",
    "collection_name": "pdf_documents",
    "persist_directory": "./chroma_db"
  },
  "embedder": {
    "type": "ollama",
    "host": "http://localhost:11434",
    "model": "nomic-embed-text"
  },
  "recreate": false,
  "upsert": true,
  "async_load": true
}
```

### Script Execution
- **Mode**: `"detach"` (background) or `"run"` (blocking)
- **Timeout**: Maximum execution time in seconds
- **Python**: Custom Python interpreter path

### Script Arguments
The responder script takes only the required arguments passed by the callback:
- `--log-url`: Airflow logs API endpoint
- `--config-b64`: Base64-encoded configuration
- `--dag-id`: DAG identifier
- `--dag-run-id`: DAG run identifier
- `--task-id`: Task identifier
- `--try-number`: Task try number

All other settings (log processing, output paths, etc.) are configured via the Airflow Variable.

## 📊 **Features**

- **Automatic Failure Detection**: Triggers on DAG/task failures
- **Log Analysis**: Fetches and analyzes Airflow task logs
- **AI Team Analysis**: Uses collaborative AI team for enhanced root cause analysis
  - **LogIngestor**: Specialized log analysis and summarization
  - **RootCauseAnalyst**: Expert root cause identification with confidence scoring
  - **FixPlanner**: Concrete fix steps and prevention measures
  - **Verifier**: Quality assurance and final report validation
- **PDF Knowledge Base**: Integrates custom Problem_Solutions.pdf for enhanced problem-solving
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
