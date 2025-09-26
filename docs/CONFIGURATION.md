# Configuration Guide

Complete configuration options for the Airflow AI-Powered Failure Analysis System.

## 📋 Configuration Structure

The system uses a single Airflow Variable `v_callback_fetch_failed_task` containing JSON configuration:

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
      "type": "lancedb",
      "table_name": "vectors",
      "uri": "./lancedb"
    },
    "embedder": {
      "type": "ollama",
      "id": "nomic-embed-text"
    },
    "recreate": false,
    "upsert": true,
    "async_load": false
  },
  "script_path": "/path/to/airflow/dags/airflow_failure_responder.py",
  "invoke": {
    "mode": "detach",
    "timeout_sec": 30,
    "python": "/usr/bin/python3"
  }
}
```

## 🔧 Configuration Sections

### Airflow Connection
- **base_url**: Airflow instance URL
- **auth.basic**: Username and password for Airflow API
- **tls.verify**: SSL verification (true/false or CA bundle path)
- **timeouts**: Connection and total timeout settings

### LLM Providers

#### OpenAI-like
```json
"llm": {
  "driver": "openai_like",
  "base_url": "https://api.openai.com/v1",
  "api_key": "sk-...",
  "model": "gpt-4o-mini",
  "temperature": 0.1,
  "max_tokens": 800
}
```

#### Ollama
```json
"llm": {
  "driver": "ollama",
  "host": "http://localhost:11434",
  "model": "llama3.1"
}
```

#### vLLM
```json
"llm": {
  "driver": "vllm",
  "base_url": "http://localhost:8000/v1",
  "model": "your-model"
}
```

### Output Methods

#### stdout (Default)
```json
"output": {
  "method": "stdout"
}
```

#### File Output
```json
"output": {
  "method": "file",
  "file_path": "/tmp/failed_task_log.json"
}
```

#### Microsoft Teams
```json
"output": {
  "method": "teams",
  "teams_webhook": "https://api.powerplatform.com/...",
  "teams_verify_ssl": false
}
```

### Script Execution
- **mode**: `"detach"` (background) or `"run"` (blocking)
- **timeout_sec**: Maximum execution time in seconds
- **python**: Custom Python interpreter path

## 🤖 AI Team Configuration

The system uses a sequential team of specialized AI agents:

### Agent Roles
- **LogIngestor**: Extracts error information from raw logs
- **RootCauseAnalyst**: Searches knowledge base for similar patterns, identifies root cause
- **FixPlanner**: Finds documented solutions from knowledge base, creates fix plan
- **Verifier**: Validates solutions against knowledge base, outputs final JSON

### Knowledge Base Integration
All agents have access to the PDF knowledge base and will:
1. Search for similar problems and solutions first
2. Reference documented solutions from your PDF files
3. Validate solutions against best practices in the knowledge base

## 📄 PDF Knowledge Base

### Default Configuration (LanceDB + Ollama)
```json
"pdf_knowledge": {
  "path": "./docs/Problem_Solutions.pdf",
  "vector_db": {
    "type": "lancedb",
    "table_name": "vectors",
    "uri": "./lancedb"
  },
  "embedder": {
    "type": "ollama",
    "id": "nomic-embed-text"
  },
  "recreate": false,
  "upsert": true
}
```

### Vector Database Options

#### LanceDB (Default)
```json
"vector_db": {
  "type": "lancedb",
  "table_name": "vectors",
  "uri": "./lancedb"
}
```

#### Chroma
```json
"vector_db": {
  "type": "chroma",
  "collection_name": "pdf_documents",
  "persist_directory": "./chroma_db"
}
```

#### PgVector
```json
"vector_db": {
  "type": "pgvector",
  "table_name": "pdf_documents",
  "db_url": "postgresql+psycopg://user:pass@localhost:5432/db"
}
```

### Embedder Options

#### Ollama (Default)
```json
"embedder": {
  "type": "ollama",
  "id": "nomic-embed-text"
}
```

#### OpenAI
```json
"embedder": {
  "type": "openai",
  "api_key": "your-api-key",
  "base_url": "https://api.openai.com/v1",
  "id": "text-embedding-3-small"
}
```

See [PDF Integration Guide](PDF_INTEGRATION.md) for detailed PDF configuration.

## 🖥️ GPU Cluster Embedders

See [GPU Cluster Setup](GPU_CLUSTER.md) for custom embedder configuration.

## 🔍 Advanced Options

### Log Processing
- **tail_lines**: Number of tail lines to preserve (default: 160)
- **max_log_chars**: Maximum characters for log tail (default: 1800)

### Error Handling
- **retry_attempts**: Number of retry attempts for failed requests
- **backoff_factor**: Exponential backoff factor for retries

### Performance
- **async_load**: Load PDF knowledge base asynchronously
- **upsert**: Update existing documents instead of recreating
- **recreate**: Force recreation of knowledge base

## 🧪 Testing Configuration

Test your configuration with the example DAG:
```python
# Trigger the example DAG to test your configuration
# Check Airflow logs for any configuration errors
```

## 🔧 Troubleshooting

### Common Issues
1. **Authentication Errors**: Verify username/password and base_url
2. **LLM Errors**: Check API keys and model availability
3. **PDF Errors**: Ensure PDF file exists and is accessible
4. **Output Errors**: Verify file paths and Teams webhook URLs

### Debug Mode
Enable debug logging by setting log level to DEBUG in your Airflow configuration.

### Validation
The system validates configuration on startup and logs any issues found.
