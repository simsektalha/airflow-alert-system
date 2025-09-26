# Airflow AI-Powered Failure Analysis System

An enterprise-grade Airflow failure analysis system that leverages AI-powered teams to automatically diagnose, analyze, and provide actionable insights for failed DAG tasks through intelligent root cause analysis, automated fix recommendations, and multi-channel notifications.

## 🚀 Quick Start

### 1. Deploy
```bash
cp airflow_failure_responder.py /path/to/airflow/dags/
cp dags/callbacks/trigger_failure_responder.py /path/to/airflow/dags/callbacks/
cp dags/example.py /path/to/airflow/dags/
```

### 2. Configure
Set up the `v_callback_fetch_failed_task` Airflow Variable:
```json
{
  "base_url": "https://your-airflow-instance:8080",
  "auth": { "basic": { "username": "user", "password": "pass" } },
  "llm": {
    "driver": "openai_like",
    "base_url": "https://api.openai.com/v1",
    "api_key": "your-api-key",
    "model": "gpt-4o-mini"
  },
  "output": { "method": "stdout" }
}
```

### 3. Use
```python
from callbacks.trigger_failure_responder import on_failure_trigger_fetcher

default_args = {
    "on_failure_callback": on_failure_trigger_fetcher,
}
```

## 📊 Features

- **🤖 AI Team Analysis**: Collaborative AI agents for enhanced root cause analysis
- **📄 PDF Knowledge Base**: Integrates custom Problem_Solutions.pdf for proven solutions
- **🔍 Hybrid Search**: Advanced vector search with multiple embedder options
- **📤 Multi-Channel Output**: stdout, file, or Microsoft Teams notifications
- **⚡ Event-Driven**: Automatic failure detection via Airflow callbacks
- **🔧 Configurable**: Flexible configuration via Airflow Variables

## 📁 Project Structure

```
airflow-alert-system/
├── airflow_failure_responder.py          # Main failure responder script
├── dags/
│   ├── callbacks/
│   │   └── trigger_failure_responder.py  # Airflow callback function
│   └── example.py                        # Example DAG with failure scenarios
├── docs/                                 # Detailed documentation
│   ├── CONFIGURATION.md                  # Complete configuration guide
│   ├── PDF_INTEGRATION.md               # PDF knowledge base setup
│   └── GPU_CLUSTER.md                   # GPU cluster embedder guide
└── README.md                            # This file
```

## 🔧 Configuration

### Basic Configuration
- **LLM Providers**: OpenAI, Ollama, vLLM, Custom GPU clusters
- **Output Methods**: stdout, file, Microsoft Teams
- **PDF Knowledge**: Custom Problem_Solutions.pdf integration
- **Vector Databases**: Chroma, PgVector, LanceDB
- **Vector Databases**: Chroma, PgVector, LanceDB

### Advanced Features
- **Hybrid Search**: Combines vector and keyword search
- **Custom Embedders**: GPU cluster embedding models
- **Async Loading**: Non-blocking PDF processing
- **Upsert Support**: Incremental knowledge base updates

## 📚 Documentation

- **[Configuration Guide](docs/CONFIGURATION.md)** - Complete configuration options
- **[PDF Integration](docs/PDF_INTEGRATION.md)** - PDF knowledge base setup
- **[GPU Cluster Setup](docs/GPU_CLUSTER.md)** - Custom embedder configuration

## 🗂️ LanceDB (Local Dev/Test Vector DB)

Use LanceDB to keep knowledge fully local without external services. Configure in your Airflow Variable JSON under `pdf_knowledge`:

```json
{
  "pdf_knowledge": {
    "path": "./docs/Problem_Solutions.pdf",
    "vector_db": {
      "type": "lancedb",
      "table_name": "pdf_documents_dev",
      "uri": ".dev_lancedb"
    },
    "embedder": {
      "type": "ollama",
      "host": "http://localhost:11434",
      "model": "nomic-embed-text"
    },
    "recreate": true,
    "upsert": true
  }
}
```

Notes:
- Set `uri` to a local folder (e.g., `.dev_lancedb`). Delete it to reset.
- For offline/dev, prefer a local embedder (e.g., `ollama`). You can also use any OpenAI-compatible embedder by setting `embedder.type` to `custom` and providing `base_url`.

## 🧪 Testing

Use the example DAG to test the system:
```python
# The example DAG includes various failure scenarios
# Trigger it manually to test the failure callback system
```

## 📞 Support

- Check Airflow logs for callback execution
- Verify Airflow Variable configuration
- Test with the provided example DAG
- Monitor output notifications

---

**Production Ready**: This system is designed for production use with robust error handling and comprehensive logging.