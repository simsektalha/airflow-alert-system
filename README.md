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

- **🤖 Sequential AI Team**: Specialized agents working in sequence for focused analysis
- **📄 PDF Knowledge Base**: Integrates custom Problem_Solutions.pdf for proven solutions
- **🔍 Smart Search**: Agents search knowledge base first for documented solutions
- **📤 Multi-Channel Output**: stdout, file, or Microsoft Teams notifications
- **⚡ Event-Driven**: Automatic failure detection via Airflow callbacks
- **🔧 Configurable**: Flexible configuration via Airflow Variables
- **🎯 Focused Workflow**: Each agent has specific input/output responsibilities

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

## 🤖 AI Team Workflow

The system uses a sequential team of specialized AI agents:

### 1. **LogIngestor**
- **Input**: Raw Airflow logs
- **Process**: Extracts and summarizes error information
- **Output**: Clean error summary (max 10 lines)

### 2. **RootCauseAnalyst**
- **Input**: Error summary from LogIngestor
- **Process**: Searches knowledge base for similar patterns, identifies root cause
- **Output**: Root cause analysis with category and confidence

### 3. **FixPlanner**
- **Input**: Root cause analysis from RootCauseAnalyst
- **Process**: Searches knowledge base for documented solutions, creates fix plan
- **Output**: Specific fix steps and prevention measures

### 4. **Verifier**
- **Input**: All previous agent outputs
- **Process**: Validates solutions against knowledge base, consolidates
- **Output**: Final JSON response in required schema

## 🔧 Configuration

### Basic Configuration
- **LLM Providers**: OpenAI, Ollama, vLLM, Custom GPU clusters
- **Output Methods**: stdout, file, Microsoft Teams
- **PDF Knowledge**: Custom Problem_Solutions.pdf integration
- **Vector Databases**: Chroma, PgVector, LanceDB (default: LanceDB)

### Advanced Features
- **Smart Search**: Agents search knowledge base first for documented solutions
- **Custom Embedders**: GPU cluster embedding models
- **Async Loading**: Non-blocking PDF processing
- **Upsert Support**: Incremental knowledge base updates

## 📚 Documentation

- **[Configuration Guide](docs/CONFIGURATION.md)** - Complete configuration options
- **[PDF Integration](docs/PDF_INTEGRATION.md)** - PDF knowledge base setup
- **[GPU Cluster Setup](docs/GPU_CLUSTER.md)** - Custom embedder configuration

## 🗂️ Default Configuration (LanceDB + Ollama)

The system now defaults to LanceDB (vector database) and Ollama (embedder) for local development:

```json
{
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
    "recreate": true,
    "upsert": true
  }
}
```

**Key Changes:**
- **Default Vector DB**: LanceDB (was Chroma)
- **Default Embedder**: Ollama (was OpenAI)
- **Optional Host**: Ollama host parameter is optional (defaults to localhost:11434)
- **Model Parameter**: Changed from `model` to `id` for Ollama embedder

**Notes:**
- Set `uri` to a local folder (e.g., `./lancedb`). Delete it to reset.
- Ollama host is optional - if not provided, defaults to `http://localhost:11434`
- For offline/dev, this configuration keeps everything local

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