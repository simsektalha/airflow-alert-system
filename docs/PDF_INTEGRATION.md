# PDF Knowledge Base Integration

Guide for integrating your custom `Problem_Solutions.pdf` with the sequential AI team for enhanced problem-solving.

## 🎯 Overview

The PDF knowledge base allows your AI agents to reference documented problem-solution pairs from your `Problem_Solutions.pdf` file, providing more accurate and consistent failure analysis. The sequential AI team actively searches this knowledge base to find proven solutions before creating new ones.

## 🤖 AI Team Integration

The PDF knowledge base is used by the sequential AI team as follows:

- **RootCauseAnalyst**: Searches for similar error patterns and documented solutions
- **FixPlanner**: Finds proven solutions from your PDF documentation first
- **Verifier**: Validates solutions against documented best practices

This ensures that all solutions are based on your documented, proven methods rather than generic AI responses.

## 📋 Configuration

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
  "upsert": true,
  "async_load": false
}
```

### Advanced Configuration (PgVector + OpenAI)
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
    "api_key": "your-embedder-api-key",
    "base_url": "https://api.openai.com/v1",
    "id": "text-embedding-3-small"
  },
  "recreate": false,
  "upsert": true,
  "async_load": true
}
```

## 🔧 Configuration Options

### PDF Path
- **path**: Absolute path to your Problem_Solutions.pdf file
- Must be accessible by the Airflow process
- Supports local files only

### Vector Database

#### Chroma (Default)
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
  "db_url": "postgresql+psycopg://user:pass@host:port/db"
}
```

### Embedder Configuration

#### OpenAI Embedder
```json
"embedder": {
  "type": "openai",
  "api_key": "your-api-key",
  "base_url": "https://api.openai.com/v1",
  "model": "text-embedding-3-small"
}
```

#### Ollama Embedder
```json
"embedder": {
  "type": "ollama",
  "host": "http://localhost:11434",
  "model": "nomic-embed-text"
}
```

#### Custom GPU Cluster Embedder
```json
"embedder": {
  "type": "custom",
  "base_url": "http://your-gpu-cluster:8000/v1",
  "model": "your-embedding-model",
  "api_key": "your-api-key",
  "headers": {
    "Authorization": "Bearer your-token"
  },
  "timeout": 60
}
```

### Processing Options
- **recreate**: Whether to recreate the knowledge base (default: false)
- **upsert**: Whether to update existing documents (default: true)
- **async_load**: Whether to load asynchronously (default: false)

## 🚀 How It Works

### 1. PDF Processing
- PDF is chunked into manageable pieces
- Each chunk is converted to vector embeddings
- Embeddings are stored in the vector database

### 2. Agent Integration
- All AI agents can search the knowledge base
- Agents reference similar problems and solutions
- Enhanced analysis based on documented experiences

### 3. Search Process
- Hybrid search combines vector and keyword search
- Agents find relevant problem-solution pairs
- Context is used to improve analysis accuracy

## 📊 Benefits

### Enhanced Analysis
- **Proven Solutions**: Reference documented problem-solution pairs
- **Consistent Approach**: Standardized problem-solving methodology
- **Historical Context**: Learn from past experiences
- **Accuracy**: Better root cause analysis with documented cases

### Performance
- **Fast Retrieval**: Vector search for quick relevant document finding
- **Scalable**: Handle large PDF documents efficiently
- **Cached**: Embeddings are cached for performance
- **Incremental**: Update knowledge base without full recreation

## 🔧 Setup Instructions

### 1. Prepare PDF
- Ensure your `Problem_Solutions.pdf` is well-structured
- Include clear problem descriptions and solutions
- Use consistent formatting for better chunking

### 2. Configure Vector Database
- Choose between Chroma (local) or PgVector (database)
- Ensure sufficient storage space
- Configure appropriate access permissions

### 3. Set Up Embedder
- Choose embedding model based on your needs
- Configure API keys and endpoints
- Test embedding generation

### 4. Test Integration
- Deploy configuration
- Trigger test failure
- Verify PDF knowledge is being used

## 🧪 Testing

### Test PDF Loading
```python
# Check logs for PDF loading messages
# Verify no errors during knowledge base creation
```

### Test Agent Integration
```python
# Trigger a failure and check if agents reference PDF content
# Look for knowledge base search results in logs
```

### Test Search Quality
```python
# Verify agents find relevant problem-solution pairs
# Check if analysis quality improves with PDF knowledge
```

## 🔧 Troubleshooting

### Common Issues

#### PDF Not Found
```
[pdf] PDF file not found: /path/to/Problem_Solutions.pdf
```
**Solution**: Verify PDF path and file permissions

#### Embedder Errors
```
[pdf] Failed to setup PDF knowledge base: Embedder error
```
**Solution**: Check embedder configuration and API keys

#### Vector Database Errors
```
[pdf] Failed to setup PDF knowledge base: Vector DB error
```
**Solution**: Verify vector database configuration and connectivity

### Debug Mode
Enable debug logging to see detailed PDF processing information:
```json
{
  "logging": {
    "level": "DEBUG"
  }
}
```

## 📈 Performance Optimization

### Large PDFs
- Use `async_load: true` for large PDFs
- Consider chunking strategy for better performance
- Monitor memory usage during processing

### Frequent Updates
- Use `upsert: true` for incremental updates
- Avoid `recreate: true` unless necessary
- Consider separate knowledge bases for different PDFs

### Search Performance
- Use hybrid search for better accuracy
- Optimize embedding model for your use case
- Consider vector database indexing

## 🔄 Maintenance

### Updating PDF
1. Replace PDF file
2. Set `recreate: true` or `upsert: true`
3. Restart Airflow or trigger callback

### Monitoring
- Monitor knowledge base size
- Check search performance
- Verify agent integration

### Backup
- Backup vector database regularly
- Keep PDF file backups
- Document configuration changes
