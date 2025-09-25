# GPU Cluster Embedder Setup

Guide for configuring custom embedding models served on your GPU cluster.

## 🎯 Overview

This guide covers setting up custom embedding models on your GPU cluster to provide high-performance, cost-effective embeddings for the PDF knowledge base.

## 🚀 Supported Solutions

### vLLM
High-performance LLM serving with embedding support.

### TGI (Text Generation Inference)
Hugging Face's inference server for embedding models.

### Custom FastAPI
Your own embedding service implementation.

### Kubernetes
Deploy embedding models at scale.

## 📋 Configuration

### Basic Custom Embedder
```json
"pdf_knowledge": {
  "embedder": {
    "type": "custom",
    "base_url": "http://your-gpu-cluster:8000/v1",
    "model": "your-embedding-model",
    "api_key": "your-api-key",
    "timeout": 60
  }
}
```

### Advanced Configuration
```json
"pdf_knowledge": {
  "embedder": {
    "type": "custom",
    "base_url": "http://your-gpu-cluster:8000/v1",
    "model": "your-embedding-model",
    "api_key": "your-api-key",
    "headers": {
      "Authorization": "Bearer your-token",
      "X-Custom-Header": "value"
    },
    "timeout": 60
  }
}
```

## 🔧 API Requirements

### Required Endpoint Format
Your GPU cluster must expose an OpenAI-compatible embedding API:

```
POST /v1/embeddings
Content-Type: application/json
Authorization: Bearer your-api-key

{
  "input": "text to embed",
  "model": "your-embedding-model"
}
```

### Response Format
```json
{
  "data": [
    {
      "embedding": [0.1, 0.2, 0.3, ...],
      "index": 0
    }
  ],
  "model": "your-embedding-model",
  "usage": {
    "prompt_tokens": 10,
    "total_tokens": 10
  }
}
```

## 🛠️ Setup Examples

### vLLM Setup

#### 1. Install vLLM
```bash
pip install vllm
```

#### 2. Start vLLM Server
```bash
python -m vllm.entrypoints.openai.api_server \
  --model your-embedding-model \
  --port 8000 \
  --host 0.0.0.0
```

#### 3. Configure
```json
"embedder": {
  "type": "custom",
  "base_url": "http://localhost:8000/v1",
  "model": "your-embedding-model"
}
```

### TGI Setup

#### 1. Install TGI
```bash
pip install text-generation-inference
```

#### 2. Start TGI Server
```bash
text-generation-launcher \
  --model-id your-embedding-model \
  --port 8000 \
  --hostname 0.0.0.0
```

#### 3. Configure
```json
"embedder": {
  "type": "custom",
  "base_url": "http://localhost:8000/v1",
  "model": "your-embedding-model"
}
```

### Custom FastAPI Setup

#### 1. Create FastAPI Service
```python
from fastapi import FastAPI
from pydantic import BaseModel
import torch
from transformers import AutoModel, AutoTokenizer

app = FastAPI()
model = AutoModel.from_pretrained("your-embedding-model")
tokenizer = AutoTokenizer.from_pretrained("your-embedding-model")

class EmbeddingRequest(BaseModel):
    input: str
    model: str

@app.post("/v1/embeddings")
async def create_embeddings(request: EmbeddingRequest):
    inputs = tokenizer(request.input, return_tensors="pt")
    with torch.no_grad():
        outputs = model(**inputs)
        embeddings = outputs.last_hidden_state.mean(dim=1)
    
    return {
        "data": [{"embedding": embeddings[0].tolist(), "index": 0}],
        "model": request.model,
        "usage": {"prompt_tokens": len(inputs["input_ids"][0]), "total_tokens": len(inputs["input_ids"][0])}
    }
```

#### 2. Run Service
```bash
uvicorn app:app --host 0.0.0.0 --port 8000
```

#### 3. Configure
```json
"embedder": {
  "type": "custom",
  "base_url": "http://localhost:8000/v1",
  "model": "your-embedding-model"
}
```

## 🐳 Docker Setup

### vLLM Docker
```dockerfile
FROM vllm/vllm-openai:latest

CMD ["python", "-m", "vllm.entrypoints.openai.api_server", \
     "--model", "your-embedding-model", \
     "--port", "8000", \
     "--host", "0.0.0.0"]
```

### TGI Docker
```dockerfile
FROM ghcr.io/huggingface/text-generation-inference:latest

CMD ["text-generation-launcher", \
     "--model-id", "your-embedding-model", \
     "--port", "8000", \
     "--hostname", "0.0.0.0"]
```

## ☸️ Kubernetes Setup

### vLLM Deployment
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: vllm-embedder
spec:
  replicas: 1
  selector:
    matchLabels:
      app: vllm-embedder
  template:
    metadata:
      labels:
        app: vllm-embedder
    spec:
      containers:
      - name: vllm
        image: vllm/vllm-openai:latest
        command: ["python", "-m", "vllm.entrypoints.openai.api_server"]
        args: ["--model", "your-embedding-model", "--port", "8000", "--host", "0.0.0.0"]
        ports:
        - containerPort: 8000
        resources:
          requests:
            nvidia.com/gpu: 1
          limits:
            nvidia.com/gpu: 1
---
apiVersion: v1
kind: Service
metadata:
  name: vllm-embedder-service
spec:
  selector:
    app: vllm-embedder
  ports:
  - port: 8000
    targetPort: 8000
  type: LoadBalancer
```

## 🔧 Configuration Options

### Basic Options
- **base_url**: URL of your GPU cluster embedding service
- **model**: Name of the embedding model
- **api_key**: API key for authentication (optional)

### Advanced Options
- **headers**: Custom headers for requests
- **timeout**: Request timeout in seconds
- **retries**: Number of retry attempts
- **backoff_factor**: Exponential backoff factor

### Example with All Options
```json
"embedder": {
  "type": "custom",
  "base_url": "http://your-gpu-cluster:8000/v1",
  "model": "your-embedding-model",
  "api_key": "your-api-key",
  "headers": {
    "Authorization": "Bearer your-token",
    "X-Custom-Header": "value"
  },
  "timeout": 60,
  "retries": 3,
  "backoff_factor": 2
}
```

## 🧪 Testing

### Test Endpoint
```bash
curl -X POST "http://your-gpu-cluster:8000/v1/embeddings" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-api-key" \
  -d '{
    "input": "test text",
    "model": "your-embedding-model"
  }'
```

### Test Configuration
```python
# Check logs for embedder initialization
# Verify no errors during PDF processing
# Test with small PDF first
```

## 🔧 Troubleshooting

### Common Issues

#### Connection Errors
```
[pdf] Failed to setup PDF knowledge base: Connection error
```
**Solution**: Verify base_url and network connectivity

#### Authentication Errors
```
[pdf] Failed to setup PDF knowledge base: Authentication error
```
**Solution**: Check api_key and headers configuration

#### Model Errors
```
[pdf] Failed to setup PDF knowledge base: Model error
```
**Solution**: Verify model name and availability

#### Timeout Errors
```
[pdf] Failed to setup PDF knowledge base: Timeout error
```
**Solution**: Increase timeout value or check server performance

### Debug Mode
Enable debug logging to see detailed embedder information:
```json
{
  "logging": {
    "level": "DEBUG"
  }
}
```

## 📈 Performance Optimization

### GPU Utilization
- Monitor GPU memory usage
- Optimize batch sizes
- Use appropriate model precision

### Network Optimization
- Use local network for GPU cluster
- Optimize request batching
- Consider connection pooling

### Caching
- Implement embedding caching
- Use Redis for distributed caching
- Cache frequently used embeddings

## 🔄 Maintenance

### Monitoring
- Monitor GPU cluster health
- Check embedding service status
- Monitor performance metrics

### Updates
- Update embedding models
- Restart services as needed
- Monitor for model drift

### Backup
- Backup model configurations
- Keep service configurations
- Document deployment procedures
