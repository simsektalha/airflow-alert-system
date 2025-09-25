# Production Migration Guide

This guide explains how to migrate your existing production Airflow AI failure callbacks to the enhanced Agent Teams system.

## 🎯 **Migration Overview**

Your production system has been enhanced with:
- **Agent Teams**: Collaborative AI analysis with specialized agents
- **Unified Configuration**: Single configuration supporting both systems
- **Backward Compatibility**: Existing functionality preserved
- **Automatic Fallback**: Graceful degradation if Agent Teams fails

## 📁 **What Changed**

### Files Modified
- `production/dags/callbacks/trigger_failure_responder.py` - Enhanced with Agent Teams
- `production/dags/example.py` - Updated to use new system
- `production/airflow_failure_responder.py` - Added Agent Teams support

### Files Added
- `production/unified_config.py` - Unified configuration system
- `production/test_integration.py` - Integration tests
- `production/MIGRATION_GUIDE.md` - This guide

## 🚀 **Migration Steps**

### Step 1: Install Dependencies
```bash
# Install the enhanced system
pip install -e .

# Install Agent Teams dependencies (if not already installed)
pip install agno httpx
```

### Step 2: Configure Agent Teams (Optional)
Add these environment variables for Agent Teams:
```bash
# For OpenAI-compatible APIs
export LLM_PROVIDER="openailike"
export LLM_BASE_URL="https://api.openai.com/v1"
export LLM_API_KEY="your-api-key"
export LLM_MODEL="gpt-4o-mini"

# For Ollama (local)
export LLM_PROVIDER="ollama"
export LLM_BASE_URL="http://localhost:11434"
export LLM_MODEL="llama3.1"
```

### Step 3: Test the Integration
```bash
# Run integration tests
python production/test_integration.py
```

### Step 4: Deploy to Airflow
```bash
# Copy enhanced files to your Airflow DAGs directory
cp production/dags/callbacks/trigger_failure_responder.py /path/to/airflow/dags/callbacks/
cp production/dags/example.py /path/to/airflow/dags/
cp production/airflow_failure_responder.py /path/to/airflow/dags/
cp production/unified_config.py /path/to/airflow/dags/
```

### Step 5: Update Your DAGs
Update your existing DAGs to use the enhanced callback:

```python
# Before (your existing code)
from callbacks.trigger_failure_responder import on_failure_trigger_fetcher

# After (enhanced version - same import, enhanced functionality)
from callbacks.trigger_failure_responder import on_failure_trigger_fetcher

# The callback now automatically uses Agent Teams if available
# and falls back to your existing system if not
```

## 🔧 **Configuration Options**

### Option 1: Agent Teams Only (Recommended)
```python
# Set environment variables for Agent Teams
export LLM_PROVIDER="openailike"
export LLM_BASE_URL="https://api.openai.com/v1"
export LLM_API_KEY="your-api-key"

# Your existing DAG code works unchanged
# The system will automatically use Agent Teams
```

### Option 2: Legacy Only (Keep Existing Behavior)
```python
# Don't set Agent Teams environment variables
# The system will automatically use your existing configuration
# from the v_callback_fetch_failed_task Airflow Variable
```

### Option 3: Hybrid (Agent Teams with Legacy Fallback)
```python
# Set Agent Teams environment variables
# Keep your existing v_callback_fetch_failed_task Airflow Variable
# The system will try Agent Teams first, then fall back to legacy
```

## 📊 **What You Get**

### Enhanced Analysis
- **Collaborative AI**: 4 specialized agents work together
- **Better Root Cause Analysis**: More accurate failure diagnosis
- **Structured Output**: Consistent JSON responses
- **Confidence Scoring**: AI confidence in analysis results

### Backward Compatibility
- **Existing Configuration**: Your `v_callback_fetch_failed_task` variable still works
- **Same Interface**: No changes to your DAG code required
- **Teams Integration**: Your existing Teams notifications continue to work
- **Error Handling**: Robust fallback mechanisms

### Improved Reliability
- **Automatic Fallback**: If Agent Teams fails, uses your existing system
- **Configuration Validation**: Validates both systems before use
- **Enhanced Logging**: Better visibility into what's happening
- **Error Recovery**: Graceful handling of failures

## 🧪 **Testing**

### Run Integration Tests
```bash
python production/test_integration.py
```

### Test in Airflow
1. Deploy the enhanced files to your Airflow environment
2. Trigger a test DAG that will fail
3. Check the logs for Agent Teams analysis
4. Verify Teams notifications still work

### Monitor Performance
- Check Airflow logs for configuration summary
- Monitor analysis quality improvements
- Verify fallback behavior when needed

## 🔍 **Troubleshooting**

### Agent Teams Not Working
- Check environment variables: `LLM_PROVIDER`, `LLM_BASE_URL`, `LLM_API_KEY`
- Verify LLM service is accessible
- Check logs for Agent Teams initialization errors
- System will automatically fallback to legacy

### Legacy System Not Working
- Check your `v_callback_fetch_failed_task` Airflow Variable
- Verify Airflow API connectivity
- Check script paths and permissions

### Both Systems Failing
- Check Airflow callback configuration
- Verify DAG permissions and access
- Check logs for detailed error messages

## 📈 **Performance Impact**

### Positive Changes
- **Better Analysis**: More accurate root cause identification
- **Faster Resolution**: Better fix recommendations
- **Reduced Manual Work**: Automated analysis and suggestions

### Potential Considerations
- **LLM API Calls**: Agent Teams makes API calls to LLM services
- **Processing Time**: Slightly longer analysis time for better results
- **Resource Usage**: Minimal additional memory usage

## 🔄 **Rollback Plan**

If you need to rollback to the original system:

1. **Keep Original Files**: Your original files are preserved
2. **Remove Environment Variables**: Unset Agent Teams environment variables
3. **Restore Original Callback**: Copy your original callback file back
4. **Test**: Verify the system works as before

## 📞 **Support**

### Logs to Check
- Airflow task logs for callback execution
- Airflow scheduler logs for configuration loading
- LLM service logs for API calls

### Common Issues
- **Import Errors**: Ensure all files are in the correct paths
- **Configuration Errors**: Check environment variables and Airflow Variables
- **Permission Issues**: Verify file permissions and Airflow access

### Getting Help
- Check the integration test output for configuration issues
- Review logs for specific error messages
- Test with a simple DAG first before deploying to production

## 🎉 **Success Criteria**

Your migration is successful when:
- ✅ Integration tests pass
- ✅ Agent Teams analysis works (if configured)
- ✅ Legacy fallback works (if Agent Teams fails)
- ✅ Teams notifications continue to work
- ✅ Existing DAGs work without modification
- ✅ Analysis quality is improved

## 🔮 **Next Steps**

After successful migration:
1. **Monitor Performance**: Track analysis quality and response times
2. **Tune Configuration**: Adjust LLM settings based on results
3. **Expand Usage**: Apply to more DAGs as confidence grows
4. **Customize Agents**: Modify agent instructions for your specific use cases
5. **Add Features**: Consider additional enhancements like custom notifications
