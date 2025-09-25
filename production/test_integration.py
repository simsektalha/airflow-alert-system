"""
Integration test for production system with Agent Teams.

This script tests the integration between the production system
and the new Agent Teams approach.
"""

import asyncio
import json
import logging
import os
import sys
from typing import Any, Dict

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from production.unified_config import create_unified_config
from production.airflow_failure_responder import build_agent_team_from_cfg, build_agent_from_cfg

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
LOG = logging.getLogger("integration_test")


def test_unified_configuration() -> None:
    """Test unified configuration loading."""
    LOG.info("=== Testing Unified Configuration ===")
    
    try:
        # Test with Agent Teams enabled
        config = create_unified_config(use_agent_teams=True)
        
        # Check configuration summary
        summary = config.get_config_summary()
        LOG.info(f"Configuration summary: {json.dumps(summary, indent=2)}")
        
        # Test individual components
        agent_config = config.get_agent_config()
        legacy_config = config.get_legacy_config()
        llm_config = config.get_llm_config()
        airflow_config = config.get_airflow_config()
        
        LOG.info(f"Agent Teams available: {config.is_agent_teams_available()}")
        LOG.info(f"Legacy system available: {config.is_legacy_available()}")
        LOG.info(f"Recommended method: {config.get_analysis_method()}")
        
        # Validate configuration
        is_valid = config.validate()
        LOG.info(f"Configuration valid: {is_valid}")
        
        if not is_valid:
            LOG.warning("No valid configuration found - this is expected in test environment")
        
    except Exception as e:
        LOG.error(f"Configuration test failed: {e}")


def test_agent_teams_initialization() -> None:
    """Test Agent Teams initialization."""
    LOG.info("=== Testing Agent Teams Initialization ===")
    
    try:
        # Create test configuration
        test_config = {
            "llm": {
                "driver": "ollama",
                "host": "http://localhost:11434",
                "model": "llama3.1",
                "temperature": 0.1,
                "max_tokens": 800,
            }
        }
        
        # Test Agent Teams
        team = build_agent_team_from_cfg(test_config)
        if team:
            LOG.info("Agent Teams initialized successfully")
            LOG.info(f"Team name: {team.name}")
            LOG.info(f"Team members: {[member.name for member in team.members]}")
        else:
            LOG.warning("Agent Teams not available (agno not installed or config invalid)")
        
        # Test single agent
        agent = build_agent_from_cfg(test_config)
        if agent:
            LOG.info("Single agent initialized successfully")
            LOG.info(f"Agent name: {agent.name}")
        else:
            LOG.warning("Single agent not available (agno not installed or config invalid)")
            
    except Exception as e:
        LOG.error(f"Agent Teams test failed: {e}")


async def test_analysis_flow() -> None:
    """Test the analysis flow with mock data."""
    LOG.info("=== Testing Analysis Flow ===")
    
    try:
        # Mock failure data
        mock_error_focus = """
Traceback (most recent call last):
  File "/usr/local/airflow/dags/test_dag.py", line 15, in <module>
    import nonexistent_module
ModuleNotFoundError: No module named 'nonexistent_module'
"""
        
        mock_log_tail = """
[2025-01-01 10:00:00] INFO - Starting task execution
[2025-01-01 10:00:01] ERROR - Task failed with ModuleNotFoundError
[2025-01-01 10:00:01] INFO - Task execution completed
"""
        
        mock_identifiers = {
            "dag_id": "test_dag",
            "dag_run_id": "test_run_001",
            "task_id": "test_task",
            "try_number": 1,
        }
        
        # Test configuration
        test_config = {
            "llm": {
                "driver": "ollama",
                "host": "http://localhost:11434",
                "model": "llama3.1",
                "temperature": 0.1,
                "max_tokens": 800,
            }
        }
        
        # Test Agent Teams analysis
        team = build_agent_team_from_cfg(test_config)
        if team:
            LOG.info("Testing Agent Teams analysis...")
            try:
                from production.airflow_failure_responder import ask_agent_team_for_analysis
                result = await ask_agent_team_for_analysis(
                    team=team,
                    error_focus=mock_error_focus,
                    log_tail=mock_log_tail,
                    identifiers=mock_identifiers
                )
                LOG.info(f"Agent Teams analysis result: {json.dumps(result, indent=2)}")
            except Exception as e:
                LOG.warning(f"Agent Teams analysis failed: {e}")
        
        # Test single agent analysis
        agent = build_agent_from_cfg(test_config)
        if agent:
            LOG.info("Testing single agent analysis...")
            try:
                from production.airflow_failure_responder import ask_llm_for_analysis
                result = await ask_llm_for_analysis(
                    agent=agent,
                    error_focus=mock_error_focus,
                    log_tail=mock_log_tail,
                    identifiers=mock_identifiers
                )
                LOG.info(f"Single agent analysis result: {json.dumps(result, indent=2)}")
            except Exception as e:
                LOG.warning(f"Single agent analysis failed: {e}")
        
    except Exception as e:
        LOG.error(f"Analysis flow test failed: {e}")


def test_callback_integration() -> None:
    """Test callback integration."""
    LOG.info("=== Testing Callback Integration ===")
    
    try:
        # Test callback import
        from production.dags.callbacks.trigger_failure_responder import on_failure_trigger_fetcher
        
        LOG.info("Callback imported successfully")
        
        # Test callback function exists
        assert callable(on_failure_trigger_fetcher), "Callback should be callable"
        LOG.info("Callback function is callable")
        
        # Test callback signature (without actually calling it)
        import inspect
        sig = inspect.signature(on_failure_trigger_fetcher)
        LOG.info(f"Callback signature: {sig}")
        
    except Exception as e:
        LOG.error(f"Callback integration test failed: {e}")


def test_dag_integration() -> None:
    """Test DAG integration."""
    LOG.info("=== Testing DAG Integration ===")
    
    try:
        # Test DAG import
        from production.dags.example import dag
        
        LOG.info("DAG imported successfully")
        LOG.info(f"DAG ID: {dag.dag_id}")
        LOG.info(f"DAG description: {dag.description}")
        
        # Test DAG has callbacks
        if hasattr(dag, 'on_failure_callback') and dag.on_failure_callback:
            LOG.info("DAG has failure callback configured")
        else:
            LOG.warning("DAG does not have failure callback configured")
        
        # Test DAG tasks
        task_count = len(dag.tasks)
        LOG.info(f"DAG has {task_count} tasks")
        
        for task_id, task in dag.tasks.items():
            LOG.info(f"Task: {task_id} - {type(task).__name__}")
            if hasattr(task, 'on_failure_callback') and task.on_failure_callback:
                LOG.info(f"  - Has failure callback")
        
    except Exception as e:
        LOG.error(f"DAG integration test failed: {e}")


async def main() -> None:
    """Run all integration tests."""
    LOG.info("Starting integration tests...")
    
    # Test configuration
    test_unified_configuration()
    
    # Test Agent Teams
    test_agent_teams_initialization()
    
    # Test analysis flow
    await test_analysis_flow()
    
    # Test callback integration
    test_callback_integration()
    
    # Test DAG integration
    test_dag_integration()
    
    LOG.info("Integration tests completed!")


if __name__ == "__main__":
    asyncio.run(main())
