"""
Unified configuration system for production integration.

This module provides a unified configuration approach that supports
both the new Agent Teams system and the existing production configuration.
"""

import json
import logging
import os
from typing import Any, Dict, Optional, Union

from airflow.models import Variable

from src.event_driven.config import AlertConfig

LOG = logging.getLogger(__name__)


class UnifiedConfig:
    """Unified configuration supporting both Agent Teams and legacy systems."""
    
    def __init__(self, use_agent_teams: bool = True) -> None:
        """Initialize unified configuration.
        
        Args:
            use_agent_teams: Whether to prioritize Agent Teams configuration.
        """
        self.use_agent_teams = use_agent_teams
        self.agent_config: Optional[AlertConfig] = None
        self.legacy_config: Optional[Dict[str, Any]] = None
        
        self._load_configurations()
    
    def _load_configurations(self) -> None:
        """Load both Agent Teams and legacy configurations."""
        # Load Agent Teams configuration
        if self.use_agent_teams:
            try:
                self.agent_config = AlertConfig()
                self.agent_config.validate()
                LOG.info("Agent Teams configuration loaded successfully")
            except Exception as e:
                LOG.warning(f"Failed to load Agent Teams configuration: {e}")
                self.agent_config = None
        
        # Load legacy configuration from Airflow Variable
        try:
            cfg_raw = Variable.get("v_callback_fetch_failed_task")
            self.legacy_config = json.loads(cfg_raw)
            LOG.info("Legacy configuration loaded successfully")
        except Exception as e:
            LOG.warning(f"Failed to load legacy configuration: {e}")
            self.legacy_config = None
    
    def get_agent_config(self) -> Optional[AlertConfig]:
        """Get Agent Teams configuration.
        
        Returns:
            AlertConfig instance if available, None otherwise.
        """
        return self.agent_config
    
    def get_legacy_config(self) -> Optional[Dict[str, Any]]:
        """Get legacy configuration.
        
        Returns:
            Legacy configuration dict if available, None otherwise.
        """
        return self.legacy_config
    
    def is_agent_teams_available(self) -> bool:
        """Check if Agent Teams is available and configured.
        
        Returns:
            True if Agent Teams is available, False otherwise.
        """
        return self.agent_config is not None
    
    def is_legacy_available(self) -> bool:
        """Check if legacy system is available and configured.
        
        Returns:
            True if legacy system is available, False otherwise.
        """
        return self.legacy_config is not None
    
    def get_analysis_method(self) -> str:
        """Get the recommended analysis method.
        
        Returns:
            'agent_teams', 'legacy', or 'heuristic' based on availability.
        """
        if self.is_agent_teams_available():
            return "agent_teams"
        elif self.is_legacy_available():
            return "legacy"
        else:
            return "heuristic"
    
    def get_airflow_config(self) -> Dict[str, Any]:
        """Get Airflow configuration from legacy config.
        
        Returns:
            Airflow configuration dict.
        """
        if not self.legacy_config:
            return {}
        
        return {
            "base_url": self.legacy_config.get("base_url", ""),
            "username": self.legacy_config.get("auth", {}).get("basic", {}).get("username", ""),
            "password": self.legacy_config.get("auth", {}).get("basic", {}).get("password", ""),
            "verify": self.legacy_config.get("tls", {}).get("verify", True),
            "timeouts": self.legacy_config.get("timeouts", {}),
        }
    
    def get_llm_config(self) -> Dict[str, Any]:
        """Get LLM configuration, prioritizing Agent Teams.
        
        Returns:
            LLM configuration dict.
        """
        # Try Agent Teams config first
        if self.agent_config:
            return {
                "provider": self.agent_config.llm_provider,
                "base_url": self.agent_config.llm_base_url,
                "api_key": self.agent_config.llm_api_key,
                "model": self.agent_config.llm_model,
            }
        
        # Fallback to legacy config
        if self.legacy_config and "llm" in self.legacy_config:
            llm_cfg = self.legacy_config["llm"]
            return {
                "provider": llm_cfg.get("driver", "openai_like"),
                "base_url": llm_cfg.get("base_url"),
                "api_key": llm_cfg.get("api_key"),
                "model": llm_cfg.get("model", "llama3.1"),
                "temperature": llm_cfg.get("temperature", 0.1),
                "max_tokens": llm_cfg.get("max_tokens", 800),
            }
        
        return {}
    
    def get_script_config(self) -> Dict[str, Any]:
        """Get script execution configuration from legacy config.
        
        Returns:
            Script configuration dict.
        """
        if not self.legacy_config:
            return {}
        
        return {
            "script_path": self.legacy_config.get("script_path", "/usr/local/airflow/dags/airflow_failure_responder.py"),
            "responder_args": self.legacy_config.get("responder_args", []),
            "invoke": self.legacy_config.get("invoke", {}),
        }
    
    def get_notification_config(self) -> Dict[str, Any]:
        """Get notification configuration.
        
        Returns:
            Notification configuration dict.
        """
        # This would be where Teams integration config goes
        # For now, return empty dict
        return {}
    
    def validate(self) -> bool:
        """Validate that at least one configuration is available.
        
        Returns:
            True if at least one configuration is valid, False otherwise.
        """
        return self.is_agent_teams_available() or self.is_legacy_available()
    
    def get_config_summary(self) -> Dict[str, Any]:
        """Get a summary of available configurations.
        
        Returns:
            Configuration summary dict.
        """
        return {
            "agent_teams_available": self.is_agent_teams_available(),
            "legacy_available": self.is_legacy_available(),
            "recommended_method": self.get_analysis_method(),
            "airflow_config": bool(self.get_airflow_config().get("base_url")),
            "llm_config": bool(self.get_llm_config().get("provider")),
            "script_config": bool(self.get_script_config().get("script_path")),
        }


def create_unified_config(use_agent_teams: bool = True) -> UnifiedConfig:
    """Create a unified configuration instance.
    
    Args:
        use_agent_teams: Whether to prioritize Agent Teams configuration.
        
    Returns:
        UnifiedConfig instance.
    """
    return UnifiedConfig(use_agent_teams=use_agent_teams)


def get_environment_config() -> Dict[str, Any]:
    """Get configuration from environment variables.
    
    Returns:
        Environment configuration dict.
    """
    return {
        "llm_provider": os.getenv("LLM_PROVIDER", "openailike"),
        "llm_base_url": os.getenv("LLM_BASE_URL"),
        "llm_api_key": os.getenv("LLM_API_KEY"),
        "llm_model": os.getenv("LLM_MODEL", "gpt-4o-mini"),
        "airflow_base_url": os.getenv("AIRFLOW_BASE_URL"),
        "airflow_username": os.getenv("AIRFLOW_USERNAME"),
        "airflow_password": os.getenv("AIRFLOW_PASSWORD"),
    }


def merge_configs(*configs: Dict[str, Any]) -> Dict[str, Any]:
    """Merge multiple configuration dictionaries.
    
    Args:
        *configs: Configuration dictionaries to merge.
        
    Returns:
        Merged configuration dict.
    """
    merged = {}
    for config in configs:
        if config:
            merged.update(config)
    return merged
