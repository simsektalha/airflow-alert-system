"""
Configuration management for the event-driven alert system.

This module handles configuration loading from environment variables
and provides default values for all settings.
"""

import os
from typing import Optional


class AlertConfig:
    """Configuration for the Airflow AI alert system."""
    
    def __init__(
        self,
        airflow_base_url: Optional[str] = None,
        airflow_username: Optional[str] = None,
        airflow_password: Optional[str] = None,
        llm_provider: Optional[str] = None,
        llm_base_url: Optional[str] = None,
        llm_api_key: Optional[str] = None,
        llm_model: Optional[str] = None,
        ollama_base_url: Optional[str] = None,
        output_file: Optional[str] = None
    ) -> None:
        """Initialize configuration with provided values or environment variables.
        
        Args:
            airflow_base_url: Airflow instance base URL.
            airflow_username: Airflow username.
            airflow_password: Airflow password.
            llm_provider: LLM provider ('openailike' or 'ollama').
            llm_base_url: LLM API base URL (for openailike provider).
            llm_api_key: LLM API key (for openailike provider).
            llm_model: LLM model name.
            ollama_base_url: Ollama server base URL (for ollama provider).
            output_file: Optional file path to write analysis results.
        """
        # Airflow configuration
        self.airflow_base_url = (
            airflow_base_url or 
            os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080")
        )
        self.airflow_username = (
            airflow_username or 
            os.getenv("AIRFLOW_USERNAME", "admin")
        )
        self.airflow_password = (
            airflow_password or 
            os.getenv("AIRFLOW_PASSWORD", "admin")
        )
        
        # LLM configuration
        self.llm_provider = (
            llm_provider or 
            os.getenv("LLM_PROVIDER", "openailike")
        ).lower()
        
        self.llm_base_url = (
            llm_base_url or 
            os.getenv("LLM_BASE_URL", "")
        )
        self.llm_api_key = (
            llm_api_key or 
            os.getenv("LLM_API_KEY", "")
        )
        self.llm_model = (
            llm_model or 
            os.getenv("LLM_MODEL", "gpt-4o-mini")
        )
        
        self.ollama_base_url = (
            ollama_base_url or 
            os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
        )
        
        # Output configuration
        self.output_file = (
            output_file or 
            os.getenv("ALERT_OUTPUT_FILE", "airflow_analysis.log")
        )
    
    def validate(self) -> None:
        """Validate the configuration.
        
        Raises:
            ValueError: If required configuration is missing or invalid.
        """
        if not self.airflow_base_url:
            raise ValueError("AIRFLOW_BASE_URL is required")
        
        if not self.airflow_username:
            raise ValueError("AIRFLOW_USERNAME is required")
        
        if not self.airflow_password:
            raise ValueError("AIRFLOW_PASSWORD is required")
        
        if self.llm_provider == "openailike":
            if not self.llm_base_url:
                raise ValueError("LLM_BASE_URL is required for openailike provider")
            if not self.llm_api_key:
                raise ValueError("LLM_API_KEY is required for openailike provider")
        elif self.llm_provider == "ollama":
            if not self.ollama_base_url:
                raise ValueError("OLLAMA_BASE_URL is required for ollama provider")
        else:
            raise ValueError(f"Invalid LLM_PROVIDER: {self.llm_provider}. Must be 'openailike' or 'ollama'")
    
    def to_dict(self) -> dict:
        """Convert configuration to dictionary.
        
        Returns:
            Dictionary representation of the configuration.
        """
        return {
            "airflow_base_url": self.airflow_base_url,
            "airflow_username": self.airflow_username,
            "airflow_password": "***",  # Hide password
            "llm_provider": self.llm_provider,
            "llm_base_url": self.llm_base_url,
            "llm_api_key": "***" if self.llm_api_key else "",  # Hide API key
            "llm_model": self.llm_model,
            "ollama_base_url": self.ollama_base_url,
            "output_file": self.output_file
        }
