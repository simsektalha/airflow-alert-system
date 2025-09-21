"""Tests for configuration management."""

import os
import pytest
from unittest.mock import patch

from src.event_driven.config import AlertConfig


class TestAlertConfig:
    """Test cases for AlertConfig class."""
    
    def test_default_config(self) -> None:
        """Test default configuration values."""
        config = AlertConfig()
        
        assert config.airflow_base_url == "http://localhost:8080"
        assert config.airflow_username == "admin"
        assert config.airflow_password == "admin"
        assert config.llm_provider == "openailike"
        assert config.llm_model == "gpt-4o-mini"
        assert config.ollama_base_url == "http://localhost:11434"
    
    def test_custom_config(self) -> None:
        """Test custom configuration values."""
        config = AlertConfig(
            airflow_base_url="http://custom-airflow:8080",
            airflow_username="custom_user",
            airflow_password="custom_pass",
            llm_provider="ollama",
            llm_model="llama3.1"
        )
        
        assert config.airflow_base_url == "http://custom-airflow:8080"
        assert config.airflow_username == "custom_user"
        assert config.airflow_password == "custom_pass"
        assert config.llm_provider == "ollama"
        assert config.llm_model == "llama3.1"
    
    @patch.dict(os.environ, {
        "AIRFLOW_BASE_URL": "http://env-airflow:8080",
        "LLM_PROVIDER": "ollama",
        "LLM_MODEL": "qwen2.5"
    })
    def test_env_override(self) -> None:
        """Test environment variable override."""
        config = AlertConfig()
        
        assert config.airflow_base_url == "http://env-airflow:8080"
        assert config.llm_provider == "ollama"
        assert config.llm_model == "qwen2.5"
    
    def test_validation_openailike_success(self) -> None:
        """Test successful validation for openailike provider."""
        config = AlertConfig(
            llm_provider="openailike",
            llm_base_url="https://api.openai.com/v1",
            llm_api_key="test-key"
        )
        
        # Should not raise any exception
        config.validate()
    
    def test_validation_openailike_missing_url(self) -> None:
        """Test validation failure for openailike provider missing URL."""
        config = AlertConfig(
            llm_provider="openailike",
            llm_api_key="test-key"
        )
        
        with pytest.raises(ValueError, match="LLM_BASE_URL is required"):
            config.validate()
    
    def test_validation_openailike_missing_key(self) -> None:
        """Test validation failure for openailike provider missing API key."""
        config = AlertConfig(
            llm_provider="openailike",
            llm_base_url="https://api.openai.com/v1"
        )
        
        with pytest.raises(ValueError, match="LLM_API_KEY is required"):
            config.validate()
    
    def test_validation_ollama_success(self) -> None:
        """Test successful validation for ollama provider."""
        config = AlertConfig(
            llm_provider="ollama",
            ollama_base_url="http://localhost:11434"
        )
        
        # Should not raise any exception
        config.validate()
    
    def test_validation_ollama_missing_url(self) -> None:
        """Test validation failure for ollama provider missing URL."""
        config = AlertConfig(llm_provider="ollama")
        
        with pytest.raises(ValueError, match="OLLAMA_BASE_URL is required"):
            config.validate()
    
    def test_validation_invalid_provider(self) -> None:
        """Test validation failure for invalid provider."""
        config = AlertConfig(llm_provider="invalid")
        
        with pytest.raises(ValueError, match="Invalid LLM_PROVIDER"):
            config.validate()
    
    def test_to_dict(self) -> None:
        """Test conversion to dictionary."""
        config = AlertConfig(
            airflow_base_url="http://test:8080",
            llm_provider="openailike",
            llm_base_url="https://api.test.com/v1",
            llm_api_key="secret-key"
        )
        
        result = config.to_dict()
        
        assert result["airflow_base_url"] == "http://test:8080"
        assert result["llm_provider"] == "openailike"
        assert result["llm_base_url"] == "https://api.test.com/v1"
        assert result["airflow_password"] == "***"  # Password should be hidden
        assert result["llm_api_key"] == "***"  # API key should be hidden
