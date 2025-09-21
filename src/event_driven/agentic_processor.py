"""
Agentic processor for handling Airflow failures.

This module processes failure events by fetching logs and triggering
AI-powered analysis and recommendations.
"""

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

from agno.agent import Agent
from agno.models.openai import OpenAIChat

from .airflow_client import AirflowClient
from .config import AlertConfig

logger = logging.getLogger(__name__)


class AgenticFailureProcessor:
    """Processes Airflow failures using AI agents."""
    
    def __init__(self, config: AlertConfig) -> None:
        """Initialize the agentic failure processor.
        
        Args:
            config: Configuration for the alert system.
        """
        self.config = config
        self.airflow_client = AirflowClient(
            base_url=config.airflow_base_url,
            username=config.airflow_username,
            password=config.airflow_password
        )
        self.agent = self._build_agent()
    
    def _build_agent(self) -> Agent:
        """Build the AI agent for failure analysis.
        
        Returns:
            Configured Agent instance.
        """
        if self.config.llm_provider == "ollama":
            try:
                from agno.models.ollama import Ollama
                model = Ollama(
                    id=self.config.llm_model,
                    base_url=self.config.ollama_base_url,
                    temperature=0.2,
                    max_tokens=1200,
                )
            except ImportError as e:
                logger.error(f"Could not import Ollama model: {e}")
                raise
        else:
            model = OpenAIChat(
                id=self.config.llm_model,
                api_key=self.config.llm_api_key,
                base_url=self.config.llm_base_url,
                temperature=0.2,
                max_tokens=1200,
            )
        
        return Agent(
            model=model,
            name="Airflow Failure Analyst",
            instructions=[
                "You are an expert Apache Airflow and Data Engineering assistant.",
                "Given failed DAG runs and task logs, identify likely root causes and propose concise fixes.",
                "Prefer actionable steps. If logs are partial, call out assumptions.",
                "Output: brief root cause summary, 3-7 step fix list, and prevention tips.",
            ],
            markdown=True,
        )
    
    async def process_dag_failure(
        self,
        dag_id: str,
        dag_run_id: str,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """Process a DAG failure event.
        
        Args:
            dag_id: ID of the failed DAG.
            dag_run_id: ID of the failed DAG run.
            context: Additional context from Airflow.
        """
        try:
            logger.info(f"Processing DAG failure: {dag_id} - {dag_run_id}")
            
            # Get failed task instances
            task_instances = await self.airflow_client.list_task_instances(dag_id, dag_run_id)
            failed_tasks = [ti for ti in task_instances if ti.get("state") == "failed"]
            
            if not failed_tasks:
                logger.warning(f"No failed tasks found for DAG {dag_id} run {dag_run_id}")
                return
            
            # Collect failure information
            failure_info = {
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
                "failed_tasks": [],
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # Get logs for each failed task
            for task in failed_tasks[:3]:  # Limit to first 3 failed tasks
                task_id = task.get("task_id")
                try_number = task.get("try_number", 1)
                
                try:
                    log_content = await self.airflow_client.get_task_log(
                        dag_id, dag_run_id, task_id, try_number
                    )
                    
                    failure_info["failed_tasks"].append({
                        "task_id": task_id,
                        "try_number": try_number,
                        "state": task.get("state"),
                        "log_snippet": log_content[:2000] if log_content else "No logs available"
                    })
                except Exception as e:
                    logger.error(f"Failed to get logs for task {task_id}: {e}")
                    failure_info["failed_tasks"].append({
                        "task_id": task_id,
                        "try_number": try_number,
                        "state": task.get("state"),
                        "log_snippet": f"Error fetching logs: {e}"
                    })
            
            # Generate AI analysis
            await self._generate_analysis(failure_info)
            
        except Exception as e:
            logger.error(f"Error processing DAG failure: {e}", exc_info=True)
    
    async def process_task_failure(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        try_number: int,
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """Process a task failure event.
        
        Args:
            dag_id: ID of the DAG containing the failed task.
            dag_run_id: ID of the DAG run.
            task_id: ID of the failed task.
            try_number: Try number of the failed task.
            context: Additional context from Airflow.
        """
        try:
            logger.info(f"Processing task failure: {dag_id}.{task_id} (run: {dag_run_id}, try: {try_number})")
            
            # Get task log
            log_content = await self.airflow_client.get_task_log(
                dag_id, dag_run_id, task_id, try_number
            )
            
            # Collect failure information
            failure_info = {
                "dag_id": dag_id,
                "dag_run_id": dag_run_id,
                "failed_tasks": [{
                    "task_id": task_id,
                    "try_number": try_number,
                    "state": "failed",
                    "log_snippet": log_content[:2000] if log_content else "No logs available"
                }],
                "timestamp": datetime.utcnow().isoformat()
            }
            
            # Generate AI analysis
            await self._generate_analysis(failure_info)
            
        except Exception as e:
            logger.error(f"Error processing task failure: {e}", exc_info=True)
    
    async def _generate_analysis(self, failure_info: Dict[str, Any]) -> None:
        """Generate AI analysis for failure information.
        
        Args:
            failure_info: Dictionary containing failure details and logs.
        """
        try:
            # Build prompt
            prompt = self._build_analysis_prompt(failure_info)
            
            logger.info("Generating AI analysis for failure")
            
            # Get AI response
            response = await self.agent.arun(prompt)
            
            # Log the analysis
            logger.info("=== AI Failure Analysis ===")
            logger.info(response.content)
            logger.info("==========================")
            
            # Here you could also send notifications, store in database, etc.
            await self._handle_analysis_result(failure_info, response.content)
            
        except Exception as e:
            logger.error(f"Error generating analysis: {e}", exc_info=True)
    
    def _build_analysis_prompt(self, failure_info: Dict[str, Any]) -> str:
        """Build the prompt for AI analysis.
        
        Args:
            failure_info: Dictionary containing failure details.
            
        Returns:
            Formatted prompt string.
        """
        lines = [
            "# Airflow Failure Analysis",
            f"**DAG:** {failure_info['dag_id']}",
            f"**Run ID:** {failure_info['dag_run_id']}",
            f"**Timestamp:** {failure_info['timestamp']}",
            "",
            "## Failed Tasks:",
            ""
        ]
        
        for i, task in enumerate(failure_info['failed_tasks'], 1):
            lines.extend([
                f"### {i}. Task: {task['task_id']} (Try: {task['try_number']})",
                f"**State:** {task['state']}",
                "",
                "**Logs:**",
                "```log",
                task['log_snippet'],
                "```",
                ""
            ])
        
        lines.extend([
            "## Analysis Request:",
            "Please provide:",
            "1. **Root Cause:** Brief explanation of what likely caused the failure",
            "2. **Fix Steps:** Numbered list of concrete actions to resolve the issue",
            "3. **Prevention Tips:** Bullet points for preventing similar failures",
            "",
            "Keep the analysis concise and actionable."
        ])
        
        return "\n".join(lines)
    
    async def _handle_analysis_result(
        self,
        failure_info: Dict[str, Any],
        analysis: str
    ) -> None:
        """Handle the AI analysis result.
        
        Args:
            failure_info: Original failure information.
            analysis: AI-generated analysis.
        """
        # This is where you could:
        # - Send notifications (Slack, email, etc.)
        # - Store in database
        # - Create tickets
        # - Update monitoring dashboards
        # - etc.
        
        logger.info(f"Analysis completed for {failure_info['dag_id']} - {failure_info['dag_run_id']}")
        
        # Example: Store in a simple log file
        if self.config.output_file:
            try:
                with open(self.config.output_file, "a", encoding="utf-8") as f:
                    f.write(f"\n{'='*50}\n")
                    f.write(f"Timestamp: {datetime.utcnow().isoformat()}\n")
                    f.write(f"DAG: {failure_info['dag_id']}\n")
                    f.write(f"Run: {failure_info['dag_run_id']}\n")
                    f.write(f"Analysis:\n{analysis}\n")
            except Exception as e:
                logger.error(f"Failed to write analysis to file: {e}")
