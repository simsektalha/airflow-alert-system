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
from agno.team import Team
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
        # Build agent team (role-specialized agents)
        self.agents = self._build_agents()
        self.team = Team(
            name="Airflow Failure Response Team",
            members=[
                self.agents["log_ingestor"],
                self.agents["root_cause_analyst"],
                self.agents["fix_planner"],
                self.agents["verifier"],
            ],
        )
    
    def _build_agents(self) -> Dict[str, Agent]:
        """Build a small team of role-specialized agents.
        
        Returns:
            Mapping of role name to configured Agent instance.
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
        
        # Define role-specialized agents
        log_ingestor = Agent(
            model=model,
            name="LogIngestor",
            instructions=[
                "You ingest raw Airflow task logs and produce a concise, lossless summary.",
                "Extract errors, stack traces, failing operators, retries, and timings.",
                "Output only the distilled summary (<= 15 lines).",
            ],
            markdown=True,
        )

        root_cause_analyst = Agent(
            model=model,
            name="RootCauseAnalyst",
            instructions=[
                "You are an expert in Apache Airflow and data engineering root-cause analysis.",
                "Given DAG context and a log summary, identify the most likely root cause.",
                "Call out assumptions and missing context explicitly.",
                "Output a short paragraph and a confidence score (0-1).",
            ],
            markdown=True,
        )

        fix_planner = Agent(
            model=model,
            name="FixPlanner",
            instructions=[
                "You produce concrete, minimal-risk fix steps for Airflow failures.",
                "Return a numbered list of 3-7 steps and 3-5 prevention tips.",
                "Prefer configuration and code diffs that are realistically applicable.",
            ],
            markdown=True,
        )

        verifier = Agent(
            model=model,
            name="Verifier",
            instructions=[
                "You verify that the root cause and plan are consistent with the log summary.",
                "If something is off, propose a safer alternative plan.",
                "Output a final, concise report: Root cause, Fix steps, Prevention tips.",
            ],
            markdown=True,
        )

        return {
            "log_ingestor": log_ingestor,
            "root_cause_analyst": root_cause_analyst,
            "fix_planner": fix_planner,
            "verifier": verifier,
        }
    
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
            # Run the collaborative analysis via the Agno Team
            final_report = await self._run_team_collaboration(failure_info)

            # Log the analysis
            logger.info("=== AI Failure Analysis (Team) ===")
            logger.info(final_report)
            logger.info("==================================")
            
            # Here you could also send notifications, store in database, etc.
            await self._handle_analysis_result(failure_info, final_report)
            
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
    
    async def _run_team_collaboration(self, failure_info: Dict[str, Any]) -> str:
        """Run the Agno Team to collaboratively analyze the failure.
        
        The team members are expected to implicitly coordinate: the LogIngestor
        summarizes logs, the RootCauseAnalyst proposes the likely cause, the
        FixPlanner drafts actionable steps, and the Verifier finalizes the
        consolidated report.
        
        Args:
            failure_info: Failure payload containing DAG, run, and log snippets.
        
        Returns:
            Final report string produced by the team.
        """
        # Compose a single collaborative prompt for the team
        header = [
            "# Airflow Failure: Team Collaboration",
            f"DAG: {failure_info['dag_id']}",
            f"Run: {failure_info['dag_run_id']}",
            f"Timestamp: {failure_info['timestamp']}",
            "",
            "## Failed Task Logs",
        ]

        for task in failure_info.get("failed_tasks", []):
            header.extend([
                f"### Task: {task['task_id']} (Try: {task['try_number']})",
                "```log",
                task.get("log_snippet", "No logs available"),
                "```",
                "",
            ])

        instructions = [
            "Team process:",
            "1) LogIngestor: summarize the logs precisely (<= 15 lines).",
            "2) RootCauseAnalyst: infer the most likely root cause with confidence (0-1).",
            "3) FixPlanner: propose 3-7 concrete fix steps and 3-5 prevention tips.",
            "4) Verifier: check consistency and output the final consolidated report.",
            "",
            "Final output format (by Verifier only):",
            "- Root cause (<= 5 lines)",
            "- Fix steps (numbered)",
            "- Prevention tips (bullets)",
        ]

        prompt = "\n".join(header + instructions)
        response = await self.team.arun(prompt)
        return response.content

    async def _ingest_logs(self, failure_info: Dict[str, Any]) -> str:
        """Use LogIngestor to summarize failed task logs.
        
        Args:
            failure_info: Failure payload containing task log snippets.
        
        Returns:
            Concise log summary string.
        """
        lines: List[str] = [
            "# Raw Log Snippets",
        ]
        for task in failure_info.get("failed_tasks", []):
            lines.extend([
                f"## Task: {task['task_id']} (Try: {task['try_number']})",
                "```log",
                task.get("log_snippet", ""),
                "```",
            ])

        prompt = "\n".join(lines + [
            "\nSummarize the logs focusing on errors, stack traces, failing operators, retries, and timings."
        ])
        resp = await self.agents["log_ingestor"].arun(prompt)
        return resp.content

    async def _analyze_root_cause(self, failure_info: Dict[str, Any], log_summary: str) -> str:
        """Use RootCauseAnalyst to produce a concise root cause.
        
        Args:
            failure_info: Failure context.
            log_summary: Output from log ingestion.
        
        Returns:
            Root cause narrative with confidence.
        """
        context = [
            f"DAG: {failure_info['dag_id']}",
            f"Run: {failure_info['dag_run_id']}",
            f"When: {failure_info['timestamp']}",
            "",
            "## Log Summary",
            log_summary,
        ]
        resp = await self.agents["root_cause_analyst"].arun("\n".join(context))
        return resp.content

    async def _plan_fixes(self, failure_info: Dict[str, Any], log_summary: str, rca: str) -> str:
        """Use FixPlanner to generate concrete steps and prevention tips.
        
        Args:
            failure_info: Failure context.
            log_summary: Summarized logs.
            rca: Root-cause analysis text.
        
        Returns:
            Actionable plan text.
        """
        prompt = "\n".join([
            f"DAG: {failure_info['dag_id']} | Run: {failure_info['dag_run_id']}",
            "",
            "## Root Cause",
            rca,
            "",
            "## Log Summary",
            log_summary,
            "",
            "Produce numbered Fix Steps (3-7) and Prevention Tips (3-5)."
        ])
        resp = await self.agents["fix_planner"].arun(prompt)
        return resp.content

    async def _verify_and_format(
        self,
        failure_info: Dict[str, Any],
        log_summary: str,
        rca: str,
        plan: str,
    ) -> str:
        """Use Verifier to validate and compose the final report.
        
        Args:
            failure_info: Failure context.
            log_summary: Summarized logs.
            rca: Root-cause analysis text.
            plan: Fix plan text.
        
        Returns:
            Final formatted report string.
        """
        prompt = "\n".join([
            f"DAG: {failure_info['dag_id']} | Run: {failure_info['dag_run_id']} | Time: {failure_info['timestamp']}",
            "",
            "## Log Summary",
            log_summary,
            "",
            "## Root Cause (candidate)",
            rca,
            "",
            "## Plan (candidate)",
            plan,
            "",
            "Verify consistency. If risky or inconsistent, adjust. Output final:",
            "- Root cause (<= 5 lines)",
            "- Fix steps (numbered)",
            "- Prevention tips (bullets)",
        ])
        resp = await self.agents["verifier"].arun(prompt)
        return resp.content

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
