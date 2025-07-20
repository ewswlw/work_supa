"""
🎯 PIPELINE MANAGER
==================
Comprehensive pipeline orchestration with dependency management,
error recovery, and monitoring capabilities.
"""

import asyncio
import subprocess
from typing import Dict, List, Optional, Any, NamedTuple
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import dataclass
from enum import Enum
import yaml
import sys
import pandas as pd

from ..utils.logging import LogManager
from ..utils.data_analyzer import analyze_pipeline_data


class PipelineStage(Enum):
    """Pipeline execution stages with dependency order."""
    UNIVERSE = "universe"
    PORTFOLIO = "portfolio"
    HISTORICAL_GSPREAD = "historical-gspread"
    GSPREAD_ANALYTICS = "gspread-analytics"
    RUNS_EXCEL = "runs-excel"
    RUNS_MONITOR = "runs-monitor"


@dataclass
class PipelineResult:
    """Result of pipeline execution."""
    stage: PipelineStage
    success: bool
    duration: timedelta
    records_processed: int
    output_files: List[str]
    error_message: Optional[str] = None
    warnings: List[str] = None


@dataclass
class ExecutionPlan:
    """Complete pipeline execution plan."""
    stages: List[PipelineStage]
    dependencies: Dict[PipelineStage, List[PipelineStage]]
    parallel_groups: List[List[PipelineStage]]
    estimated_duration: timedelta


@dataclass
class ExecutionResults:
    """Complete pipeline execution results."""
    success: bool
    total_duration: timedelta
    stage_results: Dict[PipelineStage, PipelineResult]
    total_records: int
    total_files: int


class PipelineManager:
    """
    Master pipeline orchestrator with comprehensive management capabilities.
    
    Features:
    - Dependency-aware execution
    - Parallel processing where possible
    - Error recovery and retry logic
    - Real-time monitoring
    - Comprehensive logging and reporting
    """
    
    # Define pipeline dependencies
    DEPENDENCIES = {
        PipelineStage.UNIVERSE: [],
        PipelineStage.PORTFOLIO: [],
        PipelineStage.HISTORICAL_GSPREAD: [],
        PipelineStage.RUNS_EXCEL: [],
        PipelineStage.RUNS_MONITOR: [PipelineStage.RUNS_EXCEL]
    }
    
    # Estimated execution times (in minutes)
    ESTIMATED_TIMES = {
        PipelineStage.UNIVERSE: 2,
        PipelineStage.PORTFOLIO: 3,
        PipelineStage.HISTORICAL_GSPREAD: 5,
        PipelineStage.RUNS_EXCEL: 4,
        PipelineStage.RUNS_MONITOR: 2
    }
    
    # Pipeline script mappings
    SCRIPT_MAPPINGS = {
        PipelineStage.UNIVERSE: "universe/universe_raw_to_parquet.py",
        PipelineStage.PORTFOLIO: "portfolio/portfolio_excel_to_parquet.py",
        PipelineStage.HISTORICAL_GSPREAD: "historical g spread/g_z.py",
        PipelineStage.RUNS_EXCEL: "runs/excel_to_df_debug.py",
        PipelineStage.RUNS_MONITOR: "runs/run_monitor.py"
    }
    
    def __init__(self, config, logger: LogManager):
        self.config = config
        self.logger = logger
        self.results: Dict[PipelineStage, PipelineResult] = {}
        
    def create_execution_plan(self, args) -> ExecutionPlan:
        """Create optimized execution plan based on arguments."""
        self.logger.info("[PLAN] Creating pipeline execution plan...")
        
        # Determine which stages to run
        stages = self._determine_stages(args)
        
        # Calculate dependencies
        dependencies = {stage: self.DEPENDENCIES[stage] for stage in stages}
        
        # Create parallel execution groups
        parallel_groups = self._create_parallel_groups(stages, dependencies)
        
        # Estimate total duration
        estimated_duration = self._estimate_duration(stages, parallel_groups)
        
        plan = ExecutionPlan(
            stages=stages,
            dependencies=dependencies,
            parallel_groups=parallel_groups,
            estimated_duration=estimated_duration
        )
        
        self.logger.info(f"[PLAN] Execution plan created:")
        self.logger.info(f"  Stages: {len(stages)}")
        self.logger.info(f"  Parallel groups: {len(parallel_groups)}")
        self.logger.info(f"  Estimated duration: {estimated_duration}")
        
        # Display detailed execution plan
        self.display_execution_plan(plan)
        
        return plan
    
    def _determine_stages(self, args) -> List[PipelineStage]:
        """Determine which pipeline stages to execute."""
        if args.full:
            return list(PipelineStage)
        
        stages = []
        if args.universe:
            stages.append(PipelineStage.UNIVERSE)
        if args.portfolio:
            stages.append(PipelineStage.PORTFOLIO)
        if getattr(args, 'historical_gspread', False):
            stages.append(PipelineStage.HISTORICAL_GSPREAD)
        if args.runs:
            stages.extend([PipelineStage.RUNS_EXCEL, PipelineStage.RUNS_MONITOR])
        
        # Handle resume_from
        if getattr(args, 'resume_from', None):
            try:
                resume_stage = PipelineStage(args.resume_from)
                all_stages = list(PipelineStage)
                resume_index = all_stages.index(resume_stage)
                stages = all_stages[resume_index:]
            except ValueError:
                self.logger.warning(f"Invalid resume stage: {args.resume_from}")
        
        # If no specific stages selected, default to full pipeline
        if not stages:
            # Filter out removed stages (GSPREAD_ANALYTICS was removed)
            stages = [stage for stage in PipelineStage if stage != PipelineStage.GSPREAD_ANALYTICS]
        
        return stages
    
    def _create_parallel_groups(self, stages: List[PipelineStage], 
                               dependencies: Dict[PipelineStage, List[PipelineStage]]) -> List[List[PipelineStage]]:
        """Create groups of stages that can run in parallel."""
        groups = []
        remaining = set(stages)
        completed = set()
        
        while remaining:
            # Find stages that can run now (all dependencies met)
            ready = []
            for stage in remaining:
                if all(dep in completed or dep not in stages for dep in dependencies[stage]):
                    ready.append(stage)
            
            if not ready:
                # This shouldn't happen with valid dependencies
                raise ValueError(f"Circular dependency detected in stages: {remaining}")
            
            groups.append(ready)
            completed.update(ready)
            remaining -= set(ready)
        
        return groups
    
    def _estimate_duration(self, stages: List[PipelineStage], 
                          parallel_groups: List[List[PipelineStage]]) -> timedelta:
        """Estimate total execution duration."""
        total_minutes = 0
        for group in parallel_groups:
            # For parallel groups, take the maximum time
            group_time = max(self.ESTIMATED_TIMES[stage] for stage in group)
            total_minutes += group_time
        
        return timedelta(minutes=total_minutes)
    
    def display_execution_plan(self, plan: ExecutionPlan):
        """Display detailed execution plan."""
        self.logger.info("[PLAN] PIPELINE EXECUTION PLAN")
        self.logger.info("=" * 50)
        
        for i, group in enumerate(plan.parallel_groups, 1):
            if len(group) == 1:
                stage_time = self.ESTIMATED_TIMES[group[0]]
                self.logger.info(f"Step {i}: {group[0].value} (~{stage_time}m)")
            else:
                max_time = max(self.ESTIMATED_TIMES[stage] for stage in group)
                stage_names = [stage.value for stage in group]
                self.logger.info(f"Step {i}: {', '.join(stage_names)} (parallel, ~{max_time}m)")
        
        self.logger.info("=" * 50)
        self.logger.info(f"Total estimated time: {plan.estimated_duration}")
        self.logger.info(f"Total stages: {len(plan.stages)}")
    
    async def execute_pipeline(self, plan: ExecutionPlan, args) -> ExecutionResults:
        """Execute the complete pipeline according to the plan."""
        self.logger.info("[START] Starting pipeline execution...")
        
        start_time = datetime.now()
        overall_success = True
        
        for i, group in enumerate(plan.parallel_groups, 1):
            self.logger.info(f"[STEP] Executing step {i}/{len(plan.parallel_groups)}: {[s.value for s in group]}")
            
            if len(group) == 1 or not getattr(args, 'parallel', False):
                # Sequential execution
                for stage in group:
                    result = await self._execute_stage(stage, args)
                    self.results[stage] = result
                    if not result.success:
                        overall_success = False
                        if not getattr(args, 'force', False):
                            self.logger.error(f"[FAIL] Stage {stage.value} failed, stopping execution")
                            break
            else:
                # Parallel execution
                tasks = [self._execute_stage(stage, args) for stage in group]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for stage, result in zip(group, results):
                    if isinstance(result, Exception):
                        self.results[stage] = PipelineResult(
                            stage=stage,
                            success=False,
                            duration=timedelta(0),
                            records_processed=0,
                            output_files=[],
                            error_message=str(result)
                        )
                        overall_success = False
                    else:
                        self.results[stage] = result
                        if not result.success:
                            overall_success = False
        
        total_duration = datetime.now() - start_time
        
        return ExecutionResults(
            success=overall_success,
            total_duration=total_duration,
            stage_results=self.results,
            total_records=sum(r.records_processed for r in self.results.values()),
            total_files=sum(len(r.output_files) for r in self.results.values())
        )
    
    async def _execute_stage(self, stage: PipelineStage, args=None) -> PipelineResult:
        """Execute a single pipeline stage."""
        self.logger.info(f"[RUN] Executing {stage.value}...")
        start_time = datetime.now()
        
        try:
            # Get the script path for this stage
            script_path = self.SCRIPT_MAPPINGS.get(stage)
            if not script_path:
                raise ValueError(f"No script mapping found for stage: {stage}")
            
            if not Path(script_path).exists():
                raise FileNotFoundError(f"Script not found: {script_path}")
            
            # Execute the pipeline script using poetry
            cmd = ["poetry", "run", "python", script_path]
            
            # Add force-full-refresh flag if requested
            if args and getattr(args, 'force_full_refresh', False):
                cmd.append("--force-full-refresh")
                self.logger.debug(f"[FLAG] Adding --force-full-refresh flag to {stage.value}")
            
            self.logger.debug(f"[CMD] Executing command: {' '.join(cmd)}")
            
            # Run the subprocess
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=Path.cwd()
            )
            
            stdout, stderr = await process.communicate()
            
            duration = datetime.now() - start_time
            
            # Parse output to extract metrics
            try:
                stdout_text = stdout.decode('utf-8')
            except UnicodeDecodeError:
                try:
                    stdout_text = stdout.decode('utf-8-sig')
                except UnicodeDecodeError:
                    stdout_text = stdout.decode('latin1')
            
            records_processed = self._extract_record_count(stdout_text, stage)
            output_files = self._extract_output_files(stdout_text, stage)
            
            success = process.returncode == 0
            if stderr and not success:
                try:
                    error_message = stderr.decode('utf-8')
                except UnicodeDecodeError:
                    try:
                        error_message = stderr.decode('utf-8-sig')
                    except UnicodeDecodeError:
                        error_message = stderr.decode('latin1')
            else:
                error_message = None
            
            result = PipelineResult(
                stage=stage,
                success=success,
                duration=duration,
                records_processed=records_processed,
                output_files=output_files,
                error_message=error_message
            )
            
            if success:
                self.logger.info(f"[OK] {stage.value} completed in {duration}")
                self.logger.info(f"   Records processed: {records_processed:,}")
                self.logger.info(f"   Output files: {len(output_files)}")
            else:
                self.logger.error(f"[FAIL] {stage.value} failed after {duration}")
                if error_message:
                    self.logger.error(f"   Error: {error_message}")
            
            return result
            
        except Exception as e:
            duration = datetime.now() - start_time
            error_msg = f"Stage {stage.value} failed: {str(e)}"
            self.logger.error(error_msg, exc=e)
            
            return PipelineResult(
                stage=stage,
                success=False,
                duration=duration,
                records_processed=0,
                output_files=[],
                error_message=error_msg
            )
    
    def _extract_record_count(self, output: str, stage: PipelineStage) -> int:
        """Extract record count from pipeline output."""
        # Look for common patterns in output
        patterns = [
            "records",
            "rows", 
            "Total rows in final dataset:",
            "Final dataset:",
            "shape:",
            "Loaded"
        ]
        
        lines = output.split('\n')
        for line in lines:
            line_lower = line.lower()
            for pattern in patterns:
                if pattern in line_lower:
                    # Try to extract numbers from the line
                    import re
                    numbers = re.findall(r'[\d,]+', line)
                    if numbers:
                        try:
                            # Take the largest number found
                            return max(int(num.replace(',', '')) for num in numbers)
                        except ValueError:
                            continue
        
        return 0
    
    def _extract_output_files(self, output: str, stage: PipelineStage) -> List[str]:
        """Extract output file paths from pipeline output."""
        output_files = []
        
        # Common output file patterns based on stage
        stage_patterns = {
            PipelineStage.UNIVERSE: ["universe.parquet", "universe_processed.csv"],
            PipelineStage.PORTFOLIO: ["portfolio.parquet"],
            PipelineStage.HISTORICAL_GSPREAD: ["bond_z.parquet"],
            PipelineStage.RUNS_EXCEL: ["combined_runs.parquet"],
            PipelineStage.RUNS_MONITOR: ["run_monitor.parquet", "run_monitor.csv"]
        }
        
        patterns = stage_patterns.get(stage, [])
        for pattern in patterns:
            # Check if file exists
            for ext in ['.parquet', '.csv']:
                if pattern.endswith(ext):
                    # Look for the file in common locations
                    possible_paths = [
                        f"{stage.value.replace('-', ' ')}/{pattern}",
                        f"runs/{pattern}",
                        pattern
                    ]
                    
                    for path in possible_paths:
                        if Path(path).exists():
                            output_files.append(path)
                            break
        
        return output_files
    
    def generate_execution_report(self, results: ExecutionResults):
        """Generate comprehensive execution report."""
        self.logger.info("[REPORT] PIPELINE EXECUTION REPORT")
        self.logger.info("=" * 60)
        self.logger.info(f"Overall Success: {'[OK]' if results.success else '[FAIL]'}")
        self.logger.info(f"Total Duration: {results.total_duration}")
        self.logger.info(f"Total Records Processed: {results.total_records:,}")
        self.logger.info(f"Total Output Files: {results.total_files}")
        self.logger.info("")
        
        self.logger.info("Stage Results:")
        self.logger.info("-" * 40)
        
        for stage, result in results.stage_results.items():
            status = "[OK]" if result.success else "[FAIL]"
            self.logger.info(f"{status} {stage.value}:")
            self.logger.info(f"   Duration: {result.duration}")
            self.logger.info(f"   Records: {result.records_processed:,}")
            self.logger.info(f"   Files: {len(result.output_files)}")
            
            if result.error_message:
                self.logger.info(f"   Error: {result.error_message}")
            
            self.logger.info("")
        
        self.logger.info("=" * 60)
    
    def load_processed_data(self) -> Dict[str, pd.DataFrame]:
        """
        Load all processed data files for analysis.
        
        Returns:
            Dictionary of {table_name: dataframe}
        """
        self.logger.info("[ANALYSIS] Loading processed data for analysis...")
        
        table_data = {}
        
        # Define expected data files and their table names
        data_files = {
            'universe': 'universe/universe.parquet',
            'portfolio': 'portfolio/portfolio.parquet',
            'runs': 'runs/combined_runs.parquet',
            'run_monitor': 'runs/run_monitor.parquet',
            'g_spread': 'historical g spread/bond_z.parquet'  # Single g-spread analysis output
        }
        
        for table_name, file_path in data_files.items():
            try:
                if Path(file_path).exists():
                    df = pd.read_parquet(file_path)
                    table_data[table_name] = df
                    self.logger.info(f"  ✅ Loaded {table_name}: {df.shape[0]:,} rows × {df.shape[1]} columns")
                else:
                    self.logger.info(f"  ⚠️  File not found: {file_path}")
            except Exception as e:
                self.logger.warning(f"  ❌ Failed to load {table_name}: {str(e)}")
        
        self.logger.info(f"[ANALYSIS] Loaded {len(table_data)} tables for analysis")
        return table_data
    
    def analyze_processed_data(self, table_data: Dict[str, pd.DataFrame]) -> str:
        """
        Analyze processed data and return formatted output.
        
        Args:
            table_data: Dictionary of {table_name: dataframe}
            
        Returns:
            Formatted analysis output
        """
        if not table_data:
            return "📊 No data available for analysis"
        
        self.logger.info("[ANALYSIS] Starting comprehensive data analysis...")
        
        try:
            # Run complete analysis
            analysis_output = analyze_pipeline_data(
                table_data=table_data,
                logger=self.logger,
                show_details=True
            )
            
            self.logger.info("[ANALYSIS] Data analysis completed successfully")
            return analysis_output
            
        except Exception as e:
            error_msg = f"Data analysis failed: {str(e)}"
            self.logger.error(f"[ANALYSIS] {error_msg}")
            return f"❌ {error_msg}"
    
    async def send_notifications(self, endpoint: str, results: ExecutionResults):
        """Send notifications about pipeline execution."""
        self.logger.info(f"[NOTIFY] Sending notifications to: {endpoint}")
        
        if endpoint == "console":
            # Console notification is just the report
            self.generate_execution_report(results)
        else:
            # For other endpoints, we'd implement email, Slack, etc.
            self.logger.info(f"Notification endpoint '{endpoint}' not implemented yet") 