"""
Tests for pipeline manager functionality.
"""

import pytest
import asyncio
from unittest.mock import Mock, patch, AsyncMock
from datetime import timedelta
import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from src.orchestrator.pipeline_manager import (
    PipelineManager, PipelineStage, PipelineResult, 
    ExecutionPlan, ExecutionResults
)
from src.orchestrator.pipeline_config import PipelineConfig, OrchestrationConfig
from src.utils.logging import LogManager


class TestPipelineStage:
    """Test PipelineStage enum."""
    
    def test_stage_values(self):
        """Test that all stages have correct values."""
        assert PipelineStage.UNIVERSE.value == "universe"
        assert PipelineStage.PORTFOLIO.value == "portfolio"
        assert PipelineStage.HISTORICAL_GSPREAD.value == "historical-gspread"
        assert PipelineStage.GSPREAD_ANALYTICS.value == "gspread-analytics"
        assert PipelineStage.RUNS_EXCEL.value == "runs-excel"
        assert PipelineStage.RUNS_MONITOR.value == "runs-monitor"


class TestPipelineManager:
    """Test PipelineManager class."""
    
    @pytest.fixture
    def mock_config(self):
        """Create mock configuration."""
        return PipelineConfig(
            orchestration=OrchestrationConfig(),
            universe={},
            portfolio={},
            g_spread={},
            runs={},
            logging={}
        )
    
    @pytest.fixture
    def mock_logger(self):
        """Create mock logger."""
        return Mock(spec=LogManager)
    
    @pytest.fixture
    def pipeline_manager(self, mock_config, mock_logger):
        """Create PipelineManager instance."""
        return PipelineManager(mock_config, mock_logger)
    
    def test_dependencies(self, pipeline_manager):
        """Test pipeline dependencies are correctly defined."""
        deps = pipeline_manager.DEPENDENCIES
        
        # Independent stages
        assert deps[PipelineStage.UNIVERSE] == []
        assert deps[PipelineStage.PORTFOLIO] == []
        assert deps[PipelineStage.HISTORICAL_GSPREAD] == []
        assert deps[PipelineStage.RUNS_EXCEL] == []
        
        # Dependent stages
        assert PipelineStage.UNIVERSE in deps[PipelineStage.GSPREAD_ANALYTICS]
        assert PipelineStage.PORTFOLIO in deps[PipelineStage.GSPREAD_ANALYTICS]
        assert PipelineStage.HISTORICAL_GSPREAD in deps[PipelineStage.GSPREAD_ANALYTICS]
        assert deps[PipelineStage.RUNS_MONITOR] == [PipelineStage.RUNS_EXCEL]
    
    def test_script_mappings(self, pipeline_manager):
        """Test that all stages have script mappings."""
        mappings = pipeline_manager.SCRIPT_MAPPINGS
        
        for stage in PipelineStage:
            assert stage in mappings
            assert isinstance(mappings[stage], str)
            assert mappings[stage].endswith('.py')
    
    def test_determine_stages_full(self, pipeline_manager):
        """Test determining stages for full pipeline."""
        args = Mock(full=True, universe=False, portfolio=False, runs=False)
        args.historical_gspread = False
        args.gspread_analytics = False
        args.resume_from = None
        
        stages = pipeline_manager._determine_stages(args)
        
        assert len(stages) == len(PipelineStage)
        assert all(stage in stages for stage in PipelineStage)
    
    def test_determine_stages_specific(self, pipeline_manager):
        """Test determining stages for specific pipelines."""
        args = Mock(full=False, universe=True, portfolio=True, runs=False)
        args.historical_gspread = False
        args.gspread_analytics = False
        args.resume_from = None
        
        stages = pipeline_manager._determine_stages(args)
        
        assert PipelineStage.UNIVERSE in stages
        assert PipelineStage.PORTFOLIO in stages
        assert PipelineStage.HISTORICAL_GSPREAD not in stages
        assert PipelineStage.GSPREAD_ANALYTICS not in stages
        assert PipelineStage.RUNS_EXCEL not in stages
        assert PipelineStage.RUNS_MONITOR not in stages
    
    def test_determine_stages_runs(self, pipeline_manager):
        """Test determining stages for runs pipeline."""
        args = Mock(full=False, universe=False, portfolio=False, runs=True)
        args.historical_gspread = False
        args.gspread_analytics = False
        args.resume_from = None
        
        stages = pipeline_manager._determine_stages(args)
        
        assert PipelineStage.RUNS_EXCEL in stages
        assert PipelineStage.RUNS_MONITOR in stages
    
    def test_determine_stages_resume_from(self, pipeline_manager):
        """Test determining stages with resume_from."""
        args = Mock(full=False, universe=False, portfolio=False, runs=False)
        args.historical_gspread = False
        args.gspread_analytics = False
        args.resume_from = "gspread-analytics"
        
        stages = pipeline_manager._determine_stages(args)
        
        # Should include gspread-analytics and all stages after it
        expected_stages = [
            PipelineStage.GSPREAD_ANALYTICS,
            PipelineStage.RUNS_EXCEL,
            PipelineStage.RUNS_MONITOR
        ]
        
        for stage in expected_stages:
            assert stage in stages
    
    def test_create_parallel_groups_independent(self, pipeline_manager):
        """Test creating parallel groups for independent stages."""
        stages = [PipelineStage.UNIVERSE, PipelineStage.PORTFOLIO, PipelineStage.HISTORICAL_GSPREAD]
        dependencies = {stage: [] for stage in stages}
        
        groups = pipeline_manager._create_parallel_groups(stages, dependencies)
        
        # All independent stages should be in one group
        assert len(groups) == 1
        assert len(groups[0]) == 3
        assert all(stage in groups[0] for stage in stages)
    
    def test_create_parallel_groups_with_dependencies(self, pipeline_manager):
        """Test creating parallel groups with dependencies."""
        stages = [
            PipelineStage.UNIVERSE, 
            PipelineStage.PORTFOLIO,
            PipelineStage.GSPREAD_ANALYTICS
        ]
        dependencies = {
            PipelineStage.UNIVERSE: [],
            PipelineStage.PORTFOLIO: [],
            PipelineStage.GSPREAD_ANALYTICS: [PipelineStage.UNIVERSE, PipelineStage.PORTFOLIO]
        }
        
        groups = pipeline_manager._create_parallel_groups(stages, dependencies)
        
        # Should have two groups: independent stages first, then dependent stage
        assert len(groups) == 2
        assert PipelineStage.UNIVERSE in groups[0]
        assert PipelineStage.PORTFOLIO in groups[0]
        assert PipelineStage.GSPREAD_ANALYTICS in groups[1]
    
    def test_estimate_duration(self, pipeline_manager):
        """Test duration estimation."""
        stages = [PipelineStage.UNIVERSE, PipelineStage.PORTFOLIO]
        parallel_groups = [[PipelineStage.UNIVERSE, PipelineStage.PORTFOLIO]]
        
        duration = pipeline_manager._estimate_duration(stages, parallel_groups)
        
        # Should be the maximum of the two stages (3 minutes for portfolio)
        expected_minutes = max(
            pipeline_manager.ESTIMATED_TIMES[PipelineStage.UNIVERSE],
            pipeline_manager.ESTIMATED_TIMES[PipelineStage.PORTFOLIO]
        )
        assert duration == timedelta(minutes=expected_minutes)
    
    def test_extract_record_count(self, pipeline_manager):
        """Test extracting record count from output."""
        output = """
        Processing data...
        Loaded 1,234 records from file
        Processing complete
        """
        
        count = pipeline_manager._extract_record_count(output, PipelineStage.UNIVERSE)
        assert count == 1234
    
    def test_extract_record_count_no_match(self, pipeline_manager):
        """Test extracting record count when no match found."""
        output = "No numerical data here"
        
        count = pipeline_manager._extract_record_count(output, PipelineStage.UNIVERSE)
        assert count == 0
    
    @pytest.mark.asyncio
    async def test_execute_stage_success(self, pipeline_manager):
        """Test successful stage execution."""
        with patch('asyncio.create_subprocess_exec') as mock_subprocess:
            # Mock successful process
            mock_process = AsyncMock()
            mock_process.communicate.return_value = (
                b"Processing complete. Loaded 1000 records.",
                b""
            )
            mock_process.returncode = 0
            mock_subprocess.return_value = mock_process
            
            # Mock file existence
            with patch('pathlib.Path.exists', return_value=True):
                result = await pipeline_manager._execute_stage(PipelineStage.UNIVERSE)
            
            assert result.success is True
            assert result.stage == PipelineStage.UNIVERSE
            assert result.records_processed == 1000
            assert result.error_message is None
    
    @pytest.mark.asyncio
    async def test_execute_stage_failure(self, pipeline_manager):
        """Test failed stage execution."""
        with patch('asyncio.create_subprocess_exec') as mock_subprocess:
            # Mock failed process
            mock_process = AsyncMock()
            mock_process.communicate.return_value = (
                b"",
                b"Error: File not found"
            )
            mock_process.returncode = 1
            mock_subprocess.return_value = mock_process
            
            # Mock file existence
            with patch('pathlib.Path.exists', return_value=True):
                result = await pipeline_manager._execute_stage(PipelineStage.UNIVERSE)
            
            assert result.success is False
            assert result.stage == PipelineStage.UNIVERSE
            assert "Error: File not found" in result.error_message
    
    @pytest.mark.asyncio
    async def test_execute_stage_script_not_found(self, pipeline_manager):
        """Test stage execution when script file doesn't exist."""
        with patch('pathlib.Path.exists', return_value=False):
            result = await pipeline_manager._execute_stage(PipelineStage.UNIVERSE)
        
        assert result.success is False
        assert "Script not found" in result.error_message
    
    def test_create_execution_plan(self, pipeline_manager):
        """Test creating execution plan."""
        args = Mock(full=True, universe=False, portfolio=False, runs=False)
        args.historical_gspread = False
        args.gspread_analytics = False
        args.resume_from = None
        
        plan = pipeline_manager.create_execution_plan(args)
        
        assert isinstance(plan, ExecutionPlan)
        assert len(plan.stages) == len(PipelineStage)
        assert isinstance(plan.dependencies, dict)
        assert isinstance(plan.parallel_groups, list)
        assert isinstance(plan.estimated_duration, timedelta) 