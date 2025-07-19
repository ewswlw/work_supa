"""
Tests for Database Logging System

This module tests the database logging functionality including
structured logging, operation contexts, and performance monitoring.
"""

import pytest
import tempfile
import os
import json
import time
from pathlib import Path
import sys
from unittest.mock import Mock, patch, MagicMock

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent / "src"))

from db.utils.db_logger import DatabaseLogger


class TestDatabaseLogger:
    """Test DatabaseLogger class functionality."""
    
    @pytest.fixture
    def temp_log_dir(self):
        """Create temporary log directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
    
    @pytest.fixture
    def logger(self, temp_log_dir):
        """Create DatabaseLogger instance."""
        logger = DatabaseLogger(log_dir=temp_log_dir)
        yield logger
        # Clean up logger resources
        logger.cleanup()
    
    def test_initialization(self, temp_log_dir):
        """Test DatabaseLogger initialization."""
        logger = DatabaseLogger(log_dir=temp_log_dir)
        
        assert logger.log_dir == Path(temp_log_dir)
        assert logger.log_dir.exists()
        
        # Check that loggers were created
        assert logger.db_logger is not None
        assert logger.db_sql_logger is not None
        assert logger.db_cusip_logger is not None
        assert logger.db_audit_logger is not None
        assert logger.db_perf_logger is not None
    
    def test_log_file_creation(self, temp_log_dir):
        """Test that log files are created."""
        logger = DatabaseLogger(log_dir=temp_log_dir)
        
        # Check that log files exist
        log_files = ['db.log', 'db_sql.log', 'db_cusip.log', 'db_audit.log', 'db_perf.log']
        for log_file in log_files:
            assert (Path(temp_log_dir) / log_file).exists()
    
    def test_log_event_info(self, logger):
        """Test logging info event."""
        logger.log_event("Test info message", {'test': 'data'}, 'INFO')
        
        # Check log file
        log_file = logger.log_dir / 'db.log'
        with open(log_file, 'r') as f:
            log_entry = f.readline().strip()
        
        # Parse JSON log entry
        log_data = json.loads(log_entry.split('INFO: ')[1])
        
        assert log_data['message'] == 'Test info message'
        assert log_data['details']['test'] == 'data'
        assert log_data['event_type'] == 'database_event'
    
    def test_log_event_debug(self, logger):
        """Test logging debug event."""
        logger.log_event("Test debug message", {'test': 'data'}, 'DEBUG')
        
        # Check log file
        log_file = logger.log_dir / 'db.log'
        with open(log_file, 'r') as f:
            log_entry = f.readline().strip()
        
        # Parse JSON log entry
        log_data = json.loads(log_entry.split('DEBUG: ')[1])
        
        assert log_data['message'] == 'Test debug message'
        assert log_data['details']['test'] == 'data'
    
    def test_log_event_warning(self, logger):
        """Test logging warning event."""
        logger.log_event("Test warning message", {'test': 'data'}, 'WARNING')
        
        # Check log file
        log_file = logger.log_dir / 'db.log'
        with open(log_file, 'r') as f:
            log_entry = f.readline().strip()
        
        # Parse JSON log entry
        log_data = json.loads(log_entry.split('WARNING: ')[1])
        
        assert log_data['message'] == 'Test warning message'
        assert log_data['details']['test'] == 'data'
    
    def test_log_event_error(self, logger):
        """Test logging error event."""
        logger.log_event("Test error message", {'test': 'data'}, 'ERROR')
        
        # Check log file
        log_file = logger.log_dir / 'db.log'
        with open(log_file, 'r') as f:
            log_entry = f.readline().strip()
        
        # Parse JSON log entry
        log_data = json.loads(log_entry.split('ERROR: ')[1])
        
        assert log_data['message'] == 'Test error message'
        assert log_data['details']['test'] == 'data'
    
    def test_log_sql_success(self, logger):
        """Test logging successful SQL operation."""
        logger.log_sql("SELECT * FROM test", (1,), 15.5, 10, True)
        
        # Check log file
        log_file = logger.log_dir / 'db_sql.log'
        with open(log_file, 'r') as f:
            log_entry = f.readline().strip()
        
        # Parse JSON log entry
        log_data = json.loads(log_entry.split('INFO: ')[1])
        
        assert log_data['details']['sql'] == "SELECT * FROM test"
        assert log_data['details']['params'] == [1]  # JSON serializes tuples as lists
        assert log_data['details']['duration_ms'] == 15.5
        assert log_data['details']['rows_affected'] == 10
        assert log_data['details']['success'] is True
    
    def test_log_sql_failure(self, logger):
        """Test logging failed SQL operation."""
        logger.log_sql("SELECT * FROM nonexistent", None, None, None, False)
        
        # Check log file
        log_file = logger.log_dir / 'db_sql.log'
        with open(log_file, 'r') as f:
            log_entry = f.readline().strip()
        
        # Parse JSON log entry
        log_data = json.loads(log_entry.split('ERROR: ')[1])
        
        assert log_data['details']['sql'] == "SELECT * FROM nonexistent"
        assert log_data['details']['success'] is False
    
    def test_log_cusip_operation_success(self, logger):
        """Test logging successful CUSIP operation."""
        logger.log_cusip_operation('standardize', '912810TM0', '912810TM0', True, {'source': 'test'})
        
        # Check log file
        log_file = logger.log_dir / 'db_cusip.log'
        with open(log_file, 'r') as f:
            log_entry = f.readline().strip()
        
        # Parse JSON log entry
        log_data = json.loads(log_entry.split('INFO: ')[1])
        
        assert log_data['details']['operation'] == 'standardize'
        assert log_data['details']['cusip_original'] == '912810TM0'
        assert log_data['details']['cusip_standardized'] == '912810TM0'
        assert log_data['details']['success'] is True
        assert log_data['details']['source'] == 'test'
    
    def test_log_cusip_operation_failure(self, logger):
        """Test logging failed CUSIP operation."""
        logger.log_cusip_operation('standardize', 'INVALID', 'INVALID', False, {'source': 'test'})
        
        # Check log file
        log_file = logger.log_dir / 'db_cusip.log'
        with open(log_file, 'r') as f:
            log_entry = f.readline().strip()
        
        # Parse JSON log entry
        log_data = json.loads(log_entry.split('ERROR: ')[1])
        
        assert log_data['details']['operation'] == 'standardize'
        assert log_data['details']['cusip_original'] == 'INVALID'
        assert log_data['details']['success'] is False
    
    def test_log_audit(self, logger):
        """Test logging audit event."""
        logger.log_audit('INSERT', 'test_table', 100, {'user': 'test_user'})
        
        # Check log file
        log_file = logger.log_dir / 'db_audit.log'
        with open(log_file, 'r') as f:
            log_entry = f.readline().strip()
        
        # Parse JSON log entry
        log_data = json.loads(log_entry.split('INFO: ')[1])
        
        assert log_data['details']['action'] == 'INSERT'
        assert log_data['details']['table_name'] == 'test_table'
        assert log_data['details']['record_count'] == 100
        assert log_data['details']['user'] == 'test_user'
    
    def test_log_performance(self, logger):
        """Test logging performance metrics."""
        logger.log_performance('batch_insert', 1500.5, 1000, {'table': 'test_table'})
        
        # Check log file
        log_file = logger.log_dir / 'db_perf.log'
        with open(log_file, 'r') as f:
            log_entry = f.readline().strip()
        
        # Parse JSON log entry
        log_data = json.loads(log_entry.split('INFO: ')[1])
        
        assert log_data['details']['operation'] == 'batch_insert'
        assert log_data['details']['duration_ms'] == 1500.5
        assert log_data['details']['record_count'] == 1000
        assert log_data['details']['records_per_second'] == pytest.approx(666.67, rel=0.01)
        assert log_data['details']['table'] == 'test_table'
    
    def test_log_error(self, logger):
        """Test logging error with context."""
        error = ValueError("Test error message")
        context = {'operation': 'test_op', 'table': 'test_table'}
        
        logger.log_error(error, context)
        
        # Check log file
        log_file = logger.log_dir / 'db.log'
        with open(log_file, 'r') as f:
            log_entry = f.readline().strip()
        
        # Parse JSON log entry
        log_data = json.loads(log_entry.split('ERROR: ')[1])
        
        assert log_data['details']['error_type'] == 'ValueError'
        assert log_data['details']['error_message'] == 'Test error message'
        assert 'error_traceback' in log_data['details']
        assert log_data['details']['context']['operation'] == 'test_op'
        assert log_data['details']['context']['table'] == 'test_table'
    
    def test_operation_context_success(self, logger):
        """Test operation context with successful execution."""
        with logger.operation_context('test_operation', {'param': 'value'}):
            time.sleep(0.01)  # Small delay to ensure timing
        
        # Check that both start and completion were logged
        log_file = logger.log_dir / 'db.log'
        with open(log_file, 'r') as f:
            log_entries = f.readlines()
        
        assert len(log_entries) >= 2
        
        # Parse start log entry
        start_data = json.loads(log_entries[0].split('INFO: ')[1])
        assert start_data['message'] == 'Starting operation: test_operation'
        assert start_data['details']['param'] == 'value'
        
        # Parse completion log entry
        completion_data = json.loads(log_entries[1].split('INFO: ')[1])
        assert completion_data['message'] == 'Completed operation: test_operation'
        assert completion_data['details']['status'] == 'success'
        assert 'duration_ms' in completion_data['details']
    
    def test_operation_context_failure(self, logger):
        """Test operation context with failed execution."""
        with pytest.raises(ValueError):
            with logger.operation_context('test_operation', {'param': 'value'}):
                raise ValueError("Test error")
        
        # Check that both start and error were logged
        log_file = logger.log_dir / 'db.log'
        with open(log_file, 'r') as f:
            log_entries = f.readlines()
        
        assert len(log_entries) >= 2
        
        # Parse start log entry
        start_data = json.loads(log_entries[0].split('INFO: ')[1])
        assert start_data['message'] == 'Starting operation: test_operation'
        
        # Parse error log entry
        error_data = json.loads(log_entries[1].split('ERROR: ')[1])
        assert error_data['details']['error_type'] == 'ValueError'
        assert error_data['details']['context']['operation_name'] == 'test_operation'
        assert error_data['details']['context']['status'] == 'failed'
    
    def test_log_pipeline_step(self, logger):
        """Test logging pipeline step."""
        step_details = {'step': 'data_loading', 'records': 1000, 'duration': 15.5}
        logger.log_pipeline_step('test_step', step_details)
        
        # Check log file
        log_file = logger.log_dir / 'db.log'
        with open(log_file, 'r') as f:
            log_entry = f.readline().strip()
        
        # Parse JSON log entry
        log_data = json.loads(log_entry.split('INFO: ')[1])
        
        assert log_data['message'] == 'Pipeline step: test_step'
        assert log_data['details']['step'] == 'data_loading'
        assert log_data['details']['records'] == 1000
        assert log_data['details']['duration'] == 15.5
    
    def test_log_schema_operation_success(self, logger):
        """Test logging successful schema operation."""
        logger.log_schema_operation('CREATE', 'test_table', True, {'columns': 5})
        
        # Check log file
        log_file = logger.log_dir / 'db.log'
        with open(log_file, 'r') as f:
            log_entry = f.readline().strip()
        
        # Parse JSON log entry
        log_data = json.loads(log_entry.split('INFO: ')[1])
        
        assert log_data['details']['operation'] == 'CREATE'
        assert log_data['details']['table_name'] == 'test_table'
        assert log_data['details']['success'] is True
        assert log_data['details']['columns'] == 5
    
    def test_log_schema_operation_failure(self, logger):
        """Test logging failed schema operation."""
        logger.log_schema_operation('CREATE', 'test_table', False, {'error': 'table exists'})
        
        # Check log file
        log_file = logger.log_dir / 'db.log'
        with open(log_file, 'r') as f:
            log_entry = f.readline().strip()
        
        # Parse JSON log entry
        log_data = json.loads(log_entry.split('ERROR: ')[1])
        
        assert log_data['details']['operation'] == 'CREATE'
        assert log_data['details']['table_name'] == 'test_table'
        assert log_data['details']['success'] is False
        assert log_data['details']['error'] == 'table exists'
    
    def test_log_data_quality_passed(self, logger):
        """Test logging passed data quality check."""
        logger.log_data_quality('test_table', 'null_check', 'passed', {'null_count': 0})
        
        # Check log file
        log_file = logger.log_dir / 'db_audit.log'
        with open(log_file, 'r') as f:
            log_entry = f.readline().strip()
        
        # Parse JSON log entry
        log_data = json.loads(log_entry.split('INFO: ')[1])
        
        assert log_data['details']['table_name'] == 'test_table'
        assert log_data['details']['check_type'] == 'null_check'
        assert log_data['details']['check_result'] == 'passed'
        assert log_data['details']['null_count'] == 0
    
    def test_log_data_quality_warning(self, logger):
        """Test logging warning data quality check."""
        logger.log_data_quality('test_table', 'range_check', 'warning', {'out_of_range': 5})
        
        # Check log file
        log_file = logger.log_dir / 'db_audit.log'
        with open(log_file, 'r') as f:
            log_entry = f.readline().strip()
        
        # Parse JSON log entry
        log_data = json.loads(log_entry.split('WARNING: ')[1])
        
        assert log_data['details']['check_result'] == 'warning'
        assert log_data['details']['out_of_range'] == 5
    
    def test_log_data_quality_failed(self, logger):
        """Test logging failed data quality check."""
        logger.log_data_quality('test_table', 'integrity_check', 'failed', {'error': 'constraint violation'})
        
        # Check log file
        log_file = logger.log_dir / 'db_audit.log'
        with open(log_file, 'r') as f:
            log_entry = f.readline().strip()
        
        # Parse JSON log entry
        log_data = json.loads(log_entry.split('ERROR: ')[1])
        
        assert log_data['details']['check_result'] == 'failed'
        assert log_data['details']['error'] == 'constraint violation'
    
    def test_get_log_summary(self, logger):
        """Test getting log summary."""
        # Create some log entries
        logger.log_event("Test message 1")
        logger.log_event("Test message 2")
        logger.log_event("Test message 3", level='ERROR')
        
        # Get summary
        summary = logger.get_log_summary()
        
        assert summary['total_entries'] >= 3
        assert summary['error_count'] >= 1
        assert 'db.log' in summary['log_files']
        assert summary['log_files']['db.log']['line_count'] >= 3
    
    def test_get_log_summary_no_files(self, temp_log_dir):
        """Test getting log summary with no log files."""
        logger = DatabaseLogger(log_dir=temp_log_dir)

        summary = logger.get_log_summary()

        assert summary['total_entries'] == 0
        assert summary['error_count'] == 0
        # Log files are created but empty
        assert len(summary['log_files']) == 5  # 5 log files created
        for log_file_info in summary['log_files'].values():
            assert log_file_info['line_count'] == 0  # All files are empty
    
    def test_memory_usage_tracking(self, logger):
        """Test memory usage tracking in log entries."""
        logger.log_event("Test message")
        
        # Check log file
        log_file = logger.log_dir / 'db.log'
        with open(log_file, 'r') as f:
            log_entry = f.readline().strip()
        
        # Parse JSON log entry
        log_data = json.loads(log_entry.split('INFO: ')[1])
        
        assert 'memory_usage_mb' in log_data
        assert isinstance(log_data['memory_usage_mb'], (int, float))
    
    def test_database_size_tracking(self, logger):
        """Test database size tracking in log entries."""
        logger.log_event("Test message")
        
        # Check log file
        log_file = logger.log_dir / 'db.log'
        with open(log_file, 'r') as f:
            log_entry = f.readline().strip()
        
        # Parse JSON log entry
        log_data = json.loads(log_entry.split('INFO: ')[1])
        
        assert 'database_file_size_mb' in log_data
        assert isinstance(log_data['database_file_size_mb'], (int, float))
    
    def test_operation_id_tracking(self, logger):
        """Test operation ID tracking in log entries."""
        with logger.operation_context('test_operation'):
            logger.log_event("Nested message")
        
        # Check log file
        log_file = logger.log_dir / 'db.log'
        with open(log_file, 'r') as f:
            log_entries = f.readlines()
        
        # Parse log entries
        start_data = json.loads(log_entries[0].split('INFO: ')[1])
        nested_data = json.loads(log_entries[1].split('INFO: ')[1])
        completion_data = json.loads(log_entries[2].split('INFO: ')[1])
        
        # All should have the same operation ID
        operation_id = start_data['operation_id']
        assert nested_data['operation_id'] == operation_id
        assert completion_data['operation_id'] == operation_id
    
    def test_nested_operation_context(self, logger):
        """Test nested operation contexts."""
        with logger.operation_context('outer_operation'):
            with logger.operation_context('inner_operation'):
                logger.log_event("Nested message")
        
        # Check log file
        log_file = logger.log_dir / 'db.log'
        with open(log_file, 'r') as f:
            log_entries = f.readlines()
        
        # Should have operation context for inner operation
        nested_data = json.loads(log_entries[2].split('INFO: ')[1])
        assert nested_data['operation_context']['operation_name'] == 'inner_operation'
    
    def test_logger_duplicate_handler_prevention(self, temp_log_dir):
        """Test that duplicate handlers are not created."""
        logger1 = DatabaseLogger(log_dir=temp_log_dir)
        logger2 = DatabaseLogger(log_dir=temp_log_dir)

        # Each logger should have its own unique loggers (not singleton behavior)
        assert logger1.db_logger is not logger2.db_logger
        # But both should have handlers
        assert len(logger1.db_logger.handlers) > 0
        assert len(logger2.db_logger.handlers) > 0
        assert logger1.db_sql_logger is not logger2.db_sql_logger
    
    def test_logger_propagate_setting(self, logger):
        """Test that loggers don't propagate to root logger."""
        assert logger.db_logger.propagate is False
        assert logger.db_sql_logger.propagate is False
        assert logger.db_cusip_logger.propagate is False
        assert logger.db_audit_logger.propagate is False
        assert logger.db_perf_logger.propagate is False 