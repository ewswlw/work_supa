"""
Data models and schemas for the pipeline.
"""
from dataclasses import dataclass
from datetime import datetime, time
from typing import Optional, Any
from enum import Enum


class ProcessingStatus(Enum):
    """Enumeration for processing status"""
    SUCCESS = "success"
    FAILED = "failed"
    WARNING = "warning"


@dataclass
class ProcessingResult:
    """Represents the result of a processing operation"""
    success: bool
    message: str
    status: ProcessingStatus
    data: Optional[Any] = None
    error: Optional[Exception] = None
    metadata: Optional[dict] = None
    
    @classmethod
    def success_result(cls, message: str, data: Any = None, metadata: dict = None):
        """Create a successful processing result"""
        return cls(
            success=True,
            message=message,
            status=ProcessingStatus.SUCCESS,
            data=data,
            metadata=metadata
        )
    
    @classmethod
    def failure_result(cls, message: str, error: Exception = None, metadata: dict = None):
        """Create a failed processing result"""
        return cls(
            success=False,
            message=message,
            status=ProcessingStatus.FAILED,
            error=error,
            metadata=metadata
        )
    
    @classmethod
    def warning_result(cls, message: str, data: Any = None, metadata: dict = None):
        """Create a warning processing result"""
        return cls(
            success=True,
            message=message,
            status=ProcessingStatus.WARNING,
            data=data,
            metadata=metadata
        )


@dataclass
class ExcelFileInfo:
    """Information about an Excel file"""
    file_path: str
    file_name: str
    modification_date: datetime
    file_size: int
    sheet_count: int = 1


@dataclass
class ProcessingStats:
    """Statistics about the processing operation"""
    files_processed: int
    rows_processed: int
    rows_loaded: int
    rows_duplicated: int
    rows_invalid: int
    processing_time: float
    start_time: datetime
    end_time: datetime


@dataclass
class DataQualityReport:
    """Data quality assessment report"""
    total_rows: int
    total_columns: int
    null_counts: dict
    duplicate_rows: int
    data_types: dict
    numeric_stats: dict
    validation_passed: bool
    warnings: list
    errors: list 