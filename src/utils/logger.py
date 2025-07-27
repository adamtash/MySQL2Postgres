"""
Logging Utility Module

Configures structured logging for the migration tool with file and console output.
"""

import logging
import logging.handlers
import os
from datetime import datetime
from pathlib import Path
from typing import Optional


def setup_logging(log_level: str = 'INFO', log_file: Optional[str] = None) -> logging.Logger:
    """
    Setup logging configuration for the migration tool.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file path. If None, uses timestamp-based filename
        
    Returns:
        Configured logger instance
    """
    # Create logs directory if it doesn't exist
    logs_dir = Path('logs')
    logs_dir.mkdir(exist_ok=True)
    
    # Generate log file name if not provided
    if log_file is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = logs_dir / f'migration_{timestamp}.log'
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # File handler with rotation
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, log_level.upper()))
    
    # Use simpler format for console
    console_formatter = logging.Formatter(
        '%(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)
    
    # Log initial setup
    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized - Level: {log_level}, File: {log_file}")
    
    return root_logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger instance for a specific module.
    
    Args:
        name: Logger name (usually __name__)
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)


def log_exception(logger: logging.Logger, message: str, exc: Exception) -> None:
    """
    Log an exception with full traceback.
    
    Args:
        logger: Logger instance
        message: Custom message to include
        exc: Exception to log
    """
    logger.error(f"{message}: {str(exc)}", exc_info=True)


def log_performance(logger: logging.Logger, operation: str, duration: float, 
                   records: Optional[int] = None) -> None:
    """
    Log performance metrics for operations.
    
    Args:
        logger: Logger instance
        operation: Description of the operation
        duration: Duration in seconds
        records: Number of records processed (optional)
    """
    if records:
        rate = records / duration if duration > 0 else 0
        logger.info(
            f"Performance - {operation}: {duration:.2f}s, "
            f"{records} records, {rate:.2f} records/sec"
        )
    else:
        logger.info(f"Performance - {operation}: {duration:.2f}s")


class MigrationLogger:
    """Specialized logger for migration operations."""
    
    def __init__(self, name: str):
        self.logger = get_logger(name)
        self.start_time = None
        self.operation_count = 0
    
    def start_operation(self, operation: str):
        """Start timing an operation."""
        self.start_time = datetime.now()
        self.logger.info(f"Starting {operation}")
    
    def end_operation(self, operation: str, success: bool = True, 
                     records: Optional[int] = None):
        """End timing an operation and log results."""
        if self.start_time:
            duration = (datetime.now() - self.start_time).total_seconds()
            status = "completed" if success else "failed"
            
            if records:
                rate = records / duration if duration > 0 else 0
                self.logger.info(
                    f"{operation} {status} in {duration:.2f}s - "
                    f"{records} records processed ({rate:.2f} records/sec)"
                )
            else:
                self.logger.info(f"{operation} {status} in {duration:.2f}s")
            
            self.operation_count += 1
            self.start_time = None
    
    def log_table_start(self, table_name: str, record_count: int):
        """Log the start of table migration."""
        self.logger.info(f"Migrating table '{table_name}' ({record_count} records)")
    
    def log_table_complete(self, table_name: str, migrated_count: int, 
                          duration: float):
        """Log completion of table migration."""
        rate = migrated_count / duration if duration > 0 else 0
        self.logger.info(
            f"Completed table '{table_name}' - {migrated_count} records "
            f"in {duration:.2f}s ({rate:.2f} records/sec)"
        )
    
    def log_batch_progress(self, table_name: str, batch_num: int, 
                          total_batches: int, records_processed: int):
        """Log batch processing progress."""
        progress = (batch_num / total_batches) * 100
        self.logger.debug(
            f"Table '{table_name}' - Batch {batch_num}/{total_batches} "
            f"({progress:.1f}%) - {records_processed} records processed"
        )
    
    def log_error(self, operation: str, error: Exception, critical: bool = False):
        """Log an error with appropriate level."""
        level = logging.CRITICAL if critical else logging.ERROR
        self.logger.log(
            level,
            f"Error in {operation}: {str(error)}",
            exc_info=True
        )
    
    def log_warning(self, message: str):
        """Log a warning message."""
        self.logger.warning(message)
    
    def log_info(self, message: str):
        """Log an info message."""
        self.logger.info(message)
    
    def log_debug(self, message: str):
        """Log a debug message."""
        self.logger.debug(message)
