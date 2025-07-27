"""
Error Collection and Management Module

Handles categorization, collection, and reporting of errors during migration.
"""

import logging
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass


class ErrorCategory(Enum):
    """Categories of errors that can occur during migration."""
    CONFIGURATION = "CONFIGURATION"
    CONNECTION = "CONNECTION"
    SCHEMA_ANALYSIS = "SCHEMA_ANALYSIS"
    MISSING_TABLES = "MISSING_TABLES"
    DATA_TYPE_MAPPING = "DATA_TYPE_MAPPING"
    MIGRATION_ERROR = "MIGRATION_ERROR"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    PERMISSION_ERROR = "PERMISSION_ERROR"
    TIMEOUT_ERROR = "TIMEOUT_ERROR"
    RESOURCE_ERROR = "RESOURCE_ERROR"
    UNKNOWN_ERROR = "UNKNOWN_ERROR"


@dataclass
class MigrationError:
    """Represents a single migration error with context."""
    category: ErrorCategory
    message: str
    table_name: Optional[str] = None
    critical: bool = False
    timestamp: datetime = None
    stack_trace: Optional[str] = None
    context: Optional[Dict] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class ErrorCollector:
    """Collects and manages errors during migration operations."""
    
    def __init__(self):
        self.errors: List[MigrationError] = []
        self.warnings: List[MigrationError] = []
        self.logger = logging.getLogger(__name__)
        self._error_counts: Dict[ErrorCategory, int] = {}
        self._critical_error_count = 0
    
    def add_error(self, category: str, message: str, table_name: Optional[str] = None,
                  critical: bool = False, stack_trace: Optional[str] = None,
                  context: Optional[Dict] = None) -> None:
        """
        Add an error to the collection.
        
        Args:
            category: Error category string
            message: Error message
            table_name: Name of affected table (if applicable)
            critical: Whether this is a critical error
            stack_trace: Stack trace string (if available)
            context: Additional context dictionary
        """
        try:
            error_category = ErrorCategory(category)
        except ValueError:
            error_category = ErrorCategory.UNKNOWN_ERROR
            self.logger.warning(f"Unknown error category: {category}")
        
        error = MigrationError(
            category=error_category,
            message=message,
            table_name=table_name,
            critical=critical,
            stack_trace=stack_trace,
            context=context
        )
        
        if critical:
            self.errors.append(error)
            self._critical_error_count += 1
            self.logger.critical(f"Critical error in {table_name or 'unknown'}: {message}")
        else:
            self.warnings.append(error)
            self.logger.warning(f"Warning in {table_name or 'unknown'}: {message}")
        
        # Update counts
        self._error_counts[error_category] = self._error_counts.get(error_category, 0) + 1
    
    def add_table_error(self, table_name: str, category: str, message: str,
                       critical: bool = False, stack_trace: Optional[str] = None) -> None:
        """
        Add a table-specific error.
        
        Args:
            table_name: Name of the affected table
            category: Error category
            message: Error message
            critical: Whether this is critical
            stack_trace: Stack trace if available
        """
        self.add_error(
            category=category,
            message=message,
            table_name=table_name,
            critical=critical,
            stack_trace=stack_trace,
            context={'table': table_name}
        )
    
    def add_connection_error(self, database: str, message: str, critical: bool = True) -> None:
        """
        Add a database connection error.
        
        Args:
            database: Database name (MySQL/PostgreSQL)
            message: Error message
            critical: Whether this is critical (default True)
        """
        self.add_error(
            category=ErrorCategory.CONNECTION.value,
            message=f"{database} connection error: {message}",
            critical=critical,
            context={'database': database}
        )
    
    def add_schema_error(self, table_name: str, message: str, critical: bool = False) -> None:
        """
        Add a schema analysis error.
        
        Args:
            table_name: Name of the affected table
            message: Error message
            critical: Whether this is critical
        """
        self.add_table_error(
            table_name=table_name,
            category=ErrorCategory.SCHEMA_ANALYSIS.value,
            message=message,
            critical=critical
        )
    
    def add_missing_table_error(self, table_name: str, source_db: str, target_db: str) -> None:
        """
        Add a missing table error.
        
        Args:
            table_name: Name of the missing table
            source_db: Source database name
            target_db: Target database name
        """
        self.add_table_error(
            table_name=table_name,
            category=ErrorCategory.MISSING_TABLES.value,
            message=f"Table '{table_name}' exists in {source_db} but not in {target_db}",
            critical=False
        )
    
    def has_errors(self) -> bool:
        """Check if any errors have been collected."""
        return len(self.errors) > 0
    
    def has_critical_errors(self) -> bool:
        """Check if any critical errors have been collected."""
        return self._critical_error_count > 0
    
    def has_warnings(self) -> bool:
        """Check if any warnings have been collected."""
        return len(self.warnings) > 0
    
    def get_error_count(self) -> int:
        """Get total error count."""
        return len(self.errors)
    
    def get_warning_count(self) -> int:
        """Get total warning count."""
        return len(self.warnings)
    
    def get_critical_error_count(self) -> int:
        """Get critical error count."""
        return self._critical_error_count
    
    def get_errors_by_category(self, category: ErrorCategory) -> List[MigrationError]:
        """
        Get all errors of a specific category.
        
        Args:
            category: Error category to filter by
            
        Returns:
            List of errors in the specified category
        """
        all_issues = self.errors + self.warnings
        return [error for error in all_issues if error.category == category]
    
    def get_errors_by_table(self, table_name: str) -> List[MigrationError]:
        """
        Get all errors for a specific table.
        
        Args:
            table_name: Table name to filter by
            
        Returns:
            List of errors for the specified table
        """
        all_issues = self.errors + self.warnings
        return [error for error in all_issues if error.table_name == table_name]
    
    def clear(self) -> None:
        """Clear all collected errors and warnings."""
        self.errors.clear()
        self.warnings.clear()
        self._error_counts.clear()
        self._critical_error_count = 0
        self.logger.info("Error collector cleared")
    
    def print_summary(self) -> None:
        """Print a summary of collected errors and warnings."""
        print("\n" + "=" * 60)
        print("📊 ERROR SUMMARY")
        print("=" * 60)
        
        if not self.has_errors() and not self.has_warnings():
            print("✅ No errors or warnings to report!")
            return
        
        # Summary statistics
        print(f"Total Issues: {len(self.errors) + len(self.warnings)}")
        print(f"Critical Errors: {self._critical_error_count}")
        print(f"Warnings: {len(self.warnings)}")
        print()
        
        # Errors by category
        if self._error_counts:
            print("📋 Errors by Category:")
            for category, count in sorted(self._error_counts.items(), key=lambda x: x[0].value):
                print(f"  {category.value}: {count}")
            print()
        
        # Critical errors
        if self.has_critical_errors():
            print("🚨 CRITICAL ERRORS:")
            critical_errors = [e for e in self.errors if e.critical]
            for i, error in enumerate(critical_errors, 1):
                print(f"  {i}. [{error.category.value}] {error.message}")
                if error.table_name:
                    print(f"     Table: {error.table_name}")
                if error.stack_trace:
                    print(f"     Stack trace available in logs")
                print()
        
        # Non-critical errors
        non_critical_errors = [e for e in self.errors if not e.critical]
        if non_critical_errors:
            print("⚠️  ERRORS:")
            for i, error in enumerate(non_critical_errors, 1):
                print(f"  {i}. [{error.category.value}] {error.message}")
                if error.table_name:
                    print(f"     Table: {error.table_name}")
                print()
        
        # Warnings
        if self.has_warnings():
            print("ℹ️  WARNINGS:")
            for i, warning in enumerate(self.warnings, 1):
                print(f"  {i}. [{warning.category.value}] {warning.message}")
                if warning.table_name:
                    print(f"     Table: {warning.table_name}")
                print()
        
        print("=" * 60)
    
    def export_to_dict(self) -> Dict:
        """
        Export errors to dictionary format for serialization.
        
        Returns:
            Dictionary containing all error information
        """
        return {
            'summary': {
                'total_issues': len(self.errors) + len(self.warnings),
                'critical_errors': self._critical_error_count,
                'warnings': len(self.warnings),
                'error_counts': {cat.value: count for cat, count in self._error_counts.items()}
            },
            'errors': [
                {
                    'category': error.category.value,
                    'message': error.message,
                    'table_name': error.table_name,
                    'critical': error.critical,
                    'timestamp': error.timestamp.isoformat(),
                    'context': error.context
                }
                for error in self.errors
            ],
            'warnings': [
                {
                    'category': warning.category.value,
                    'message': warning.message,
                    'table_name': warning.table_name,
                    'timestamp': warning.timestamp.isoformat(),
                    'context': warning.context
                }
                for warning in self.warnings
            ]
        }
    
    def get_tables_with_errors(self) -> List[str]:
        """
        Get list of table names that have errors.
        
        Returns:
            List of table names with errors
        """
        tables = set()
        for error in self.errors + self.warnings:
            if error.table_name:
                tables.add(error.table_name)
        return sorted(list(tables))
    
    def should_stop_migration(self, fail_fast: bool = False) -> bool:
        """
        Determine if migration should be stopped based on errors.
        
        Args:
            fail_fast: Whether to stop on any critical error
            
        Returns:
            True if migration should be stopped
        """
        if fail_fast and self.has_critical_errors():
            return True
        
        # Stop if too many critical errors (threshold)
        if self._critical_error_count > 5:
            return True
        
        # Stop if critical connection errors
        connection_errors = self.get_errors_by_category(ErrorCategory.CONNECTION)
        critical_connection_errors = [e for e in connection_errors if e.critical]
        if critical_connection_errors:
            return True
        
        return False
