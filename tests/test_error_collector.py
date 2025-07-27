"""
Test Error Collector

Unit tests for the error collection and management module.
"""

import pytest
from datetime import datetime

# Add src to path for testing
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.utils.error_collector import ErrorCollector, ErrorCategory, MigrationError


class TestErrorCollector:
    """Test cases for ErrorCollector class."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.error_collector = ErrorCollector()
    
    def test_add_error_critical(self):
        """Test adding a critical error."""
        self.error_collector.add_error(
            ErrorCategory.MIGRATION_ERROR.value,
            "Test critical error",
            table_name="test_table",
            critical=True
        )
        
        assert self.error_collector.has_errors()
        assert self.error_collector.has_critical_errors()
        assert self.error_collector.get_error_count() == 1
        assert self.error_collector.get_critical_error_count() == 1
    
    def test_add_error_warning(self):
        """Test adding a warning (non-critical error)."""
        self.error_collector.add_error(
            ErrorCategory.SCHEMA_ANALYSIS.value,
            "Test warning",
            table_name="test_table",
            critical=False
        )
        
        assert self.error_collector.has_warnings()
        assert not self.error_collector.has_critical_errors()
        assert self.error_collector.get_warning_count() == 1
        assert self.error_collector.get_critical_error_count() == 0
    
    def test_add_table_error(self):
        """Test adding a table-specific error."""
        self.error_collector.add_table_error(
            "users_table",
            ErrorCategory.DATA_TYPE_MAPPING.value,
            "Column type mismatch",
            critical=True
        )
        
        table_errors = self.error_collector.get_errors_by_table("users_table")
        assert len(table_errors) == 1
        assert table_errors[0].table_name == "users_table"
        assert table_errors[0].critical is True
    
    def test_add_connection_error(self):
        """Test adding a connection error."""
        self.error_collector.add_connection_error(
            "MySQL",
            "Connection timeout",
            critical=True
        )
        
        connection_errors = self.error_collector.get_errors_by_category(ErrorCategory.CONNECTION)
        assert len(connection_errors) == 1
        assert "MySQL connection error" in connection_errors[0].message
    
    def test_add_schema_error(self):
        """Test adding a schema analysis error."""
        self.error_collector.add_schema_error(
            "products_table",
            "Missing column in target schema"
        )
        
        schema_errors = self.error_collector.get_errors_by_category(ErrorCategory.SCHEMA_ANALYSIS)
        assert len(schema_errors) == 1
        assert schema_errors[0].table_name == "products_table"
    
    def test_add_missing_table_error(self):
        """Test adding a missing table error."""
        self.error_collector.add_missing_table_error(
            "orders_table",
            "MySQL",
            "PostgreSQL"
        )
        
        missing_table_errors = self.error_collector.get_errors_by_category(ErrorCategory.MISSING_TABLES)
        assert len(missing_table_errors) == 1
        assert "exists in MySQL but not in PostgreSQL" in missing_table_errors[0].message
    
    def test_get_errors_by_category(self):
        """Test retrieving errors by category."""
        # Add different types of errors
        self.error_collector.add_error(ErrorCategory.MIGRATION_ERROR.value, "Migration error 1")
        self.error_collector.add_error(ErrorCategory.MIGRATION_ERROR.value, "Migration error 2")
        self.error_collector.add_error(ErrorCategory.VALIDATION_ERROR.value, "Validation error")
        
        migration_errors = self.error_collector.get_errors_by_category(ErrorCategory.MIGRATION_ERROR)
        validation_errors = self.error_collector.get_errors_by_category(ErrorCategory.VALIDATION_ERROR)
        
        assert len(migration_errors) == 2
        assert len(validation_errors) == 1
    
    def test_get_errors_by_table(self):
        """Test retrieving errors by table name."""
        # Add errors for different tables
        self.error_collector.add_table_error("table1", ErrorCategory.MIGRATION_ERROR.value, "Error 1")
        self.error_collector.add_table_error("table1", ErrorCategory.VALIDATION_ERROR.value, "Error 2")
        self.error_collector.add_table_error("table2", ErrorCategory.MIGRATION_ERROR.value, "Error 3")
        
        table1_errors = self.error_collector.get_errors_by_table("table1")
        table2_errors = self.error_collector.get_errors_by_table("table2")
        
        assert len(table1_errors) == 2
        assert len(table2_errors) == 1
    
    def test_clear_errors(self):
        """Test clearing all errors."""
        # Add some errors
        self.error_collector.add_error(ErrorCategory.MIGRATION_ERROR.value, "Test error", critical=True)
        self.error_collector.add_error(ErrorCategory.VALIDATION_ERROR.value, "Test warning", critical=False)
        
        assert self.error_collector.has_errors()
        assert self.error_collector.has_warnings()
        
        # Clear errors
        self.error_collector.clear()
        
        assert not self.error_collector.has_errors()
        assert not self.error_collector.has_warnings()
        assert self.error_collector.get_error_count() == 0
        assert self.error_collector.get_warning_count() == 0
    
    def test_should_stop_migration_fail_fast(self):
        """Test migration stopping logic with fail_fast enabled."""
        # Add a critical error
        self.error_collector.add_error(
            ErrorCategory.MIGRATION_ERROR.value,
            "Critical migration error",
            critical=True
        )
        
        assert self.error_collector.should_stop_migration(fail_fast=True)
        assert not self.error_collector.should_stop_migration(fail_fast=False)
    
    def test_should_stop_migration_too_many_errors(self):
        """Test migration stopping logic with too many critical errors."""
        # Add multiple critical errors
        for i in range(6):  # More than threshold of 5
            self.error_collector.add_error(
                ErrorCategory.MIGRATION_ERROR.value,
                f"Critical error {i}",
                critical=True
            )
        
        assert self.error_collector.should_stop_migration(fail_fast=False)
    
    def test_should_stop_migration_critical_connection_error(self):
        """Test migration stopping logic with critical connection errors."""
        self.error_collector.add_connection_error(
            "MySQL",
            "Database server unreachable",
            critical=True
        )
        
        assert self.error_collector.should_stop_migration(fail_fast=False)
    
    def test_get_tables_with_errors(self):
        """Test getting list of tables with errors."""
        self.error_collector.add_table_error("table1", ErrorCategory.MIGRATION_ERROR.value, "Error 1")
        self.error_collector.add_table_error("table2", ErrorCategory.VALIDATION_ERROR.value, "Error 2")
        self.error_collector.add_table_error("table1", ErrorCategory.SCHEMA_ANALYSIS.value, "Error 3")
        
        tables_with_errors = self.error_collector.get_tables_with_errors()
        
        assert "table1" in tables_with_errors
        assert "table2" in tables_with_errors
        assert len(tables_with_errors) == 2
    
    def test_export_to_dict(self):
        """Test exporting errors to dictionary format."""
        # Add various types of errors
        self.error_collector.add_error(
            ErrorCategory.MIGRATION_ERROR.value,
            "Test error",
            table_name="test_table",
            critical=True
        )
        self.error_collector.add_error(
            ErrorCategory.VALIDATION_ERROR.value,
            "Test warning",
            critical=False
        )
        
        export_dict = self.error_collector.export_to_dict()
        
        assert 'summary' in export_dict
        assert 'errors' in export_dict
        assert 'warnings' in export_dict
        
        assert export_dict['summary']['total_issues'] == 2
        assert export_dict['summary']['critical_errors'] == 1
        assert export_dict['summary']['warnings'] == 1
        
        assert len(export_dict['errors']) == 1
        assert len(export_dict['warnings']) == 1
    
    def test_unknown_error_category(self):
        """Test handling of unknown error categories."""
        self.error_collector.add_error(
            "UNKNOWN_CATEGORY",  # Invalid category
            "Test error with unknown category"
        )
        
        # Should still add the error but categorize as UNKNOWN_ERROR
        assert self.error_collector.has_warnings()
        unknown_errors = self.error_collector.get_errors_by_category(ErrorCategory.UNKNOWN_ERROR)
        assert len(unknown_errors) == 1


class TestMigrationError:
    """Test cases for MigrationError dataclass."""
    
    def test_migration_error_creation(self):
        """Test MigrationError creation with all fields."""
        error = MigrationError(
            category=ErrorCategory.MIGRATION_ERROR,
            message="Test error message",
            table_name="test_table",
            critical=True,
            context={'batch_number': 5}
        )
        
        assert error.category == ErrorCategory.MIGRATION_ERROR
        assert error.message == "Test error message"
        assert error.table_name == "test_table"
        assert error.critical is True
        assert error.context['batch_number'] == 5
        assert isinstance(error.timestamp, datetime)
    
    def test_migration_error_auto_timestamp(self):
        """Test automatic timestamp generation."""
        error = MigrationError(
            category=ErrorCategory.VALIDATION_ERROR,
            message="Test error"
        )
        
        assert error.timestamp is not None
        assert isinstance(error.timestamp, datetime)


if __name__ == '__main__':
    pytest.main([__file__])
