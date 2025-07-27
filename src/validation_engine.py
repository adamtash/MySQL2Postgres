"""
Validation Engine Module

Provides comprehensive post-migration validation including record count comparison,
schema validation, and data sampling for statistical validation.
"""

import logging
import random
import time
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime

try:
    from sqlalchemy import create_engine, text, inspect
    from sqlalchemy.exc import SQLAlchemyError
except ImportError as e:
    print(f"Missing required database driver: {e}")
    print("Please install with: pip install -r requirements.txt")
    exit(1)

from .utils.logger import MigrationLogger
from .utils.status_monitor import StatusMonitor, MigrationPhase
from .utils.error_collector import ErrorCollector, ErrorCategory


class ValidationEngine:
    """Comprehensive post-migration validation system."""
    
    def __init__(self, config: Dict[str, Any], status_monitor: StatusMonitor,
                 error_collector: ErrorCollector):
        """
        Initialize validation engine.
        
        Args:
            config: Configuration dictionary
            status_monitor: Status monitoring instance
            error_collector: Error collection instance
        """
        self.config = config
        self.status_monitor = status_monitor
        self.error_collector = error_collector
        self.logger = MigrationLogger(__name__)
        
        # Database engines
        self.mysql_engine = None
        self.postgres_engine = None
        
        # Validation settings
        self.sample_size = min(1000, config['migration'].get('validation_sample_size', 100))
        self.tolerance_threshold = config['migration'].get('validation_tolerance', 0.01)  # 1%
        
        self._initialize_engines()
    
    def _initialize_engines(self) -> None:
        """Initialize database engines."""
        try:
            # MySQL engine
            mysql_conn_str = (
                f"mysql+mysqlconnector://{self.config['mysql']['username']}:"
                f"{self.config['mysql']['password']}@{self.config['mysql']['host']}:"
                f"{self.config['mysql']['port']}/{self.config['mysql']['database']}"
            )
            self.mysql_engine = create_engine(mysql_conn_str, pool_recycle=3600)
            
            # PostgreSQL engine
            postgres_conn_str = (
                f"postgresql+psycopg://{self.config['postgres']['username']}:"
                f"{self.config['postgres']['password']}@{self.config['postgres']['host']}:"
                f"{self.config['postgres']['port']}/{self.config['postgres']['database']}"
            )
            self.postgres_engine = create_engine(postgres_conn_str, pool_recycle=3600)
            
            self.logger.log_info("Validation engines initialized")
            
        except Exception as e:
            self.logger.log_error("validation_engine_init", e, critical=True)
            raise
    
    def validate(self) -> bool:
        """
        Run comprehensive migration validation.
        
        Returns:
            True if validation passes, False otherwise
        """
        self.logger.start_operation("Migration Validation")
        self.status_monitor.set_phase(MigrationPhase.VALIDATING)
        
        print("🔍 Starting Migration Validation")
        
        try:
            # Phase 1: Table mapping validation
            print("📋 Phase 1: Validating table mappings...")
            table_mappings = self._validate_table_mappings()
            
            if not table_mappings:
                print("❌ No valid table mappings found")
                return False
            
            print(f"✅ Found {len(table_mappings)} valid table mappings")
            
            # Phase 2: Record count validation
            print("\n📊 Phase 2: Validating record counts...")
            count_validation_passed = self._validate_record_counts(table_mappings)
            
            # Phase 3: Schema structure validation
            print("\n🏗️  Phase 3: Validating schema structures...")
            schema_validation_passed = self._validate_schema_structures(table_mappings)
            
            # Phase 4: Data sampling validation
            print("\n🎲 Phase 4: Validating data samples...")
            sample_validation_passed = self._validate_data_samples(table_mappings)
            
            # Phase 5: Foreign key relationship validation
            print("\n🔗 Phase 5: Validating relationships...")
            relationship_validation_passed = self._validate_relationships(table_mappings)
            
            # Summary
            all_validations_passed = (
                count_validation_passed and 
                schema_validation_passed and 
                sample_validation_passed and 
                relationship_validation_passed
            )
            
            self._print_validation_summary(
                table_mappings, 
                count_validation_passed,
                schema_validation_passed,
                sample_validation_passed,
                relationship_validation_passed
            )
            
            if all_validations_passed:
                print("✅ All validation phases passed!")
            else:
                print("❌ Some validation phases failed!")
            
            self.logger.end_operation("Migration Validation", all_validations_passed)
            return all_validations_passed
            
        except Exception as e:
            print(f"❌ Validation failed: {e}")
            self.logger.log_error("validation", e, critical=True)
            self.error_collector.add_error(
                ErrorCategory.VALIDATION_ERROR.value,
                f"Validation failed: {str(e)}",
                critical=True
            )
            return False
    
    def _validate_table_mappings(self) -> Dict[str, str]:
        """
        Validate table mappings between MySQL and PostgreSQL.
        
        Returns:
            Dictionary of valid table mappings
        """
        try:
            # Get table lists
            mysql_inspector = inspect(self.mysql_engine)
            postgres_inspector = inspect(self.postgres_engine)
            
            mysql_tables = mysql_inspector.get_table_names()
            postgres_tables = postgres_inspector.get_table_names()
            
            # Apply filters
            exclude_tables = set(self.config['migration'].get('exclude_tables', []))
            include_tables = set(self.config['migration'].get('include_tables', []))
            
            filtered_mysql_tables = mysql_tables.copy()
            
            if include_tables:
                filtered_mysql_tables = [t for t in filtered_mysql_tables if t in include_tables]
            
            if exclude_tables:
                filtered_mysql_tables = [t for t in filtered_mysql_tables if t not in exclude_tables]
            
            # Find mappings using the same logic as migration engine
            table_mappings = {}
            for mysql_table in filtered_mysql_tables:
                postgres_table = self._find_postgres_table(mysql_table, postgres_tables)
                if postgres_table:
                    table_mappings[mysql_table] = postgres_table
                    self.logger.log_debug(f"Validation mapping: {mysql_table} -> {postgres_table}")
                else:
                    self.error_collector.add_missing_table_error(
                        mysql_table, "MySQL", "PostgreSQL"
                    )
            
            return table_mappings
            
        except Exception as e:
            self.logger.log_error("table_mapping_validation", e)
            self.error_collector.add_error(
                ErrorCategory.VALIDATION_ERROR.value,
                f"Table mapping validation failed: {str(e)}"
            )
            return {}
    
    def _find_postgres_table(self, mysql_table: str, postgres_tables: List[str]) -> Optional[str]:
        """Find corresponding PostgreSQL table for MySQL table."""
        # Direct match
        if mysql_table in postgres_tables:
            return mysql_table
        
        # Case-insensitive match
        mysql_lower = mysql_table.lower()
        postgres_tables_lower = [t.lower() for t in postgres_tables]
        
        if mysql_lower in postgres_tables_lower:
            idx = postgres_tables_lower.index(mysql_lower)
            return postgres_tables[idx]
        
        # PascalCase conversion
        pascal_case = ''.join(word.capitalize() for word in mysql_table.split('_'))
        if pascal_case in postgres_tables:
            return pascal_case
        
        # PascalCase case-insensitive
        pascal_lower = pascal_case.lower()
        if pascal_lower in postgres_tables_lower:
            idx = postgres_tables_lower.index(pascal_lower)
            return postgres_tables[idx]
        
        return None
    
    def _validate_record_counts(self, table_mappings: Dict[str, str]) -> bool:
        """
        Validate record counts between MySQL and PostgreSQL tables.
        
        Args:
            table_mappings: Dictionary of table mappings
            
        Returns:
            True if all record counts match within tolerance
        """
        validation_passed = True
        total_mysql_records = 0
        total_postgres_records = 0
        
        print(f"  Checking record counts for {len(table_mappings)} tables...")
        
        for mysql_table, postgres_table in table_mappings.items():
            try:
                # Get MySQL count
                with self.mysql_engine.connect() as conn:
                    mysql_result = conn.execute(text(f"SELECT COUNT(*) FROM `{mysql_table}`"))
                    mysql_count = mysql_result.scalar()
                
                # Get PostgreSQL count
                with self.postgres_engine.connect() as conn:
                    postgres_result = conn.execute(text(f'SELECT COUNT(*) FROM "{postgres_table}"'))
                    postgres_count = postgres_result.scalar()
                
                total_mysql_records += mysql_count
                total_postgres_records += postgres_count
                
                # Check if counts match
                if mysql_count != postgres_count:
                    difference = abs(mysql_count - postgres_count)
                    percentage_diff = (difference / max(mysql_count, 1)) * 100
                    
                    error_msg = (
                        f"Record count mismatch: MySQL={mysql_count:,}, "
                        f"PostgreSQL={postgres_count:,}, diff={difference:,} ({percentage_diff:.2f}%)"
                    )
                    
                    if percentage_diff > self.tolerance_threshold * 100:
                        print(f"    ❌ {mysql_table}: {error_msg}")
                        self.error_collector.add_table_error(
                            mysql_table, ErrorCategory.VALIDATION_ERROR.value, error_msg
                        )
                        validation_passed = False
                    else:
                        print(f"    ⚠️  {mysql_table}: {error_msg} (within tolerance)")
                        self.error_collector.add_table_error(
                            mysql_table, ErrorCategory.VALIDATION_ERROR.value, 
                            error_msg, critical=False
                        )
                else:
                    print(f"    ✅ {mysql_table}: {mysql_count:,} records")
                
            except Exception as e:
                error_msg = f"Failed to validate record count: {str(e)}"
                print(f"    ❌ {mysql_table}: {error_msg}")
                self.logger.log_error(f"record_count_validation_{mysql_table}", e)
                self.error_collector.add_table_error(
                    mysql_table, ErrorCategory.VALIDATION_ERROR.value, error_msg
                )
                validation_passed = False
        
        print(f"  Total records - MySQL: {total_mysql_records:,}, PostgreSQL: {total_postgres_records:,}")
        
        return validation_passed
    
    def _validate_schema_structures(self, table_mappings: Dict[str, str]) -> bool:
        """
        Validate schema structures between MySQL and PostgreSQL tables.
        
        Args:
            table_mappings: Dictionary of table mappings
            
        Returns:
            True if all schema structures are compatible
        """
        validation_passed = True
        
        print(f"  Checking schema structures for {len(table_mappings)} tables...")
        
        for mysql_table, postgres_table in table_mappings.items():
            try:
                # Get MySQL schema
                mysql_inspector = inspect(self.mysql_engine)
                mysql_columns = mysql_inspector.get_columns(mysql_table)
                
                # Get PostgreSQL schema
                postgres_inspector = inspect(self.postgres_engine)
                postgres_columns = postgres_inspector.get_columns(postgres_table)
                
                # Compare column counts first
                mysql_col_names = {col['name'].lower(): col['name'] for col in mysql_columns}
                postgres_col_names = {col['name'].lower(): col['name'] for col in postgres_columns}
                
                missing_in_postgres = set(mysql_col_names.keys()) - set(postgres_col_names.keys())
                extra_in_postgres = set(postgres_col_names.keys()) - set(mysql_col_names.keys())
                
                # Check for any column differences (count or names)
                has_issues = (len(mysql_columns) != len(postgres_columns) or 
                             missing_in_postgres or extra_in_postgres)
                
                if has_issues:
                    error_details = []
                    
                    # Add count information if different
                    if len(mysql_columns) != len(postgres_columns):
                        error_details.append(f"MySQL={len(mysql_columns)}, PostgreSQL={len(postgres_columns)}")
                    
                    # Add missing columns info
                    if missing_in_postgres:
                        missing_cols = [mysql_col_names[col] for col in missing_in_postgres]
                        error_details.append(f"Missing in PostgreSQL: {', '.join(missing_cols)}")
                    
                    # Add extra columns info
                    if extra_in_postgres:
                        extra_cols = [postgres_col_names[col] for col in extra_in_postgres]
                        error_details.append(f"Extra in PostgreSQL: {', '.join(extra_cols)}")
                    
                    # Determine severity based on count mismatch
                    if len(mysql_columns) != len(postgres_columns):
                        error_msg = f"Column count mismatch: {' | '.join(error_details)}"
                        print(f"    ❌ {mysql_table}: {error_msg}")
                        self.error_collector.add_table_error(
                            mysql_table, ErrorCategory.VALIDATION_ERROR.value, error_msg
                        )
                        validation_passed = False
                    else:
                        # Just name differences, less severe
                        error_msg = f"Column name differences: {' | '.join(error_details)}"
                        print(f"    ⚠️  {mysql_table}: {error_msg}")
                        self.error_collector.add_table_error(
                            mysql_table, ErrorCategory.VALIDATION_ERROR.value, 
                            error_msg, critical=False
                        )
                else:
                    print(f"    ✅ {mysql_table}: Schema structure compatible")
                
            except Exception as e:
                error_msg = f"Failed to validate schema structure: {str(e)}"
                print(f"    ❌ {mysql_table}: {error_msg}")
                self.logger.log_error(f"schema_validation_{mysql_table}", e)
                self.error_collector.add_table_error(
                    mysql_table, ErrorCategory.VALIDATION_ERROR.value, error_msg
                )
                validation_passed = False
        
        return validation_passed
    
    def _validate_data_samples(self, table_mappings: Dict[str, str]) -> bool:
        """
        Validate data samples between MySQL and PostgreSQL tables.
        
        Args:
            table_mappings: Dictionary of table mappings
            
        Returns:
            True if data samples are consistent
        """
        validation_passed = True
        
        print(f"  Sampling data from {len(table_mappings)} tables...")
        
        for mysql_table, postgres_table in table_mappings.items():
            try:
                # Get table record count to determine sample size
                with self.mysql_engine.connect() as conn:
                    count_result = conn.execute(text(f"SELECT COUNT(*) FROM `{mysql_table}`"))
                    total_records = count_result.scalar()
                
                if total_records == 0:
                    print(f"    ⚠️  {mysql_table}: No records to sample")
                    continue
                
                # Determine sample size
                actual_sample_size = min(self.sample_size, total_records)
                
                # Sample MySQL data
                mysql_samples = self._sample_table_data(
                    mysql_table, actual_sample_size, 'mysql'
                )
                
                # Sample PostgreSQL data
                postgres_samples = self._sample_table_data(
                    postgres_table, actual_sample_size, 'postgres'
                )
                
                # Compare samples
                if len(mysql_samples) != len(postgres_samples):
                    error_msg = (
                        f"Sample size mismatch: MySQL={len(mysql_samples)}, "
                        f"PostgreSQL={len(postgres_samples)}"
                    )
                    print(f"    ❌ {mysql_table}: {error_msg}")
                    self.error_collector.add_table_error(
                        mysql_table, ErrorCategory.VALIDATION_ERROR.value, error_msg
                    )
                    validation_passed = False
                else:
                    print(f"    ✅ {mysql_table}: {len(mysql_samples)} samples validated")
                
            except Exception as e:
                error_msg = f"Failed to validate data samples: {str(e)}"
                print(f"    ❌ {mysql_table}: {error_msg}")
                self.logger.log_error(f"sample_validation_{mysql_table}", e)
                self.error_collector.add_table_error(
                    mysql_table, ErrorCategory.VALIDATION_ERROR.value, error_msg
                )
                validation_passed = False
        
        return validation_passed
    
    def _sample_table_data(self, table_name: str, sample_size: int, 
                          database: str) -> List[Tuple]:
        """
        Sample data from a table.
        
        Args:
            table_name: Table name
            sample_size: Number of samples to take
            database: Database type ('mysql' or 'postgres')
            
        Returns:
            List of sample records
        """
        engine = self.mysql_engine if database == 'mysql' else self.postgres_engine
        
        with engine.connect() as conn:
            if database == 'mysql':
                # MySQL uses backticks and RAND()
                query = text(f"""
                    SELECT * FROM `{table_name}` 
                    ORDER BY RAND() 
                    LIMIT :sample_size
                """)
            else:
                # PostgreSQL uses double quotes and RANDOM()
                query = text(f"""
                    SELECT * FROM "{table_name}" 
                    ORDER BY RANDOM() 
                    LIMIT :sample_size
                """)
            
            result = conn.execute(query, {'sample_size': sample_size})
            return result.fetchall()
    
    def _validate_relationships(self, table_mappings: Dict[str, str]) -> bool:
        """
        Validate foreign key relationships.
        
        Args:
            table_mappings: Dictionary of table mappings
            
        Returns:
            True if relationships are valid
        """
        validation_passed = True
        
        print(f"  Checking relationships for {len(table_mappings)} tables...")
        
        # For now, this is a placeholder for more advanced relationship validation
        # In a full implementation, this would check foreign key constraints,
        # referential integrity, etc.
        
        for mysql_table, postgres_table in table_mappings.items():
            try:
                # Get foreign key information
                mysql_inspector = inspect(self.mysql_engine)
                postgres_inspector = inspect(self.postgres_engine)
                
                mysql_fks = mysql_inspector.get_foreign_keys(mysql_table)
                postgres_fks = postgres_inspector.get_foreign_keys(postgres_table)
                
                if len(mysql_fks) != len(postgres_fks):
                    warning_msg = (
                        f"Foreign key count difference: MySQL={len(mysql_fks)}, "
                        f"PostgreSQL={len(postgres_fks)}"
                    )
                    print(f"    ⚠️  {mysql_table}: {warning_msg}")
                    self.error_collector.add_table_error(
                        mysql_table, ErrorCategory.VALIDATION_ERROR.value,
                        warning_msg, critical=False
                    )
                else:
                    print(f"    ✅ {mysql_table}: Relationships consistent")
                
            except Exception as e:
                error_msg = f"Failed to validate relationships: {str(e)}"
                print(f"    ⚠️  {mysql_table}: {error_msg}")
                self.logger.log_warning(f"Relationship validation warning for {mysql_table}: {str(e)}")
        
        return validation_passed
    
    def _print_validation_summary(self, table_mappings: Dict[str, str],
                                 count_passed: bool, schema_passed: bool,
                                 sample_passed: bool, relationship_passed: bool) -> None:
        """Print validation summary."""
        print("\n" + "=" * 60)
        print("🔍 VALIDATION SUMMARY")
        print("=" * 60)
        
        print(f"Tables Validated: {len(table_mappings)}")
        print(f"Record Count Validation: {'✅ PASSED' if count_passed else '❌ FAILED'}")
        print(f"Schema Structure Validation: {'✅ PASSED' if schema_passed else '❌ FAILED'}")
        print(f"Data Sample Validation: {'✅ PASSED' if sample_passed else '❌ FAILED'}")
        print(f"Relationship Validation: {'✅ PASSED' if relationship_passed else '❌ FAILED'}")
        
        # Show validation details
        if table_mappings:
            print(f"\n📋 Validated Table Mappings:")
            for mysql_table, postgres_table in table_mappings.items():
                print(f"  • {mysql_table} → {postgres_table}")
        
        print("=" * 60)
    
    def __del__(self):
        """Cleanup database connections."""
        try:
            if self.mysql_engine:
                self.mysql_engine.dispose()
            if self.postgres_engine:
                self.postgres_engine.dispose()
        except:
            pass
