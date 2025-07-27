"""
Migration Engine Module

Core migration orchestrator that handles the complete migration process
from MySQL to PostgreSQL with comprehensive error handling and monitoring.
"""

import logging
import time
import concurrent.futures
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime

try:
    import mysql.connector
    import psycopg
    from sqlalchemy import create_engine, text, inspect
    from sqlalchemy.exc import SQLAlchemyError
except ImportError as e:
    print(f"Missing required database driver: {e}")
    print("Please install with: pip install -r requirements.txt")
    exit(1)

from .utils.logger import MigrationLogger
from .utils.status_monitor import StatusMonitor, MigrationPhase
from .utils.error_collector import ErrorCollector, ErrorCategory
from .legacy_migrator import MyPg


class MigrationEngine:
    """Production-grade migration orchestrator."""
    
    def __init__(self, config: Dict[str, Any], status_monitor: StatusMonitor, 
                 error_collector: ErrorCollector):
        """
        Initialize migration engine.
        
        Args:
            config: Configuration dictionary
            status_monitor: Status monitoring instance
            error_collector: Error collection instance
        """
        self.config = config
        self.status_monitor = status_monitor
        self.error_collector = error_collector
        self.logger = MigrationLogger(__name__)
        
        # Database connections
        self.mysql_engine = None
        self.postgres_engine = None
        self.mysql_connection = None
        self.postgres_connection = None
        
        # Migration settings
        self.batch_size = config['migration']['batch_size']
        self.max_workers = config['migration']['max_workers']
        self.continue_on_error = config['migration']['continue_on_error']
        self.fail_on_missing_tables = config['migration']['fail_on_missing_tables']
        self.ignore_generated_columns = config['migration'].get('ignore_generated_columns', True)
        self.disable_foreign_keys = config['migration'].get('disable_foreign_keys', True)
        self.truncate_target_tables = config['migration'].get('truncate_target_tables', False)
        self.exclude_tables = set(config['migration'].get('exclude_tables', []))
        self.include_tables = set(config['migration'].get('include_tables', []))
        
        # Initialize legacy migrator
        self.legacy_migrator = None
    
    def _create_connection_string(self, db_config: Dict[str, Any], db_type: str) -> str:
        """Create SQLAlchemy connection string."""
        if db_type == 'mysql':
            return (
                f"mysql+mysqlconnector://{db_config['username']}:{db_config['password']}"
                f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
            )
        elif db_type == 'postgres':
            return (
                f"postgresql+psycopg://{db_config['username']}:{db_config['password']}"
                f"@{db_config['host']}:{db_config['port']}/{db_config['database']}"
            )
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    def _create_engines(self) -> bool:
        """
        Create database engines.
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Create MySQL engine
            mysql_conn_str = self._create_connection_string(
                self.config['mysql'], 'mysql'
            )
            self.mysql_engine = create_engine(
                mysql_conn_str,
                pool_timeout=self.config['migration']['connection_timeout'],
                pool_recycle=3600,
                echo=False
            )
            
            # Create PostgreSQL engine
            postgres_conn_str = self._create_connection_string(
                self.config['postgres'], 'postgres'
            )
            self.postgres_engine = create_engine(
                postgres_conn_str,
                pool_timeout=self.config['migration']['connection_timeout'],
                pool_recycle=3600,
                echo=False
            )
            
            self.logger.log_info("Database engines created successfully")
            return True
            
        except Exception as e:
            self.logger.log_error("engine_creation", e, critical=True)
            self.error_collector.add_error(
                ErrorCategory.CONNECTION.value,
                f"Failed to create database engines: {str(e)}",
                critical=True
            )
            return False
    
    def test_connections(self) -> bool:
        """
        Test database connections.
        
        Returns:
            True if all connections successful, False otherwise
        """
        self.logger.start_operation("Connection Testing")
        self.status_monitor.set_phase(MigrationPhase.CONNECTING)
        
        success = True
        
        try:
            # Create engines
            if not self._create_engines():
                return False
            
            # Test MySQL connection
            try:
                with self.mysql_engine.connect() as conn:
                    result = conn.execute(text("SELECT 1 as test"))
                    result.fetchone()
                print("✅ MySQL connection successful")
                self.logger.log_info("MySQL connection test passed")
            except Exception as e:
                print(f"❌ MySQL connection failed: {e}")
                self.logger.log_error("mysql_connection_test", e, critical=True)
                self.error_collector.add_connection_error("MySQL", str(e))
                success = False
            
            # Test PostgreSQL connection
            try:
                with self.postgres_engine.connect() as conn:
                    result = conn.execute(text("SELECT 1 as test"))
                    result.fetchone()
                print("✅ PostgreSQL connection successful")
                self.logger.log_info("PostgreSQL connection test passed")
            except Exception as e:
                print(f"❌ PostgreSQL connection failed: {e}")
                self.logger.log_error("postgres_connection_test", e, critical=True)
                self.error_collector.add_connection_error("PostgreSQL", str(e))
                success = False
            
            self.logger.end_operation("Connection Testing", success)
            return success
            
        except Exception as e:
            self.logger.log_error("connection_testing", e, critical=True)
            self.error_collector.add_error(
                ErrorCategory.CONNECTION.value,
                f"Connection testing failed: {str(e)}",
                critical=True
            )
            return False
    
    def _get_table_list(self) -> Tuple[List[str], List[str]]:
        """
        Get list of tables from both databases.
        
        Returns:
            Tuple of (mysql_tables, postgres_tables)
        """
        mysql_tables = []
        postgres_tables = []
        
        try:
            # Get MySQL tables
            mysql_inspector = inspect(self.mysql_engine)
            mysql_tables = mysql_inspector.get_table_names()
            self.logger.log_info(f"Found {len(mysql_tables)} MySQL tables")
            
            # Get PostgreSQL tables
            postgres_inspector = inspect(self.postgres_engine)
            postgres_tables = postgres_inspector.get_table_names()
            self.logger.log_info(f"Found {len(postgres_tables)} PostgreSQL tables")
            
        except Exception as e:
            self.logger.log_error("table_list_retrieval", e, critical=True)
            self.error_collector.add_error(
                ErrorCategory.SCHEMA_ANALYSIS.value,
                f"Failed to retrieve table lists: {str(e)}",
                critical=True
            )
        
        return mysql_tables, postgres_tables
    
    def _filter_tables(self, mysql_tables: List[str]) -> List[str]:
        """
        Apply include/exclude filters to table list.
        
        Args:
            mysql_tables: List of MySQL table names
            
        Returns:
            Filtered list of tables to migrate
        """
        filtered_tables = mysql_tables.copy()
        
        # Apply include filter
        if self.include_tables:
            filtered_tables = [t for t in filtered_tables if t in self.include_tables]
            self.logger.log_info(f"Include filter applied: {len(filtered_tables)} tables")
        
        # Apply exclude filter
        if self.exclude_tables:
            filtered_tables = [t for t in filtered_tables if t not in self.exclude_tables]
            self.logger.log_info(f"Exclude filter applied: {len(filtered_tables)} tables")
        
        return filtered_tables
    
    def _analyze_schema(self, tables_to_migrate: List[str]) -> Dict[str, Any]:
        """
        Analyze database schemas and create migration plan.
        
        Args:
            tables_to_migrate: List of tables to migrate
            
        Returns:
            Dictionary containing schema analysis results
        """
        self.logger.start_operation("Schema Analysis")
        self.status_monitor.set_phase(MigrationPhase.ANALYZING)
        
        analysis = {
            'mysql_tables': {},
            'postgres_tables': {},
            'table_mappings': {},
            'missing_tables': [],
            'total_records': 0
        }
        
        try:
            # Analyze MySQL tables
            with self.mysql_engine.connect() as conn:
                for table_name in tables_to_migrate:
                    try:
                        # Get record count
                        result = conn.execute(text(f"SELECT COUNT(*) FROM `{table_name}`"))
                        record_count = result.scalar()
                        
                        analysis['mysql_tables'][table_name] = {
                            'record_count': record_count,
                            'exists': True
                        }
                        analysis['total_records'] += record_count
                        
                        self.logger.log_debug(f"MySQL table '{table_name}': {record_count:,} records")
                        
                    except Exception as e:
                        self.logger.log_error(f"mysql_table_analysis_{table_name}", e)
                        self.error_collector.add_schema_error(
                            table_name, f"Failed to analyze MySQL table: {str(e)}"
                        )
                        analysis['mysql_tables'][table_name] = {
                            'record_count': 0,
                            'exists': False,
                            'error': str(e)
                        }
            
            # Check PostgreSQL tables
            postgres_inspector = inspect(self.postgres_engine)
            postgres_table_names = postgres_inspector.get_table_names()
            
            for table_name in tables_to_migrate:
                postgres_table = self._find_postgres_table(table_name, postgres_table_names)
                if postgres_table:
                    analysis['table_mappings'][table_name] = postgres_table
                    analysis['postgres_tables'][postgres_table] = {'exists': True}
                    self.logger.log_debug(f"Table mapping: '{table_name}' -> '{postgres_table}'")
                else:
                    analysis['missing_tables'].append(table_name)
                    self.error_collector.add_missing_table_error(table_name, "MySQL", "PostgreSQL")
            
            self.logger.end_operation("Schema Analysis", True)
            
        except Exception as e:
            self.logger.log_error("schema_analysis", e, critical=True)
            self.error_collector.add_error(
                ErrorCategory.SCHEMA_ANALYSIS.value,
                f"Schema analysis failed: {str(e)}",
                critical=True
            )
        
        return analysis
    
    def _find_postgres_table(self, mysql_table: str, postgres_tables: List[str]) -> Optional[str]:
        """
        Find corresponding PostgreSQL table for MySQL table.
        
        Args:
            mysql_table: MySQL table name
            postgres_tables: List of PostgreSQL table names
            
        Returns:
            PostgreSQL table name if found, None otherwise
        """
        # Direct match
        if mysql_table in postgres_tables:
            return mysql_table
        
        # Case-insensitive match
        mysql_lower = mysql_table.lower()
        for pg_table in postgres_tables:
            if pg_table.lower() == mysql_lower:
                return pg_table
        
        # PascalCase conversion (snake_case to PascalCase)
        pascal_case = ''.join(word.capitalize() for word in mysql_table.split('_'))
        if pascal_case in postgres_tables:
            return pascal_case
        
        # Check if PascalCase matches case-insensitively
        for pg_table in postgres_tables:
            if pg_table.lower() == pascal_case.lower():
                return pg_table
        
        return None
    
    def dry_run(self) -> bool:
        """
        Perform a dry run (simulation) of the migration.
        
        Returns:
            True if dry run successful, False otherwise
        """
        self.logger.start_operation("Dry Run Migration")
        print("🔍 Starting Dry Run (no data will be modified)")
        
        try:
            # Test connections
            if not self.test_connections():
                print("❌ Connection test failed during dry run")
                return False
            
            # Get table lists
            mysql_tables, postgres_tables = self._get_table_list()
            if not mysql_tables:
                print("❌ No MySQL tables found")
                return False
            
            # Filter tables
            tables_to_migrate = self._filter_tables(mysql_tables)
            if not tables_to_migrate:
                print("❌ No tables to migrate after filtering")
                return False
            
            print(f"📊 Tables to migrate: {len(tables_to_migrate)}")
            
            # Analyze schemas
            analysis = self._analyze_schema(tables_to_migrate)
            
            # Print dry run results
            self._print_dry_run_results(analysis)
            
            # Check for blocking issues
            if analysis['missing_tables'] and self.fail_on_missing_tables:
                print("❌ Missing tables found and fail_on_missing_tables is enabled")
                return False
            
            print("✅ Dry run completed successfully")
            self.logger.end_operation("Dry Run Migration", True)
            return True
            
        except Exception as e:
            print(f"❌ Dry run failed: {e}")
            self.logger.log_error("dry_run", e, critical=True)
            self.error_collector.add_error(
                ErrorCategory.MIGRATION_ERROR.value,
                f"Dry run failed: {str(e)}",
                critical=True
            )
            return False
    
    def _print_dry_run_results(self, analysis: Dict[str, Any]) -> None:
        """Print dry run analysis results."""
        print("\n" + "=" * 50)
        print("🔍 DRY RUN RESULTS")
        print("=" * 50)
        
        print(f"Total records to migrate: {analysis['total_records']:,}")
        print(f"Tables with mappings: {len(analysis['table_mappings'])}")
        print(f"Missing tables: {len(analysis['missing_tables'])}")
        
        if analysis['missing_tables']:
            print(f"\n⚠️  Missing PostgreSQL tables:")
            for table in analysis['missing_tables']:
                print(f"   • {table}")
        
        print(f"\n📋 Table mappings:")
        for mysql_table, postgres_table in analysis['table_mappings'].items():
            record_count = analysis['mysql_tables'][mysql_table]['record_count']
            print(f"   • {mysql_table} → {postgres_table} ({record_count:,} records)")
        
        print("=" * 50)
    
    def migrate(self) -> bool:
        """
        Execute the complete migration process.
        
        Returns:
            True if migration successful, False otherwise
        """
        self.logger.start_operation("Data Migration")
        print("🚀 Starting Data Migration")
        
        try:
            # Initialize migration
            if not self._initialize_migration():
                return False
            
            # Get and analyze tables
            mysql_tables, postgres_tables = self._get_table_list()
            tables_to_migrate = self._filter_tables(mysql_tables)
            
            if not tables_to_migrate:
                print("❌ No tables to migrate")
                return False
            
            analysis = self._analyze_schema(tables_to_migrate)
            
            # Start monitoring
            valid_tables = [t for t in tables_to_migrate if t in analysis['table_mappings']]
            self.status_monitor.start_migration(valid_tables)
            
            # Execute migration using legacy migrator
            success = self._execute_migration_with_legacy(analysis)
            
            # Complete monitoring
            self.status_monitor.complete_migration(success)
            
            if success:
                print("✅ Migration completed successfully")
            else:
                print("❌ Migration completed with errors")
            
            self.logger.end_operation("Data Migration", success)
            return success
            
        except Exception as e:
            print(f"❌ Migration failed: {e}")
            self.logger.log_error("migration", e, critical=True)
            self.error_collector.add_error(
                ErrorCategory.MIGRATION_ERROR.value,
                f"Migration failed: {str(e)}",
                critical=True
            )
            self.status_monitor.complete_migration(False)
            return False
    
    def _initialize_migration(self) -> bool:
        """Initialize migration prerequisites."""
        # Test connections
        if not self.test_connections():
            return False
        
        # Initialize legacy migrator
        try:
            self.legacy_migrator = MyPg(self.config)
            return True
        except Exception as e:
            self.logger.log_error("legacy_migrator_init", e, critical=True)
            self.error_collector.add_error(
                ErrorCategory.MIGRATION_ERROR.value,
                f"Failed to initialize legacy migrator: {str(e)}",
                critical=True
            )
            return False
    
    def _execute_migration_with_legacy(self, analysis: Dict[str, Any]) -> bool:
        """
        Execute migration using the legacy migrator.
        
        Args:
            analysis: Schema analysis results
            
        Returns:
            True if successful, False otherwise
        """
        self.status_monitor.set_phase(MigrationPhase.MIGRATING)
        
        try:
            # Disable foreign key constraints before migration
            print("🔓 Managing foreign key constraints...")
            fk_disabled = self.legacy_migrator._disable_foreign_key_checks()
            
            # Truncate target tables if requested
            if self.truncate_target_tables:
                print("🗑️  Preparing target tables...")
                self.legacy_migrator._truncate_target_tables(analysis['table_mappings'])
            
            try:
                # Execute migration for each table
                for mysql_table, postgres_table in analysis['table_mappings'].items():
                    record_count = analysis['mysql_tables'][mysql_table]['record_count']
                    
                    try:
                        self.status_monitor.start_table(mysql_table, record_count)
                        
                        # Use legacy migrator to migrate table
                        success = self.legacy_migrator.migrate_table(
                            mysql_table, 
                            postgres_table,
                            record_count,
                            self.status_monitor,
                            self.error_collector
                        )
                        
                        if success:
                            self.status_monitor.complete_table(mysql_table, True)
                        else:
                            error_msg = f"Table migration failed"
                            self.status_monitor.complete_table(mysql_table, False, error_msg)
                            
                            if not self.continue_on_error:
                                return False
                    
                    except Exception as e:
                        error_msg = f"Exception during table migration: {str(e)}"
                        self.logger.log_error(f"table_migration_{mysql_table}", e)
                        self.error_collector.add_table_error(
                            mysql_table, ErrorCategory.MIGRATION_ERROR.value, error_msg
                        )
                        self.status_monitor.complete_table(mysql_table, False, error_msg)
                        
                        if not self.continue_on_error:
                            return False
                
                migration_success = True
                
            finally:
                # Always re-enable foreign key constraints, even if migration failed
                if fk_disabled:
                    print("🔒 Restoring foreign key constraints...")
                    self.legacy_migrator._enable_foreign_key_checks()
            
            return migration_success
            
        except Exception as e:
            self.logger.log_error("migration_execution", e, critical=True)
            return False
    
    def __del__(self):
        """Cleanup database connections."""
        try:
            if self.mysql_engine:
                self.mysql_engine.dispose()
            if self.postgres_engine:
                self.postgres_engine.dispose()
        except:
            pass  # Ignore cleanup errors
