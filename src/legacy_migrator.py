"""
Legacy Migrator Module

Original migration implementation with schema detection, caching,
and batch processing capabilities.
"""

import json
import logging
import time
import math
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime

try:
    import mysql.connector
    import psycopg
    from sqlalchemy import create_engine, text, inspect, MetaData, Table
    from sqlalchemy.exc import SQLAlchemyError
except ImportError as e:
    print(f"Missing required database driver: {e}")
    print("Please install with: pip install -r requirements.txt")
    exit(1)

from .utils.logger import MigrationLogger
from .utils.status_monitor import StatusMonitor
from .utils.error_collector import ErrorCollector, ErrorCategory


class MyPg:
    """Legacy migrator class with enhanced functionality."""
    
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the legacy migrator.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.logger = MigrationLogger(__name__)
        
        # Database configuration
        self.mysql_config = config['mysql']
        self.postgres_config = config['postgres']
        
        # Migration settings
        self.batch_size = config['migration']['batch_size']
        self.max_workers = config['migration']['max_workers']
        self.ignore_generated_columns = config['migration'].get('ignore_generated_columns', True)
        self.disable_foreign_keys = config['migration'].get('disable_foreign_keys', True)
        self.truncate_target_tables = config['migration'].get('truncate_target_tables', False)
        
        # Database engines and connections
        self.mysql_engine = None
        self.postgres_engine = None
        
        # Schema cache
        self.postgres_schemas_cache: Dict[str, Dict] = {}
        self.mysql_schemas_cache: Dict[str, Dict] = {}
        
        # Connection pools
        self._mysql_pool = None
        self._postgres_pool = None
        
        self._initialize_engines()
    
    def _initialize_engines(self) -> None:
        """Initialize database engines."""
        try:
            # MySQL engine
            mysql_conn_str = (
                f"mysql+mysqlconnector://{self.mysql_config['username']}:"
                f"{self.mysql_config['password']}@{self.mysql_config['host']}:"
                f"{self.mysql_config['port']}/{self.mysql_config['database']}"
            )
            self.mysql_engine = create_engine(mysql_conn_str, pool_recycle=3600)
            
            # PostgreSQL engine
            postgres_conn_str = (
                f"postgresql+psycopg://{self.postgres_config['username']}:"
                f"{self.postgres_config['password']}@{self.postgres_config['host']}:"
                f"{self.postgres_config['port']}/{self.postgres_config['database']}"
            )
            self.postgres_engine = create_engine(postgres_conn_str, pool_recycle=3600)
            
            self.logger.log_info("Database engines initialized")
            
        except Exception as e:
            self.logger.log_error("engine_initialization", e, critical=True)
            raise
    
    def get_postgres_schema(self, table_name: str) -> Optional[Dict]:
        """
        Get PostgreSQL table schema with caching.
        
        Args:
            table_name: Name of the PostgreSQL table (will find actual table with case-insensitive matching)
            
        Returns:
            Dictionary containing schema information or None if not found
        """
        if table_name in self.postgres_schemas_cache:
            return self.postgres_schemas_cache[table_name]

        try:
            # First, find the actual PostgreSQL table name (handle case sensitivity)
            actual_table_name = self._find_actual_postgres_table(table_name)
            if not actual_table_name:
                self.logger.log_error("get_postgres_schema", 
                                    f"No PostgreSQL table found matching '{table_name}'", 
                                    critical=True)
                return None

            with self.postgres_engine.connect() as conn:
                # Get column information from information_schema using the actual table name
                query = text("""
                    SELECT 
                        column_name,
                        data_type,
                        is_nullable,
                        column_default,
                        character_maximum_length,
                        numeric_precision,
                        numeric_scale,
                        is_generated,
                        generation_expression
                    FROM information_schema.columns 
                    WHERE table_name = :table_name 
                    AND table_schema = 'public'
                    ORDER BY ordinal_position
                """)
                
                result = conn.execute(query, {'table_name': actual_table_name})
                columns = []
                
                for row in result:
                    # Check for generated columns and skip if configured to ignore them
                    is_generated = getattr(row, 'is_generated', 'NEVER') != 'NEVER'
                    if is_generated and self.ignore_generated_columns:
                        self.logger.log_debug(f"Skipping generated column '{row.column_name}' in table '{actual_table_name}'")
                        continue
                        
                    columns.append({
                        'name': row.column_name,
                        'type': row.data_type,
                        'nullable': row.is_nullable == 'YES',
                        'default': row.column_default,
                        'max_length': row.character_maximum_length,
                        'precision': row.numeric_precision,
                        'scale': row.numeric_scale,
                        'is_generated': is_generated
                    })
                
                if columns:
                    schema = {
                        'table_name': actual_table_name,  # Use actual table name
                        'columns': columns,
                        'column_names': [col['name'] for col in columns]
                    }
                    # Cache under both names for future lookups
                    self.postgres_schemas_cache[table_name] = schema
                    self.postgres_schemas_cache[actual_table_name] = schema
                    self.logger.log_debug(f"Cached schema for PostgreSQL table '{table_name}' -> '{actual_table_name}'")
                    return schema
                else:
                    self.logger.log_warning(f"No schema found for PostgreSQL table '{actual_table_name}'")
                    return None
                    
        except Exception as e:
            self.logger.log_error(f"postgres_schema_{table_name}", e)
            
            # Fallback to SQLAlchemy inspector with actual table name
            try:
                inspector = inspect(self.postgres_engine)
                columns = inspector.get_columns(actual_table_name)
                
                if columns:
                    filtered_columns = []
                    for col in columns:
                        # Check for generated columns in SQLAlchemy (computed columns)
                        is_generated = getattr(col, 'computed', None) is not None
                        if is_generated and self.ignore_generated_columns:
                            self.logger.log_debug(f"Skipping generated column '{col['name']}' in table '{actual_table_name}' (fallback)")
                            continue
                            
                        filtered_columns.append({
                            'name': col['name'],
                            'type': str(col['type']),
                            'nullable': col['nullable'],
                            'default': col.get('default'),
                            'is_generated': is_generated
                        })
                    
                    schema = {
                        'table_name': actual_table_name,
                        'columns': filtered_columns,
                        'column_names': [col['name'] for col in filtered_columns]
                    }
                    # Cache under both names
                    self.postgres_schemas_cache[table_name] = schema
                    self.postgres_schemas_cache[actual_table_name] = schema
                    self.logger.log_debug(f"Fallback schema cached for '{table_name}' -> '{actual_table_name}'")
                    return schema
                    
            except Exception as fallback_e:
                self.logger.log_error(f"postgres_schema_fallback_{table_name}", fallback_e)
            
            return None

    def _find_actual_postgres_table(self, table_name: str) -> Optional[str]:
        """
        Find the actual PostgreSQL table name using case-insensitive matching.
        
        Args:
            table_name: The table name to search for
            
        Returns:
            The actual table name in PostgreSQL or None if not found
        """
        try:
            inspector = inspect(self.postgres_engine)
            postgres_tables = inspector.get_table_names()
            
            # Direct match first
            if table_name in postgres_tables:
                return table_name
            
            # Case-insensitive match
            table_lower = table_name.lower()
            for pg_table in postgres_tables:
                if pg_table.lower() == table_lower:
                    return pg_table
            
            # PascalCase conversion (snake_case to PascalCase)
            pascal_case = ''.join(word.capitalize() for word in table_name.split('_'))
            if pascal_case in postgres_tables:
                return pascal_case
            
            # Check if PascalCase matches case-insensitively
            for pg_table in postgres_tables:
                if pg_table.lower() == pascal_case.lower():
                    return pg_table
                    
            self.logger.log_debug(f"No PostgreSQL table found matching '{table_name}' in tables: {postgres_tables[:10]}...")
            return None
            
        except Exception as e:
            self.logger.log_error("find_postgres_table", e)
            return None
    
    def get_mysql_schema(self, table_name: str) -> Optional[Dict]:
        """
        Get MySQL table schema with caching.
        
        Args:
            table_name: Name of the MySQL table
            
        Returns:
            Dictionary containing schema information or None if not found
        """
        if table_name in self.mysql_schemas_cache:
            return self.mysql_schemas_cache[table_name]
        
        try:
            inspector = inspect(self.mysql_engine)
            columns = inspector.get_columns(table_name)
            
            if columns:
                schema = {
                    'table_name': table_name,
                    'columns': [
                        {
                            'name': col['name'],
                            'type': str(col['type']),
                            'nullable': col['nullable'],
                            'default': col.get('default')
                        }
                        for col in columns
                    ],
                    'column_names': [col['name'] for col in columns]
                }
                self.mysql_schemas_cache[table_name] = schema
                self.logger.log_debug(f"Cached schema for MySQL table '{table_name}'")
                return schema
            else:
                self.logger.log_warning(f"No schema found for MySQL table '{table_name}'")
                return None
                
        except Exception as e:
            self.logger.log_error(f"mysql_schema_{table_name}", e)
            return None

    def _disable_foreign_key_checks(self) -> bool:
        """
        Disable foreign key constraint checks in PostgreSQL.
        
        Returns:
            True if successful, False otherwise
        """
        if not self.disable_foreign_keys:
            self.logger.log_debug("Foreign key disabling is disabled in configuration")
            return True
            
        try:
            with self.postgres_engine.connect() as conn:
                # Get all foreign key constraints in the database
                get_fks_query = text("""
                    SELECT tc.constraint_name, tc.table_name
                    FROM information_schema.table_constraints tc
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                    AND tc.table_schema = 'public'
                """)
                
                result = conn.execute(get_fks_query)
                fk_constraints = list(result)
                
                if fk_constraints:
                    # Store constraints for later re-enabling
                    self._disabled_constraints = fk_constraints
                    
                    # Disable each foreign key constraint
                    for constraint in fk_constraints:
                        disable_query = text(f'ALTER TABLE "{constraint.table_name}" DISABLE TRIGGER ALL')
                        conn.execute(disable_query)
                        self.logger.log_debug(f"Disabled triggers for table '{constraint.table_name}'")
                    
                    conn.commit()
                    self.logger.log_info(f"Disabled {len(fk_constraints)} foreign key constraints")
                    print(f"🔓 Disabled {len(fk_constraints)} foreign key constraints for migration")
                else:
                    self.logger.log_info("No foreign key constraints found to disable")
                    self._disabled_constraints = []
                
                return True
                
        except Exception as e:
            self.logger.log_error("disable_foreign_keys", e)
            print(f"⚠️  Warning: Could not disable foreign key constraints: {e}")
            return False

    def _enable_foreign_key_checks(self) -> bool:
        """
        Re-enable foreign key constraint checks in PostgreSQL.
        
        Returns:
            True if successful, False otherwise
        """
        if not self.disable_foreign_keys:
            return True
            
        try:
            with self.postgres_engine.connect() as conn:
                if hasattr(self, '_disabled_constraints') and self._disabled_constraints:
                    # Get unique table names from disabled constraints
                    table_names = set(constraint.table_name for constraint in self._disabled_constraints)
                    
                    # Re-enable triggers for each table
                    for table_name in table_names:
                        enable_query = text(f'ALTER TABLE "{table_name}" ENABLE TRIGGER ALL')
                        conn.execute(enable_query)
                        self.logger.log_debug(f"Re-enabled triggers for table '{table_name}'")
                    
                    conn.commit()
                    self.logger.log_info(f"Re-enabled foreign key constraints for {len(table_names)} tables")
                    print(f"🔒 Re-enabled foreign key constraints")
                else:
                    self.logger.log_info("No foreign key constraints to re-enable")
                
                return True
                
        except Exception as e:
            self.logger.log_error("enable_foreign_keys", e)
            print(f"⚠️  Warning: Could not re-enable foreign key constraints: {e}")
            return False

    def _truncate_target_tables(self, table_mappings: Dict[str, str]) -> bool:
        """
        Truncate target PostgreSQL tables before migration.
        
        Args:
            table_mappings: Dictionary of mysql_table -> postgres_table mappings
            
        Returns:
            True if successful, False otherwise
        """
        if not self.truncate_target_tables:
            self.logger.log_debug("Table truncation is disabled in configuration")
            return True
            
        try:
            with self.postgres_engine.connect() as conn:
                postgres_tables = list(set(table_mappings.values()))  # Get unique PostgreSQL table names
                
                if postgres_tables:
                    print(f"🗑️  Truncating {len(postgres_tables)} PostgreSQL tables...")
                    
                    # Truncate each table
                    for table_name in postgres_tables:
                        truncate_query = text(f'TRUNCATE TABLE "{table_name}" RESTART IDENTITY CASCADE')
                        conn.execute(truncate_query)
                        self.logger.log_debug(f"Truncated table '{table_name}'")
                    
                    conn.commit()
                    self.logger.log_info(f"Truncated {len(postgres_tables)} PostgreSQL tables")
                    print(f"✅ Truncated {len(postgres_tables)} tables (data cleared)")
                else:
                    self.logger.log_info("No tables to truncate")
                
                return True
                
        except Exception as e:
            self.logger.log_error("truncate_tables", e)
            print(f"⚠️  Warning: Could not truncate tables: {e}")
            return False
    
    def migrate_table(self, mysql_table: str, postgres_table: str, 
                     record_count: int, status_monitor: StatusMonitor,
                     error_collector: ErrorCollector) -> bool:
        """
        Migrate a single table from MySQL to PostgreSQL.
        
        Args:
            mysql_table: Source MySQL table name
            postgres_table: Target PostgreSQL table name
            record_count: Total number of records to migrate
            status_monitor: Status monitoring instance
            error_collector: Error collection instance
            
        Returns:
            True if successful, False otherwise
        """
        self.logger.log_table_start(mysql_table, record_count)
        start_time = time.time()
        
        try:
            # Get schemas
            mysql_schema = self.get_mysql_schema(mysql_table)
            postgres_schema = self.get_postgres_schema(postgres_table)
            
            if not mysql_schema:
                error_msg = f"Could not retrieve MySQL schema for table '{mysql_table}'"
                error_collector.add_schema_error(mysql_table, error_msg, critical=True)
                return False
            
            if not postgres_schema:
                error_msg = f"Could not retrieve PostgreSQL schema for table '{postgres_table}'"
                error_collector.add_schema_error(postgres_table, error_msg, critical=True)
                return False
            
            # Calculate batches
            total_batches = max(1, math.ceil(record_count / self.batch_size))
            processed_records = 0
            
            self.logger.log_info(
                f"Migrating '{mysql_table}' to '{postgres_table}' "
                f"({record_count:,} records in {total_batches} batches)"
            )
            
            # Migrate in batches
            for batch_num in range(total_batches):
                offset = batch_num * self.batch_size
                
                try:
                    batch_records = self._migrate_batch(
                        mysql_table, postgres_table,
                        mysql_schema, postgres_schema,
                        offset, self.batch_size
                    )
                    
                    processed_records += batch_records
                    
                    # Update progress
                    status_monitor.update_table_progress(
                        mysql_table, processed_records, 
                        batch_num + 1, total_batches
                    )
                    
                    self.logger.log_batch_progress(
                        mysql_table, batch_num + 1, total_batches, processed_records
                    )
                    
                except Exception as batch_e:
                    error_msg = f"Batch {batch_num + 1} failed: {str(batch_e)}"
                    self.logger.log_error(f"batch_{mysql_table}_{batch_num}", batch_e)
                    error_collector.add_table_error(
                        mysql_table, ErrorCategory.MIGRATION_ERROR.value, error_msg
                    )
                    
                    # Continue with next batch or fail based on configuration
                    if not self.config['migration']['continue_on_error']:
                        return False
            
            # Log completion
            duration = time.time() - start_time
            self.logger.log_table_complete(mysql_table, processed_records, duration)
            
            return True
            
        except Exception as e:
            duration = time.time() - start_time
            self.logger.log_error(f"table_migration_{mysql_table}", e, critical=True)
            error_collector.add_table_error(
                mysql_table, ErrorCategory.MIGRATION_ERROR.value, 
                f"Table migration failed: {str(e)}", critical=True
            )
            return False
    
    def _migrate_batch(self, mysql_table: str, postgres_table: str,
                      mysql_schema: Dict, postgres_schema: Dict,
                      offset: int, limit: int) -> int:
        """
        Migrate a single batch of records.
        
        Args:
            mysql_table: Source MySQL table name
            postgres_table: Target PostgreSQL table name
            mysql_schema: MySQL table schema
            postgres_schema: PostgreSQL table schema
            offset: Batch offset
            limit: Batch size limit
            
        Returns:
            Number of records processed in this batch
        """
        try:
            # Read data from MySQL
            mysql_data = self._read_mysql_batch(mysql_table, mysql_schema, offset, limit)
            
            if not mysql_data:
                return 0
            
            # Debug logging
            self.logger.log_info(f"DEBUG: Read {len(mysql_data)} records from MySQL table '{mysql_table}'")
            if mysql_data:
                self.logger.log_info(f"DEBUG: First MySQL record: {mysql_data[0]}")
            
            # Transform data for PostgreSQL
            postgres_data = self._transform_data(
                mysql_data, mysql_schema, postgres_schema
            )
            
            # Debug logging
            self.logger.log_info(f"DEBUG: Transformed to {len(postgres_data)} PostgreSQL records")
            if postgres_data:
                self.logger.log_info(f"DEBUG: First PostgreSQL record: {postgres_data[0]}")
            
            # Write data to PostgreSQL
            records_written = self._write_postgres_batch(
                postgres_table, postgres_schema, postgres_data
            )
            
            return records_written
            
        except Exception as e:
            self.logger.log_error(f"batch_migration_{mysql_table}_{offset}", e)
            raise
    
    def _read_mysql_batch(self, table_name: str, schema: Dict, 
                         offset: int, limit: int) -> List[Tuple]:
        """
        Read a batch of data from MySQL.
        
        Args:
            table_name: MySQL table name
            schema: MySQL table schema
            offset: Record offset
            limit: Number of records to read
            
        Returns:
            List of record tuples
        """
        try:
            with self.mysql_engine.connect() as conn:
                # Build column list
                columns = ', '.join(f'`{col["name"]}`' for col in schema['columns'])
                
                # Execute query
                query = text(f"""
                    SELECT {columns} 
                    FROM `{table_name}` 
                    ORDER BY (SELECT NULL)
                    LIMIT :limit OFFSET :offset
                """)
                
                result = conn.execute(query, {'limit': limit, 'offset': offset})
                return result.fetchall()
                
        except Exception as e:
            self.logger.log_error(f"mysql_read_{table_name}_{offset}", e)
            raise
    
    def _transform_data(self, mysql_data: List[Tuple], mysql_schema: Dict,
                       postgres_schema: Dict) -> List[Tuple]:
        """
        Transform MySQL data for PostgreSQL compatibility.
        
        Args:
            mysql_data: Raw MySQL data
            mysql_schema: MySQL table schema
            postgres_schema: PostgreSQL table schema
            
        Returns:
            Transformed data for PostgreSQL
        """
        transformed_data = []
        
        # Create mapping from MySQL column names to PostgreSQL column names
        mysql_columns = mysql_schema['column_names']
        postgres_columns = postgres_schema['column_names']
        
        # Debug logging
        self.logger.log_info(f"DEBUG: MySQL columns: {mysql_columns}")
        self.logger.log_info(f"DEBUG: PostgreSQL columns: {postgres_columns}")
        
        # Create a mapping for columns that exist in both schemas (case-insensitive)
        column_mapping = []
        for mysql_idx, mysql_col_name in enumerate(mysql_columns):
            # Try exact match first
            if mysql_col_name in postgres_columns:
                postgres_idx = postgres_columns.index(mysql_col_name)
                column_mapping.append((mysql_idx, postgres_idx))
                self.logger.log_info(f"DEBUG: Exact match - MySQL '{mysql_col_name}' -> PostgreSQL '{postgres_columns[postgres_idx]}'")
            else:
                # Try case-insensitive match
                for postgres_idx, postgres_col_name in enumerate(postgres_columns):
                    if mysql_col_name.lower() == postgres_col_name.lower():
                        column_mapping.append((mysql_idx, postgres_idx))
                        self.logger.log_info(f"DEBUG: Case-insensitive match - MySQL '{mysql_col_name}' -> PostgreSQL '{postgres_col_name}'")
                        break
        
        self.logger.log_info(f"DEBUG: Column mapping: {column_mapping}")
        
        for record in mysql_data:
            transformed_record = [None] * len(postgres_columns)
            
            # Map values based on column name matching
            for mysql_idx, postgres_idx in column_mapping:
                value = record[mysql_idx]
                mysql_col = mysql_schema['columns'][mysql_idx]
                postgres_col = postgres_schema['columns'][postgres_idx]
                
                # Transform value based on type
                transformed_value = self._transform_value(value, mysql_col, postgres_col)
                transformed_record[postgres_idx] = transformed_value
            
            transformed_data.append(tuple(transformed_record))
        
        return transformed_data
    
    def _transform_value(self, value: Any, mysql_col: Optional[Dict], 
                        postgres_col: Optional[Dict]) -> Any:
        """
        Transform a single value for PostgreSQL compatibility.
        
        Args:
            value: Original value from MySQL
            mysql_col: MySQL column information
            postgres_col: PostgreSQL column information
            
        Returns:
            Transformed value
        """
        if value is None:
            return None
        
        # Handle specific type transformations
        if mysql_col and postgres_col:
            mysql_type = mysql_col['type'].lower()
            postgres_type = postgres_col['type'].lower()
            
            # Boolean transformations
            if 'boolean' in postgres_type or 'bool' in postgres_type:
                if isinstance(value, (int, str)):
                    return bool(int(value)) if str(value).isdigit() else bool(value)
            
            # Date/time transformations
            if 'timestamp' in postgres_type or 'datetime' in postgres_type:
                if isinstance(value, str):
                    # Handle string datetime formats
                    return value
            
            # JSON transformations
            if 'json' in postgres_type and isinstance(value, str):
                return value
            
            # Array transformations - convert JSON arrays to PostgreSQL arrays
            if ('array' in postgres_type or '[]' in postgres_type) and isinstance(value, str):
                try:
                    # Try to parse as JSON array
                    if value.startswith('[') and value.endswith(']'):
                        json_array = json.loads(value)
                        if isinstance(json_array, list):
                            # Convert JSON array ["item1","item2"] to PostgreSQL array {"item1","item2"}
                            if json_array:  # Only convert non-empty arrays
                                # Escape any quotes in array elements and format for PostgreSQL
                                escaped_elements = []
                                for item in json_array:
                                    if isinstance(item, str):
                                        # Escape quotes and backslashes
                                        escaped_item = str(item).replace('\\', '\\\\').replace('"', '\\"')
                                        escaped_elements.append(f'"{escaped_item}"')
                                    else:
                                        escaped_elements.append(str(item))
                                return '{' + ','.join(escaped_elements) + '}'
                            else:
                                return '{}'  # Empty array
                except (json.JSONDecodeError, ValueError):
                    # If not valid JSON, treat as string
                    pass
            
            # Numeric transformations
            if 'numeric' in postgres_type or 'decimal' in postgres_type:
                if isinstance(value, str) and value.replace('.', '').replace('-', '').isdigit():
                    return float(value)
        
        return value
    
    def _write_postgres_batch(self, table_name: str, schema: Dict, 
                             data: List[Tuple]) -> int:
        """
        Write a batch of data to PostgreSQL.
        
        Args:
            table_name: PostgreSQL table name
            schema: PostgreSQL table schema
            data: Data to write
            
        Returns:
            Number of records written
        """
        if not data:
            return 0
        
        try:
            # Use raw psycopg connection for better performance
            conn = psycopg.connect(
                host=self.postgres_config['host'],
                port=self.postgres_config['port'],
                dbname=self.postgres_config['database'],
                user=self.postgres_config['username'],
                password=self.postgres_config['password']
            )
            
            with conn:
                with conn.cursor() as cursor:
                    # Build INSERT statement
                    columns = ', '.join(f'"{col["name"]}"' for col in schema['columns'])
                    placeholders = ', '.join(['%s'] * len(schema['columns']))
                    
                    insert_sql = f"""
                        INSERT INTO "{table_name}" ({columns}) 
                        VALUES ({placeholders})
                    """
                    
                    # Execute batch insert using executemany
                    cursor.executemany(insert_sql, data)
                    
            conn.close()
            return len(data)
            
        except Exception as e:
            self.logger.log_error(f"postgres_write_{table_name}", e)
            raise
    
    def get_table_record_count(self, table_name: str, database: str = 'mysql') -> int:
        """
        Get record count for a table.
        
        Args:
            table_name: Table name
            database: Database type ('mysql' or 'postgres')
            
        Returns:
            Number of records in the table
        """
        try:
            engine = self.mysql_engine if database == 'mysql' else self.postgres_engine
            
            with engine.connect() as conn:
                if database == 'mysql':
                    query = text(f"SELECT COUNT(*) FROM `{table_name}`")
                else:
                    query = text(f'SELECT COUNT(*) FROM "{table_name}"')
                
                result = conn.execute(query)
                return result.scalar()
                
        except Exception as e:
            self.logger.log_error(f"record_count_{database}_{table_name}", e)
            return 0
    
    def clear_cache(self) -> None:
        """Clear schema caches."""
        self.postgres_schemas_cache.clear()
        self.mysql_schemas_cache.clear()
        self.logger.log_info("Schema caches cleared")
    
    def __del__(self):
        """Cleanup resources."""
        try:
            if self.mysql_engine:
                self.mysql_engine.dispose()
            if self.postgres_engine:
                self.postgres_engine.dispose()
        except:
            pass
