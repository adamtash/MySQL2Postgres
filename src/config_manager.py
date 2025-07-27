"""
Configuration Management Module

Handles loading and validation of configuration from environment variables
and provides structured configuration dictionaries for the migration tool.
"""

import os
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from dotenv import load_dotenv


class ConfigManager:
    """Manages configuration loading and validation."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def load_config_from_env(self, env_file: str = '.env') -> Dict[str, Any]:
        """
        Load configuration from environment file.
        
        Args:
            env_file: Path to environment file
            
        Returns:
            Dictionary containing structured configuration
            
        Raises:
            ValueError: If required configuration is missing
        """
        # Load environment file if it exists
        env_path = Path(env_file)
        if env_path.exists():
            load_dotenv(env_path)
            self.logger.info(f"Loaded configuration from {env_file}")
        else:
            self.logger.warning(f"Environment file {env_file} not found, using system environment")
        
        # Build configuration dictionary
        config = {
            'mysql': self._load_mysql_config(),
            'postgres': self._load_postgres_config(),
            'migration': self._load_migration_config()
        }
        
        # Validate configuration
        self._validate_config(config)
        
        return config
    
    def _load_mysql_config(self) -> Dict[str, Any]:
        """Load MySQL connection configuration."""
        return {
            'host': self._get_required_env('MYSQL_HOST'),
            'port': int(self._get_env('MYSQL_PORT', '3306')),
            'database': self._get_required_env('MYSQL_DATABASE'),
            'username': self._get_required_env('MYSQL_USERNAME'),
            'password': self._get_required_env('MYSQL_PASSWORD'),
            'charset': self._get_env('MYSQL_CHARSET', 'utf8mb4'),
            'autocommit': self._get_env('MYSQL_AUTOCOMMIT', 'False').lower() == 'true'
        }
    
    def _load_postgres_config(self) -> Dict[str, Any]:
        """Load PostgreSQL connection configuration."""
        return {
            'host': self._get_required_env('POSTGRES_HOST'),
            'port': int(self._get_env('POSTGRES_PORT', '5432')),
            'database': self._get_required_env('POSTGRES_DATABASE'),
            'username': self._get_required_env('POSTGRES_USERNAME'),
            'password': self._get_required_env('POSTGRES_PASSWORD'),
            'sslmode': self._get_env('POSTGRES_SSLMODE', 'prefer'),
            'application_name': self._get_env('POSTGRES_APP_NAME', 'mysql2postgres')
        }
    
    def _load_migration_config(self) -> Dict[str, Any]:
        """Load migration-specific configuration."""
        exclude_tables = self._get_env('EXCLUDE_TABLES', '')
        include_tables = self._get_env('INCLUDE_TABLES', '')
        
        return {
            'batch_size': int(self._get_env('BATCH_SIZE', '100')),
            'max_workers': int(self._get_env('MAX_WORKERS', '2')),
            'log_level': self._get_env('LOG_LEVEL', 'INFO'),
            'exclude_tables': [t.strip() for t in exclude_tables.split(',') if t.strip()],
            'include_tables': [t.strip() for t in include_tables.split(',') if t.strip()],
            'fail_on_missing_tables': self._get_env('FAIL_ON_MISSING_TABLES', 'true').lower() == 'true',
            'continue_on_error': self._get_env('CONTINUE_ON_ERROR', 'false').lower() == 'true',
            'dry_run': self._get_env('DRY_RUN', 'false').lower() == 'true',
            'enable_progress_bar': self._get_env('ENABLE_PROGRESS_BAR', 'true').lower() == 'true',
            'ignore_generated_columns': self._get_env('IGNORE_GENERATED_COLUMNS', 'true').lower() == 'true',
            'disable_foreign_keys': self._get_env('DISABLE_FOREIGN_KEYS', 'true').lower() == 'true',
            'truncate_target_tables': self._get_env('TRUNCATE_TARGET_TABLES', 'false').lower() == 'true',
            'backup_before_migration': self._get_env('BACKUP_BEFORE_MIGRATION', 'true').lower() == 'true',
            'connection_timeout': int(self._get_env('CONNECTION_TIMEOUT', '30')),
            'query_timeout': int(self._get_env('QUERY_TIMEOUT', '300'))
        }
    
    def _get_env(self, key: str, default: str = '') -> str:
        """Get environment variable with default value."""
        return os.getenv(key, default)
    
    def _get_required_env(self, key: str) -> str:
        """Get required environment variable, raise error if missing."""
        value = os.getenv(key)
        if not value:
            raise ValueError(f"Required environment variable {key} is not set")
        return value
    
    def _validate_config(self, config: Dict[str, Any]) -> None:
        """
        Validate configuration for common issues.
        
        Args:
            config: Configuration dictionary
            
        Raises:
            ValueError: If configuration is invalid
        """
        # Validate batch size
        batch_size = config['migration']['batch_size']
        if batch_size <= 0:
            raise ValueError("Batch size must be greater than 0")
        if batch_size > 10000:
            self.logger.warning("Large batch size may cause memory issues")
        
        # Validate max workers
        max_workers = config['migration']['max_workers']
        if max_workers <= 0:
            raise ValueError("Max workers must be greater than 0")
        if max_workers > 10:
            self.logger.warning("High worker count may overwhelm database connections")
        
        # Validate log level
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        log_level = config['migration']['log_level']
        if log_level not in valid_levels:
            raise ValueError(f"Invalid log level {log_level}. Must be one of: {valid_levels}")
        
        # Validate database ports
        mysql_port = config['mysql']['port']
        postgres_port = config['postgres']['port']
        if not (1 <= mysql_port <= 65535):
            raise ValueError(f"Invalid MySQL port {mysql_port}")
        if not (1 <= postgres_port <= 65535):
            raise ValueError(f"Invalid PostgreSQL port {postgres_port}")
        
        # Check for conflicting table filters
        exclude_tables = set(config['migration']['exclude_tables'])
        include_tables = set(config['migration']['include_tables'])
        
        if include_tables and exclude_tables:
            overlap = include_tables.intersection(exclude_tables)
            if overlap:
                raise ValueError(f"Tables cannot be both included and excluded: {overlap}")
        
        self.logger.info("Configuration validation passed")
    
    def get_mysql_connection_string(self, config: Dict[str, Any]) -> str:
        """Generate MySQL connection string."""
        mysql_config = config['mysql']
        return (
            f"mysql+mysqlconnector://{mysql_config['username']}:{mysql_config['password']}"
            f"@{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"
        )
    
    def get_postgres_connection_string(self, config: Dict[str, Any]) -> str:
        """Generate PostgreSQL connection string."""
        postgres_config = config['postgres']
        return (
            f"postgresql+psycopg://{postgres_config['username']}:{postgres_config['password']}"
            f"@{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"
        )
    
    def create_example_env_file(self, path: str = '.env.example') -> None:
        """Create an example environment file with all required variables."""
        example_content = """# MySQL Connection Configuration
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=your_mysql_database
MYSQL_USERNAME=your_mysql_username
MYSQL_PASSWORD=your_mysql_password

# PostgreSQL Connection Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=your_postgres_database
POSTGRES_USERNAME=your_postgres_username
POSTGRES_PASSWORD=your_postgres_password

# Migration Settings
BATCH_SIZE=100
MAX_WORKERS=2
LOG_LEVEL=INFO
EXCLUDE_TABLES=
INCLUDE_TABLES=
FAIL_ON_MISSING_TABLES=true
CONTINUE_ON_ERROR=false

# Optional Settings
DRY_RUN=false
ENABLE_PROGRESS_BAR=true
BACKUP_BEFORE_MIGRATION=true
CONNECTION_TIMEOUT=30
QUERY_TIMEOUT=300
"""
        with open(path, 'w') as f:
            f.write(example_content)
        
        print(f"Created example environment file: {path}")
        print("Please copy this to .env and update with your actual values.")
