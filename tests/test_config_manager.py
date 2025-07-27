"""
Test Configuration Manager

Unit tests for the configuration management module.
"""

import os
import pytest
from pathlib import Path
from unittest.mock import patch, mock_open
import tempfile

# Add src to path for testing
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from src.config_manager import ConfigManager


class TestConfigManager:
    """Test cases for ConfigManager class."""
    
    def setup_method(self):
        """Setup test fixtures."""
        self.config_manager = ConfigManager()
    
    @patch.dict(os.environ, {
        'MYSQL_HOST': 'test-mysql-host',
        'MYSQL_PORT': '3306',
        'MYSQL_DATABASE': 'test_db',
        'MYSQL_USERNAME': 'test_user',
        'MYSQL_PASSWORD': 'test_pass',
        'POSTGRES_HOST': 'test-postgres-host',
        'POSTGRES_PORT': '5432',
        'POSTGRES_DATABASE': 'test_pg_db',
        'POSTGRES_USERNAME': 'test_pg_user',
        'POSTGRES_PASSWORD': 'test_pg_pass',
        'BATCH_SIZE': '50',
        'MAX_WORKERS': '4',
        'LOG_LEVEL': 'DEBUG'
    })
    def test_load_config_from_env_success(self):
        """Test successful configuration loading from environment."""
        config = self.config_manager.load_config_from_env('nonexistent.env')
        
        # Test MySQL config
        assert config['mysql']['host'] == 'test-mysql-host'
        assert config['mysql']['port'] == 3306
        assert config['mysql']['database'] == 'test_db'
        assert config['mysql']['username'] == 'test_user'
        assert config['mysql']['password'] == 'test_pass'
        
        # Test PostgreSQL config
        assert config['postgres']['host'] == 'test-postgres-host'
        assert config['postgres']['port'] == 5432
        assert config['postgres']['database'] == 'test_pg_db'
        assert config['postgres']['username'] == 'test_pg_user'
        assert config['postgres']['password'] == 'test_pg_pass'
        
        # Test migration config
        assert config['migration']['batch_size'] == 50
        assert config['migration']['max_workers'] == 4
        assert config['migration']['log_level'] == 'DEBUG'
    
    def test_load_config_missing_required_env(self):
        """Test configuration loading with missing required environment variables."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ValueError, match="Required environment variable MYSQL_HOST is not set"):
                self.config_manager.load_config_from_env('nonexistent.env')
    
    @patch.dict(os.environ, {
        'MYSQL_HOST': 'localhost',
        'MYSQL_PORT': '3306',
        'MYSQL_DATABASE': 'test_db',
        'MYSQL_USERNAME': 'test_user',
        'MYSQL_PASSWORD': 'test_pass',
        'POSTGRES_HOST': 'localhost',
        'POSTGRES_PORT': '5432',
        'POSTGRES_DATABASE': 'test_pg_db',
        'POSTGRES_USERNAME': 'test_pg_user',
        'POSTGRES_PASSWORD': 'test_pg_pass',
        'BATCH_SIZE': '0'  # Invalid batch size
    })
    def test_validate_config_invalid_batch_size(self):
        """Test configuration validation with invalid batch size."""
        with pytest.raises(ValueError, match="Batch size must be greater than 0"):
            self.config_manager.load_config_from_env('nonexistent.env')
    
    @patch.dict(os.environ, {
        'MYSQL_HOST': 'localhost',
        'MYSQL_PORT': '3306',
        'MYSQL_DATABASE': 'test_db',
        'MYSQL_USERNAME': 'test_user',
        'MYSQL_PASSWORD': 'test_pass',
        'POSTGRES_HOST': 'localhost',
        'POSTGRES_PORT': '5432',
        'POSTGRES_DATABASE': 'test_pg_db',
        'POSTGRES_USERNAME': 'test_pg_user',
        'POSTGRES_PASSWORD': 'test_pg_pass',
        'LOG_LEVEL': 'INVALID'  # Invalid log level
    })
    def test_validate_config_invalid_log_level(self):
        """Test configuration validation with invalid log level."""
        with pytest.raises(ValueError, match="Invalid log level INVALID"):
            self.config_manager.load_config_from_env('nonexistent.env')
    
    @patch.dict(os.environ, {
        'MYSQL_HOST': 'localhost',
        'MYSQL_PORT': '3306',
        'MYSQL_DATABASE': 'test_db',
        'MYSQL_USERNAME': 'test_user',
        'MYSQL_PASSWORD': 'test_pass',
        'POSTGRES_HOST': 'localhost',
        'POSTGRES_PORT': '5432',
        'POSTGRES_DATABASE': 'test_pg_db',
        'POSTGRES_USERNAME': 'test_pg_user',
        'POSTGRES_PASSWORD': 'test_pg_pass',
        'EXCLUDE_TABLES': 'table1,table2',
        'INCLUDE_TABLES': 'table1,table3'  # Overlapping with exclude
    })
    def test_validate_config_conflicting_table_filters(self):
        """Test configuration validation with conflicting table filters."""
        with pytest.raises(ValueError, match="Tables cannot be both included and excluded"):
            self.config_manager.load_config_from_env('nonexistent.env')
    
    @patch.dict(os.environ, {
        'MYSQL_HOST': 'localhost',
        'MYSQL_PORT': '3306',
        'MYSQL_DATABASE': 'test_db',
        'MYSQL_USERNAME': 'test_user',
        'MYSQL_PASSWORD': 'test_pass',
        'POSTGRES_HOST': 'localhost',
        'POSTGRES_PORT': '5432',
        'POSTGRES_DATABASE': 'test_pg_db',
        'POSTGRES_USERNAME': 'test_pg_user',
        'POSTGRES_PASSWORD': 'test_pg_pass'
    })
    def test_get_connection_strings(self):
        """Test connection string generation."""
        config = self.config_manager.load_config_from_env('nonexistent.env')
        
        mysql_conn_str = self.config_manager.get_mysql_connection_string(config)
        postgres_conn_str = self.config_manager.get_postgres_connection_string(config)
        
        assert 'mysql+mysqlconnector://' in mysql_conn_str
        assert 'test_user:test_pass@localhost:3306/test_db' in mysql_conn_str
        
        assert 'postgresql+psycopg://' in postgres_conn_str
        assert 'test_pg_user:test_pg_pass@localhost:5432/test_pg_db' in postgres_conn_str
    
    def test_create_example_env_file(self):
        """Test example environment file creation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            example_path = Path(temp_dir) / '.env.example'
            
            # Redirect print output for testing
            with patch('builtins.print'):
                self.config_manager.create_example_env_file(str(example_path))
            
            assert example_path.exists()
            
            content = example_path.read_text()
            assert 'MYSQL_HOST=' in content
            assert 'POSTGRES_HOST=' in content
            assert 'BATCH_SIZE=' in content
    
    def test_default_values(self):
        """Test default configuration values."""
        with patch.dict(os.environ, {
            'MYSQL_HOST': 'localhost',
            'MYSQL_DATABASE': 'test_db',
            'MYSQL_USERNAME': 'test_user',
            'MYSQL_PASSWORD': 'test_pass',
            'POSTGRES_HOST': 'localhost',
            'POSTGRES_DATABASE': 'test_pg_db',
            'POSTGRES_USERNAME': 'test_pg_user',
            'POSTGRES_PASSWORD': 'test_pg_pass'
            # Let other values use defaults
        }):
            config = self.config_manager.load_config_from_env('nonexistent.env')
            
            # Test defaults
            assert config['mysql']['port'] == 3306
            assert config['postgres']['port'] == 5432
            assert config['migration']['batch_size'] == 100
            assert config['migration']['max_workers'] == 2
            assert config['migration']['log_level'] == 'INFO'
            assert config['migration']['fail_on_missing_tables'] is True
            assert config['migration']['continue_on_error'] is False


if __name__ == '__main__':
    pytest.main([__file__])
