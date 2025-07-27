"""
Setup Wizard Module

Interactive setup wizard for configuring the migration tool,
including database connections and migration parameters.
"""

import os
import sys
from pathlib import Path
from typing import Dict, Any, Optional
import getpass

from .config_manager import ConfigManager
from .utils.logger import setup_logging


class SetupWizard:
    """Interactive setup wizard for the migration tool."""
    
    def __init__(self):
        self.config_manager = ConfigManager()
        self.config = {}
        self.existing_config = self._load_existing_config()
        
    def _load_existing_config(self) -> Dict[str, Any]:
        """Load existing configuration from .env file if it exists."""
        env_file = Path('.env')
        if not env_file.exists():
            return {}
        
        try:
            # Load existing config using ConfigManager
            existing = self.config_manager.load_config_from_env('.env')
            print(f"✅ Found existing .env configuration")
            return existing
        except Exception as e:
            print(f"⚠️  Warning: Could not load existing .env file: {e}")
            return {}
        
    def run(self) -> None:
        """Run the interactive setup wizard."""
        print("🛠️  MySQL to PostgreSQL Migration Tool - Setup Wizard")
        print("=" * 60)
        print("This wizard will help you configure the migration tool.")
        print("Press Ctrl+C at any time to cancel.\n")
        
        try:
            # Welcome and overview
            self._show_overview()
            
            # Configure MySQL connection
            print("\n📊 MySQL Database Configuration")
            print("-" * 40)
            mysql_config = self._configure_mysql()
            
            # Configure PostgreSQL connection
            print("\n🐘 PostgreSQL Database Configuration")
            print("-" * 40)
            postgres_config = self._configure_postgres()
            
            # Configure migration settings
            print("\n⚙️  Migration Settings")
            print("-" * 40)
            migration_config = self._configure_migration()
            
            # Test connections
            print("\n🔍 Connection Testing")
            print("-" * 40)
            self._test_connections_wizard(mysql_config, postgres_config)
            
            # Save configuration
            print("\n💾 Saving Configuration")
            print("-" * 40)
            self._save_configuration(mysql_config, postgres_config, migration_config)
            
            # Show next steps
            self._show_next_steps()
            
        except KeyboardInterrupt:
            print("\n\n⚠️  Setup cancelled by user")
            sys.exit(1)
        except Exception as e:
            print(f"\n❌ Setup failed: {e}")
            sys.exit(1)
    
    def _show_overview(self) -> None:
        """Show setup overview."""
        print("This setup will configure:")
        print("  • MySQL database connection")
        print("  • PostgreSQL database connection")
        print("  • Migration parameters")
        print("  • Test database connectivity")
        print("  • Create environment configuration file")
        
        if not self._confirm("Continue with setup?"):
            print("Setup cancelled.")
            sys.exit(0)
    
    def _configure_mysql(self) -> Dict[str, Any]:
        """Configure MySQL connection settings."""
        existing_mysql = self.existing_config.get('mysql', {})
        
        if existing_mysql and self._confirm(f"Use existing MySQL configuration (host: {existing_mysql.get('host', 'N/A')})?"):
            print("✅ Using existing MySQL configuration")
            return existing_mysql
            
        print("Enter MySQL database connection details:")
        
        mysql_config = {}
        
        # Host
        mysql_config['host'] = self._get_input(
            "MySQL Host", 
            default=existing_mysql.get('host', "localhost"),
            validator=self._validate_host
        )
        
        # Port
        mysql_config['port'] = self._get_input(
            "MySQL Port", 
            default=str(existing_mysql.get('port', 3306)),
            validator=self._validate_port,
            convert_type=int
        )
        
        # Database
        mysql_config['database'] = self._get_input(
            "MySQL Database Name",
            default=existing_mysql.get('database', ""),
            required=True,
            validator=self._validate_database_name
        )
        
        # Username
        mysql_config['username'] = self._get_input(
            "MySQL Username",
            default=existing_mysql.get('username', ""),
            required=True
        )
        
        # Password (always ask for password for security)
        mysql_config['password'] = self._get_password("MySQL Password")
        
        # Optional settings
        mysql_config['charset'] = self._get_input(
            "MySQL Charset",
            default=existing_mysql.get('charset', "utf8mb4")
        )
        
        return mysql_config
    
    def _configure_postgres(self) -> Dict[str, Any]:
        """Configure PostgreSQL connection settings."""
        existing_postgres = self.existing_config.get('postgres', {})
        
        if existing_postgres and self._confirm(f"Use existing PostgreSQL configuration (host: {existing_postgres.get('host', 'N/A')})?"):
            print("✅ Using existing PostgreSQL configuration")
            return existing_postgres
            
        print("Enter PostgreSQL database connection details:")
        
        postgres_config = {}
        
        # Host
        postgres_config['host'] = self._get_input(
            "PostgreSQL Host",
            default=existing_postgres.get('host', "localhost"),
            validator=self._validate_host
        )
        
        # Port
        postgres_config['port'] = self._get_input(
            "PostgreSQL Port",
            default=str(existing_postgres.get('port', 5432)),
            validator=self._validate_port,
            convert_type=int
        )
        
        # Database
        postgres_config['database'] = self._get_input(
            "PostgreSQL Database Name",
            default=existing_postgres.get('database', ""),
            required=True,
            validator=self._validate_database_name
        )
        
        # Username
        postgres_config['username'] = self._get_input(
            "PostgreSQL Username",
            default=existing_postgres.get('username', ""),
            required=True
        )
        
        # Password (always ask for password for security)
        postgres_config['password'] = self._get_password("PostgreSQL Password")
        
        # Optional settings
        postgres_config['sslmode'] = self._get_input(
            "PostgreSQL SSL Mode",
            default=existing_postgres.get('sslmode', "prefer"),
            validator=lambda x: x in ['disable', 'allow', 'prefer', 'require', 'verify-ca', 'verify-full']
        )
        
        return postgres_config
    
    def _configure_migration(self) -> Dict[str, Any]:
        """Configure migration settings."""
        existing_migration = self.existing_config.get('migration', {})
        
        if existing_migration and self._confirm(f"Use existing migration settings (batch size: {existing_migration.get('batch_size', 'N/A')})?"):
            print("✅ Using existing migration configuration")
            return existing_migration
            
        print("Configure migration parameters:")
        
        migration_config = {}
        
        # Batch size
        migration_config['batch_size'] = self._get_input(
            "Batch Size (records per batch)",
            default=str(existing_migration.get('batch_size', 100)),
            validator=lambda x: 1 <= int(x) <= 10000,
            convert_type=int
        )
        
        # Max workers
        migration_config['max_workers'] = self._get_input(
            "Maximum Worker Threads",
            default=str(existing_migration.get('max_workers', 2)),
            validator=lambda x: 1 <= int(x) <= 10,
            convert_type=int
        )
        
        # Log level
        migration_config['log_level'] = self._get_input(
            "Log Level",
            default=existing_migration.get('log_level', "INFO"),
            validator=lambda x: x.upper() in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        ).upper()
        
        # Table filters
        existing_exclude = existing_migration.get('exclude_tables', [])
        exclude_tables = self._get_input(
            "Tables to Exclude (comma-separated, optional)",
            default=','.join(existing_exclude) if existing_exclude else "",
            required=False
        )
        if exclude_tables:
            migration_config['exclude_tables'] = [
                t.strip() for t in exclude_tables.split(',') if t.strip()
            ]
        else:
            migration_config['exclude_tables'] = []
        
        existing_include = existing_migration.get('include_tables', [])
        include_tables = self._get_input(
            "Tables to Include (comma-separated, leave empty for all)",
            default=','.join(existing_include) if existing_include else "",
            required=False
        )
        if include_tables:
            migration_config['include_tables'] = [
                t.strip() for t in include_tables.split(',') if t.strip()
            ]
        else:
            migration_config['include_tables'] = []
        
        # Error handling
        migration_config['fail_on_missing_tables'] = self._confirm(
            "Fail if tables are missing in PostgreSQL?",
            default=True
        )
        
        migration_config['continue_on_error'] = self._confirm(
            "Continue migration if individual tables fail?",
            default=False
        )
        
        # Optional features
        migration_config['enable_progress_bar'] = self._confirm(
            "Enable progress bar?",
            default=True
        )

        migration_config['ignore_generated_columns'] = self._confirm(
            "Ignore generated/computed columns in PostgreSQL?",
            default=existing_migration.get('ignore_generated_columns', True)
        )

        migration_config['disable_foreign_keys'] = self._confirm(
            "Temporarily disable foreign key constraints during migration?",
            default=existing_migration.get('disable_foreign_keys', True)
        )

        migration_config['truncate_target_tables'] = self._confirm(
            "Truncate target PostgreSQL tables before migration (clears existing data)?",
            default=existing_migration.get('truncate_target_tables', False)
        )
        
        migration_config['backup_before_migration'] = self._confirm(
            "Recommend backup before migration?",
            default=True
        )
        
        return migration_config
    
    def _test_connections_wizard(self, mysql_config: Dict[str, Any], 
                               postgres_config: Dict[str, Any]) -> None:
        """Test database connections during setup."""
        if not self._confirm("Test database connections now?"):
            print("⚠️  Skipping connection test. You can test later with --test-connections")
            return
        
        print("Testing connections...")
        
        # Test MySQL
        try:
            self._test_mysql_connection(mysql_config)
            print("✅ MySQL connection successful")
        except Exception as e:
            print(f"❌ MySQL connection failed: {e}")
            if not self._confirm("Continue setup despite MySQL connection failure?"):
                raise Exception("Setup cancelled due to MySQL connection failure")
        
        # Test PostgreSQL
        try:
            self._test_postgres_connection(postgres_config)
            print("✅ PostgreSQL connection successful")
        except Exception as e:
            print(f"❌ PostgreSQL connection failed: {e}")
            if not self._confirm("Continue setup despite PostgreSQL connection failure?"):
                raise Exception("Setup cancelled due to PostgreSQL connection failure")
    
    def _test_mysql_connection(self, config: Dict[str, Any]) -> None:
        """Test MySQL connection."""
        try:
            import mysql.connector
            
            connection = mysql.connector.connect(
                host=config['host'],
                port=config['port'],
                database=config['database'],
                user=config['username'],
                password=config['password'],
                charset=config.get('charset', 'utf8mb4'),
                connection_timeout=10
            )
            
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            connection.close()
            
        except ImportError:
            raise Exception("MySQL connector not installed. Run: pip install mysql-connector-python")
        except Exception as e:
            raise Exception(f"MySQL connection failed: {str(e)}")
    
    def _test_postgres_connection(self, config: Dict[str, Any]) -> None:
        """Test PostgreSQL connection."""
        try:
            import psycopg
            
            connection = psycopg.connect(
                host=config['host'],
                port=config['port'],
                dbname=config['database'],
                user=config['username'],
                password=config['password'],
                sslmode=config.get('sslmode', 'prefer'),
                connect_timeout=10
            )
            
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            connection.close()
            
        except ImportError:
            raise Exception("PostgreSQL adapter not installed. Run: pip install 'psycopg[binary]'")
        except Exception as e:
            raise Exception(f"PostgreSQL connection failed: {str(e)}")
    
    def _save_configuration(self, mysql_config: Dict[str, Any], 
                          postgres_config: Dict[str, Any],
                          migration_config: Dict[str, Any]) -> None:
        """Save configuration to .env file."""
        env_content = self._generate_env_content(mysql_config, postgres_config, migration_config)
        
        env_file_path = Path('.env')
        
        # Backup existing .env if it exists
        if env_file_path.exists():
            backup_path = Path(f'.env.backup.{int(os.path.getmtime(env_file_path))}')
            env_file_path.rename(backup_path)
            print(f"📁 Existing .env backed up to {backup_path}")
        
        # Write new configuration
        with open(env_file_path, 'w') as f:
            f.write(env_content)
        
        print(f"✅ Configuration saved to {env_file_path}")
        print("🔒 Remember to keep your .env file secure and don't commit it to version control!")
    
    def _generate_env_content(self, mysql_config: Dict[str, Any],
                            postgres_config: Dict[str, Any],
                            migration_config: Dict[str, Any]) -> str:
        """Generate .env file content."""
        content = []
        
        # Header
        content.append("# MySQL to PostgreSQL Migration Tool Configuration")
        content.append(f"# Generated on {os.popen('date').read().strip()}")
        content.append("")
        
        # MySQL configuration
        content.append("# MySQL Connection Configuration")
        content.append(f"MYSQL_HOST={mysql_config['host']}")
        content.append(f"MYSQL_PORT={mysql_config['port']}")
        content.append(f"MYSQL_DATABASE={mysql_config['database']}")
        content.append(f"MYSQL_USERNAME={mysql_config['username']}")
        content.append(f"MYSQL_PASSWORD={mysql_config['password']}")
        if 'charset' in mysql_config:
            content.append(f"MYSQL_CHARSET={mysql_config['charset']}")
        content.append("")
        
        # PostgreSQL configuration
        content.append("# PostgreSQL Connection Configuration")
        content.append(f"POSTGRES_HOST={postgres_config['host']}")
        content.append(f"POSTGRES_PORT={postgres_config['port']}")
        content.append(f"POSTGRES_DATABASE={postgres_config['database']}")
        content.append(f"POSTGRES_USERNAME={postgres_config['username']}")
        content.append(f"POSTGRES_PASSWORD={postgres_config['password']}")
        if 'sslmode' in postgres_config:
            content.append(f"POSTGRES_SSLMODE={postgres_config['sslmode']}")
        content.append("")
        
        # Migration settings
        content.append("# Migration Settings")
        content.append(f"BATCH_SIZE={migration_config['batch_size']}")
        content.append(f"MAX_WORKERS={migration_config['max_workers']}")
        content.append(f"LOG_LEVEL={migration_config['log_level']}")
        
        exclude_tables = ','.join(migration_config.get('exclude_tables', []))
        include_tables = ','.join(migration_config.get('include_tables', []))
        
        content.append(f"EXCLUDE_TABLES={exclude_tables}")
        content.append(f"INCLUDE_TABLES={include_tables}")
        content.append(f"FAIL_ON_MISSING_TABLES={str(migration_config['fail_on_missing_tables']).lower()}")
        content.append(f"CONTINUE_ON_ERROR={str(migration_config['continue_on_error']).lower()}")
        content.append("")
        
        # Optional settings
        content.append("# Optional Settings")
        content.append(f"ENABLE_PROGRESS_BAR={str(migration_config['enable_progress_bar']).lower()}")
        content.append(f"IGNORE_GENERATED_COLUMNS={str(migration_config['ignore_generated_columns']).lower()}")
        content.append(f"DISABLE_FOREIGN_KEYS={str(migration_config['disable_foreign_keys']).lower()}")
        content.append(f"TRUNCATE_TARGET_TABLES={str(migration_config['truncate_target_tables']).lower()}")
        content.append(f"BACKUP_BEFORE_MIGRATION={str(migration_config['backup_before_migration']).lower()}")
        content.append("CONNECTION_TIMEOUT=30")
        content.append("QUERY_TIMEOUT=300")
        
        return '\n'.join(content)
    
    def _show_next_steps(self) -> None:
        """Show next steps after setup completion."""
        print("\n🎉 Setup completed successfully!")
        print("\n📝 Next Steps:")
        print("  1. Review your configuration: python cli.py --config")
        print("  2. Test connections: python cli.py --test-connections")
        print("  3. Run a dry-run: python cli.py --dry-run")
        print("  4. Execute migration: python cli.py --migrate")
        print("  5. Validate results: python cli.py --validate")
        print("\n💡 Tips:")
        print("  • Always backup your PostgreSQL database before migration")
        print("  • Start with a dry-run to identify potential issues")
        print("  • Monitor the logs during migration")
        print("  • Run validation after migration to ensure data integrity")
        print("\n📚 For more help: python cli.py --help")
    
    def _get_input(self, prompt: str, default: Optional[str] = None,
                  required: bool = True, validator=None, convert_type=None) -> Any:
        """Get user input with validation."""
        while True:
            if default:
                display_prompt = f"{prompt} [{default}]: "
            else:
                display_prompt = f"{prompt}: "
            
            try:
                value = input(display_prompt).strip()
                
                # Use default if no input provided
                if not value and default is not None:
                    value = default
                
                # Check if required
                if required and not value:
                    print("❌ This field is required. Please enter a value.")
                    continue
                
                # Skip validation if empty and not required
                if not value and not required:
                    return None if convert_type is None else convert_type()
                
                # Convert type if specified
                if convert_type and value:
                    try:
                        value = convert_type(value)
                    except ValueError:
                        print(f"❌ Invalid value. Please enter a valid {convert_type.__name__}.")
                        continue
                
                # Validate if validator provided
                if validator and value:
                    try:
                        if not validator(value):
                            print("❌ Invalid value. Please try again.")
                            continue
                    except Exception as e:
                        print(f"❌ Validation error: {e}")
                        continue
                
                return value
                
            except KeyboardInterrupt:
                raise
            except Exception as e:
                print(f"❌ Input error: {e}")
                continue
    
    def _get_password(self, prompt: str) -> str:
        """Get password input securely."""
        while True:
            try:
                password = getpass.getpass(f"{prompt}: ")
                if not password:
                    print("❌ Password is required. Please enter a password.")
                    continue
                return password
            except KeyboardInterrupt:
                raise
            except Exception as e:
                print(f"❌ Password input error: {e}")
                continue
    
    def _confirm(self, prompt: str, default: bool = False) -> bool:
        """Get yes/no confirmation."""
        default_text = "Y/n" if default else "y/N"
        
        while True:
            try:
                response = input(f"{prompt} [{default_text}]: ").strip().lower()
                
                if not response:
                    return default
                
                if response in ['y', 'yes', 'true', '1']:
                    return True
                elif response in ['n', 'no', 'false', '0']:
                    return False
                else:
                    print("❌ Please enter 'y' for yes or 'n' for no.")
                    continue
                    
            except KeyboardInterrupt:
                raise
            except Exception as e:
                print(f"❌ Input error: {e}")
                continue
    
    def _validate_host(self, host: str) -> bool:
        """Validate host format."""
        if not host:
            return False
        # Basic validation - could be enhanced with IP/hostname validation
        return len(host) > 0 and not host.isspace()
    
    def _validate_port(self, port: str) -> bool:
        """Validate port number."""
        try:
            port_num = int(port)
            return 1 <= port_num <= 65535
        except ValueError:
            return False
    
    def _validate_database_name(self, name: str) -> bool:
        """Validate database name."""
        if not name:
            return False
        # Basic validation - no spaces, reasonable length
        return len(name) > 0 and ' ' not in name and len(name) <= 64
