#!/usr/bin/env python3
"""
MySQL to PostgreSQL Migration Tool - CLI Interface

Production-ready command-line tool for migrating data from MySQL 8.0 to PostgreSQL
with comprehensive error handling, progress monitoring, and validation.
"""

import argparse
import sys
import os
from pathlib import Path
from typing import Optional, List

# Add src directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from src.config_manager import ConfigManager
from src.migration_engine import MigrationEngine
from src.validation_engine import ValidationEngine
from src.utils.logger import setup_logging
from src.utils.status_monitor import StatusMonitor
from src.utils.error_collector import ErrorCollector
from src.setup_wizard import SetupWizard


class CLI:
    """Main CLI interface for the migration tool."""
    
    def __init__(self):
        self.config_manager = ConfigManager()
        self.status_monitor = StatusMonitor()
        self.error_collector = ErrorCollector()
        
    def create_parser(self) -> argparse.ArgumentParser:
        """Create and configure the argument parser."""
        parser = argparse.ArgumentParser(
            description="MySQL to PostgreSQL Migration Tool",
            formatter_class=argparse.RawDescriptionHelpFormatter,
            epilog="""
Examples:
  %(prog)s --config                           Display current configuration
  %(prog)s --test-connections                 Test database connectivity
  %(prog)s --migrate --batch-size 500        Execute migration with custom batch size
  %(prog)s --dry-run --log-level DEBUG       Simulate migration with debug logging
  %(prog)s --validate                         Validate migration results
  %(prog)s --setup                           Run setup wizard
            """
        )
        
        # Primary actions (mutually exclusive)
        action_group = parser.add_mutually_exclusive_group(required=True)
        action_group.add_argument(
            '--config', '-c',
            action='store_true',
            help='Display current configuration'
        )
        action_group.add_argument(
            '--test-connections', '-t',
            action='store_true',
            help='Test database connectivity'
        )
        action_group.add_argument(
            '--migrate', '-m',
            action='store_true',
            help='Execute data migration'
        )
        action_group.add_argument(
            '--validate', '-v',
            action='store_true',
            help='Validate migration results'
        )
        action_group.add_argument(
            '--dry-run', '-d',
            action='store_true',
            help='Run migration simulation (no changes)'
        )
        action_group.add_argument(
            '--setup', '-s',
            action='store_true',
            help='Run setup wizard'
        )
        
        # Configuration overrides
        parser.add_argument(
            '--env-file',
            default='.env',
            help='Path to environment file (default: .env)'
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            help='Records per batch (integer)'
        )
        parser.add_argument(
            '--max-workers',
            type=int,
            help='Maximum worker threads (integer)'
        )
        parser.add_argument(
            '--exclude',
            help='Comma-separated table exclusion list'
        )
        parser.add_argument(
            '--include',
            help='Comma-separated table inclusion list'
        )
        parser.add_argument(
            '--log-level',
            choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
            help='Logging level'
        )
        parser.add_argument(
            '--fail-fast',
            action='store_true',
            help='Fail immediately on missing tables'
        )
        parser.add_argument(
            '--continue-on-error',
            action='store_true',
            help='Continue migration despite table failures'
        )
        
        return parser
    
    def setup_configuration(self, args: argparse.Namespace) -> dict:
        """Setup configuration from arguments and environment."""
        try:
            # Load base configuration
            config = self.config_manager.load_config_from_env(args.env_file)
            
            # Apply command-line overrides
            if args.batch_size:
                config['migration']['batch_size'] = args.batch_size
            if args.max_workers:
                config['migration']['max_workers'] = args.max_workers
            if args.exclude:
                config['migration']['exclude_tables'] = [
                    table.strip() for table in args.exclude.split(',')
                ]
            if args.include:
                config['migration']['include_tables'] = [
                    table.strip() for table in args.include.split(',')
                ]
            if args.log_level:
                config['migration']['log_level'] = args.log_level
            if args.fail_fast:
                config['migration']['fail_on_missing_tables'] = True
            if args.continue_on_error:
                config['migration']['continue_on_error'] = True
                
            return config
        except Exception as e:
            print(f"❌ Configuration error: {e}")
            sys.exit(1)
    
    def display_config(self, config: dict):
        """Display current configuration."""
        print("🔧 Current Configuration:")
        print("=" * 50)
        
        # MySQL Configuration
        print("\n📊 MySQL Connection:")
        mysql_config = config['mysql']
        print(f"  Host: {mysql_config['host']}:{mysql_config['port']}")
        print(f"  Database: {mysql_config['database']}")
        print(f"  Username: {mysql_config['username']}")
        print(f"  Password: {'*' * len(mysql_config['password'])}")
        
        # PostgreSQL Configuration
        print("\n🐘 PostgreSQL Connection:")
        postgres_config = config['postgres']
        print(f"  Host: {postgres_config['host']}:{postgres_config['port']}")
        print(f"  Database: {postgres_config['database']}")
        print(f"  Username: {postgres_config['username']}")
        print(f"  Password: {'*' * len(postgres_config['password'])}")
        
        # Migration Settings
        print("\n⚙️  Migration Settings:")
        migration_config = config['migration']
        print(f"  Batch Size: {migration_config['batch_size']}")
        print(f"  Max Workers: {migration_config['max_workers']}")
        print(f"  Log Level: {migration_config['log_level']}")
        print(f"  Fail on Missing Tables: {migration_config['fail_on_missing_tables']}")
        print(f"  Continue on Error: {migration_config['continue_on_error']}")
        
        if migration_config.get('exclude_tables'):
            print(f"  Excluded Tables: {', '.join(migration_config['exclude_tables'])}")
        if migration_config.get('include_tables'):
            print(f"  Included Tables: {', '.join(migration_config['include_tables'])}")
    
    def test_connections(self, config: dict) -> bool:
        """Test database connections."""
        print("🔍 Testing Database Connections...")
        
        try:
            engine = MigrationEngine(config, self.status_monitor, self.error_collector)
            success = engine.test_connections()
            
            if success:
                print("✅ All database connections successful!")
                return True
            else:
                print("❌ Connection test failed!")
                self.error_collector.print_summary()
                return False
                
        except Exception as e:
            print(f"❌ Connection test error: {e}")
            return False
    
    def run_migration(self, config: dict, dry_run: bool = False):
        """Execute migration or dry run."""
        mode = "🔍 Dry Run" if dry_run else "🚀 Migration"
        print(f"{mode} Starting...")
        
        try:
            engine = MigrationEngine(config, self.status_monitor, self.error_collector)
            
            if dry_run:
                success = engine.dry_run()
            else:
                success = engine.migrate()
            
            if success:
                print(f"✅ {mode} completed successfully!")
            else:
                print(f"❌ {mode} failed!")
                self.error_collector.print_summary()
                
        except Exception as e:
            print(f"❌ {mode} error: {e}")
            self.error_collector.add_error('MIGRATION_ERROR', str(e), critical=True)
            self.error_collector.print_summary()
    
    def run_validation(self, config: dict):
        """Run post-migration validation."""
        print("🔍 Starting Validation...")
        
        try:
            validator = ValidationEngine(config, self.status_monitor, self.error_collector)
            success = validator.validate()
            
            if success:
                print("✅ Validation completed successfully!")
            else:
                print("❌ Validation failed!")
                self.error_collector.print_summary()
                
        except Exception as e:
            print(f"❌ Validation error: {e}")
            self.error_collector.add_error('VALIDATION_ERROR', str(e), critical=True)
            self.error_collector.print_summary()
    
    def run_setup(self):
        """Run setup wizard."""
        print("🛠️  Starting Setup Wizard...")
        
        try:
            wizard = SetupWizard()
            wizard.run()
            print("✅ Setup completed successfully!")
        except Exception as e:
            print(f"❌ Setup error: {e}")
    
    def run(self):
        """Main CLI entry point."""
        parser = self.create_parser()
        args = parser.parse_args()
        
        # Setup logging
        log_level = args.log_level or 'INFO'
        setup_logging(log_level)
        
        # Handle setup separately (doesn't need config)
        if args.setup:
            self.run_setup()
            return
        
        # Load and setup configuration
        config = self.setup_configuration(args)
        
        # Execute requested action
        try:
            if args.config:
                self.display_config(config)
            elif args.test_connections:
                self.test_connections(config)
            elif args.migrate:
                self.run_migration(config, dry_run=False)
            elif args.dry_run:
                self.run_migration(config, dry_run=True)
            elif args.validate:
                self.run_validation(config)
                
        except KeyboardInterrupt:
            print("\n⚠️  Operation cancelled by user")
            sys.exit(1)
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            sys.exit(1)


def main():
    """Entry point for the CLI application."""
    cli = CLI()
    cli.run()


if __name__ == '__main__':
    main()
