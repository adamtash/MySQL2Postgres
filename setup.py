#!/usr/bin/env python3
"""
Setup Script for MySQL to PostgreSQL Migration Tool

Provides automatic virtual environment creation, dependency installation,
environment file template copying, and system requirement validation.
"""

import os
import sys
import subprocess
import venv
from pathlib import Path


class MigrationToolSetup:
    """Setup manager for the migration tool."""
    
    def __init__(self):
        self.project_root = Path(__file__).parent
        self.venv_path = self.project_root / 'venv'
        self.requirements_file = self.project_root / 'requirements.txt'
        self.env_example = self.project_root / '.env.example'
        self.env_file = self.project_root / '.env'
        
    def run_setup(self):
        """Run complete setup process."""
        print("🛠️  MySQL to PostgreSQL Migration Tool Setup")
        print("=" * 50)
        
        try:
            # Check Python version
            self.check_python_version()
            
            # Create virtual environment
            self.create_virtual_environment()
            
            # Install dependencies
            self.install_dependencies()
            
            # Setup environment file
            self.setup_environment_file()
            
            # Validate installation
            self.validate_installation()
            
            # Show completion message
            self.show_completion_message()
            
        except Exception as e:
            print(f"❌ Setup failed: {e}")
            sys.exit(1)
    
    def check_python_version(self):
        """Check if Python version is compatible."""
        print("🐍 Checking Python version...")
        
        version = sys.version_info
        if version.major < 3 or (version.major == 3 and version.minor < 8):
            raise Exception(
                f"Python 3.8+ required, but found {version.major}.{version.minor}. "
                "Please upgrade Python."
            )
        
        print(f"✅ Python {version.major}.{version.minor}.{version.micro} is compatible")
    
    def create_virtual_environment(self):
        """Create virtual environment if it doesn't exist."""
        print("📦 Setting up virtual environment...")
        
        if self.venv_path.exists():
            print(f"✅ Virtual environment already exists at {self.venv_path}")
            return
        
        try:
            venv.create(self.venv_path, with_pip=True)
            print(f"✅ Virtual environment created at {self.venv_path}")
        except Exception as e:
            raise Exception(f"Failed to create virtual environment: {e}")
    
    def get_venv_python(self):
        """Get the path to Python executable in virtual environment."""
        if os.name == 'nt':  # Windows
            return self.venv_path / 'Scripts' / 'python.exe'
        else:  # Unix-like
            return self.venv_path / 'bin' / 'python'
    
    def get_venv_pip(self):
        """Get the path to pip executable in virtual environment."""
        if os.name == 'nt':  # Windows
            return self.venv_path / 'Scripts' / 'pip.exe'
        else:  # Unix-like
            return self.venv_path / 'bin' / 'pip'
    
    def install_dependencies(self):
        """Install required dependencies."""
        print("📋 Installing dependencies...")
        
        if not self.requirements_file.exists():
            raise Exception(f"Requirements file not found: {self.requirements_file}")
        
        # Upgrade pip first
        pip_path = self.get_venv_pip()
        try:
            subprocess.run([
                str(pip_path), 'install', '--upgrade', 'pip'
            ], check=True, capture_output=True, text=True)
            print("✅ Pip upgraded successfully")
        except subprocess.CalledProcessError as e:
            print(f"⚠️  Warning: Failed to upgrade pip: {e}")
        
        # Install requirements
        try:
            result = subprocess.run([
                str(pip_path), 'install', '-r', str(self.requirements_file)
            ], check=True, capture_output=True, text=True)
            
            print("✅ Dependencies installed successfully")
            
            # Show installed packages
            if '--verbose' in sys.argv:
                print("\n📦 Installed packages:")
                for line in result.stdout.split('\n'):
                    if 'Successfully installed' in line:
                        packages = line.replace('Successfully installed', '').strip()
                        for package in packages.split():
                            print(f"  • {package}")
            
        except subprocess.CalledProcessError as e:
            raise Exception(f"Failed to install dependencies: {e.stderr}")
    
    def setup_environment_file(self):
        """Setup environment configuration file."""
        print("⚙️  Setting up environment configuration...")
        
        # Create .env.example if it doesn't exist
        if not self.env_example.exists():
            self.create_env_example()
        
        # Copy to .env if it doesn't exist
        if not self.env_file.exists():
            try:
                import shutil
                shutil.copy2(self.env_example, self.env_file)
                print(f"✅ Environment template copied to {self.env_file}")
                print("⚠️  Please edit .env with your actual database credentials")
            except Exception as e:
                print(f"⚠️  Warning: Could not copy environment file: {e}")
        else:
            print(f"✅ Environment file already exists at {self.env_file}")
    
    def create_env_example(self):
        """Create .env.example file."""
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
        
        with open(self.env_example, 'w') as f:
            f.write(example_content)
        
        print(f"✅ Created environment example at {self.env_example}")
    
    def validate_installation(self):
        """Validate that all required packages are properly installed."""
        print("🔍 Validating installation...")
        
        python_path = self.get_venv_python()
        
        # Test imports
        test_imports = [
            'mysql.connector',
            'psycopg[binary]',
            'sqlalchemy',
            'dotenv',
            'tqdm',
            'colorama',
            'tabulate'
        ]
        
        failed_imports = []
        
        for module in test_imports:
            try:
                result = subprocess.run([
                    str(python_path), '-c', f'import {module}'
                ], check=True, capture_output=True, text=True)
                print(f"  ✅ {module}")
            except subprocess.CalledProcessError:
                print(f"  ❌ {module}")
                failed_imports.append(module)
        
        if failed_imports:
            raise Exception(
                f"Failed to import required modules: {', '.join(failed_imports)}. "
                "Please check your installation."
            )
        
        print("✅ All required packages validated")
    
    def show_completion_message(self):
        """Show setup completion message with next steps."""
        print("\n" + "=" * 50)
        print("🎉 Setup completed successfully!")
        print("=" * 50)
        
        # Activation instructions
        print("\n📋 Next Steps:")
        if os.name == 'nt':  # Windows
            activate_script = self.venv_path / 'Scripts' / 'activate.bat'
            print(f"1. Activate virtual environment:")
            print(f"   {activate_script}")
        else:  # Unix-like
            activate_script = self.venv_path / 'bin' / 'activate'
            print(f"1. Activate virtual environment:")
            print(f"   source {activate_script}")
        
        print(f"2. Edit configuration file: {self.env_file}")
        print("3. Run setup wizard: python cli.py --setup")
        print("4. Test connections: python cli.py --test-connections")
        print("5. Run dry-run: python cli.py --dry-run")
        
        print("\n🔧 Alternative: Use Makefile targets")
        print("   make setup    - Run configuration wizard")
        print("   make test     - Run dry-run migration")
        print("   make migrate  - Execute migration")
        print("   make validate - Validate results")
        
        print("\n💡 Tips:")
        print("• Always backup your PostgreSQL database before migration")
        print("• Start with a dry-run to identify potential issues")
        print("• Check logs in the 'logs/' directory during migration")
        
        print(f"\n📚 For help: python cli.py --help")
    
    def check_system_requirements(self):
        """Check system requirements and database connectivity."""
        print("🔍 Checking system requirements...")
        
        # Check disk space
        try:
            import shutil
            total, used, free = shutil.disk_usage(self.project_root)
            free_gb = free // (1024**3)
            
            if free_gb < 1:
                print(f"⚠️  Warning: Low disk space ({free_gb}GB free)")
            else:
                print(f"✅ Sufficient disk space ({free_gb}GB free)")
        except Exception:
            print("⚠️  Could not check disk space")
        
        # Check memory (basic check)
        try:
            import psutil
            memory = psutil.virtual_memory()
            available_gb = memory.available // (1024**3)
            
            if available_gb < 1:
                print(f"⚠️  Warning: Low memory ({available_gb}GB available)")
            else:
                print(f"✅ Sufficient memory ({available_gb}GB available)")
        except ImportError:
            print("ℹ️  Install psutil for memory checking: pip install psutil")
        except Exception:
            print("⚠️  Could not check memory")


def main():
    """Main setup entry point."""
    # Check if setup should be run
    if len(sys.argv) > 1 and sys.argv[1] in ['--help', '-h']:
        print("MySQL to PostgreSQL Migration Tool Setup")
        print("Usage: python setup.py [--verbose]")
        print("")
        print("This script will:")
        print("• Create a virtual environment")
        print("• Install required dependencies")
        print("• Setup environment configuration template")
        print("• Validate installation")
        return
    
    setup = MigrationToolSetup()
    setup.run_setup()


if __name__ == '__main__':
    main()
