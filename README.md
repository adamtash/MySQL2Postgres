# MySQL to PostgreSQL Migration Tool

A command-line tool for migrating data from MySQL to PostgreSQL databases. This tool migrates data only - it does not create or modify schemas. You must create the target PostgreSQL schema before running the migration.

## Purpose

This tool was specifically designed to facilitate data migrations from Entity Framework Core applications using the [MySQL Pomelo provider](https://github.com/PomeloFoundation/Pomelo.EntityFrameworkCore.MySql) to the more robust and actively maintained [PostgreSQL Npgsql provider](https://github.com/npgsql/efcore.pg). The Pomelo MySQL provider for .Net 9.0 is currently in preview status (as of July 2025) and lacks regular maintenance updates, making PostgreSQL with Npgsql a more reliable long-term solution for production applications.

## Requirements

- Python 3.8+
- MySQL 8.0+ (source database)
- PostgreSQL (target database with existing schema)

## Features

- Data-only migration (schema must exist in target)
- Batch processing with configurable sizes
- Foreign key constraint management
- Case-insensitive table name mapping
- Generated column detection and exclusion
- Automatic sequence/identity column value resetting
- Dry-run mode for testing
- Progress monitoring and logging

## Quick Start

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure database connections:
```bash
python cli.py --setup
```

3. Test the configuration:
```bash
python cli.py --dry-run
```

4. Run the migration:
```bash
python cli.py --migrate
```

## Installation

Clone the repository and install dependencies:

```bash
git clone https://github.com/adamtash/MySQL2Postgres.git
cd MySQL2Postgres
pip install -r requirements.txt
```

### Virtual Environment Setup

It's recommended to use a virtual environment to avoid dependency conflicts:

#### macOS/Linux

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run the migration tool
python cli.py --migrate

# Deactivate when done
deactivate
```

#### Windows

```cmd
# Create virtual environment
python -m venv venv

# Activate virtual environment
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run the migration tool
python cli.py --migrate

# Deactivate when done
deactivate
```

**Note**: Make sure to activate the virtual environment each time you want to use the migration tool.

## Configuration

Create a `.env` file with your database settings:

```bash
# MySQL Connection
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DATABASE=your_mysql_database
MYSQL_USERNAME=your_mysql_username
MYSQL_PASSWORD=your_mysql_password

# PostgreSQL Connection
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DATABASE=your_postgres_database
POSTGRES_USERNAME=your_postgres_username
POSTGRES_PASSWORD=your_postgres_password

# Migration Settings
BATCH_SIZE=100
EXCLUDE_TABLES=table1,table2
IGNORE_GENERATED_COLUMNS=true
DISABLE_FOREIGN_KEYS=true
RESET_AUTO_INCREMENT=true
TRUNCATE_TARGET_TABLES=false
```

## Basic Usage

```bash
# Interactive setup
python cli.py --setup

# Test database connections
python cli.py --test-connections

# Run a dry-run (no actual changes)
python cli.py --dry-run

# Execute the migration
python cli.py --migrate

# Reset auto-increment values only (for previously migrated databases)
python cli.py --reset-auto-increment

# Validate migration results
python cli.py --validate
```

## Command Options

| Command | Description |
|---------|-------------|
| `--setup` | Interactive configuration wizard |
| `--test-connections` | Test database connectivity |
| `--dry-run` | Simulate migration without making changes |
| `--migrate` | Execute the data migration |
| `--reset-auto-increment` | Reset PostgreSQL auto-increment values only (for previously migrated databases) |
| `--validate` | Verify migration results |
| `--config` | Display current configuration |

### Migration Options

| Option | Description |
|--------|-------------|
| `--batch-size N` | Records per batch (default: 100) |
| `--exclude table1,table2` | Tables to exclude from migration |
| `--include table1,table2` | Only migrate specified tables |
| `--log-level LEVEL` | Set logging level (DEBUG, INFO, WARNING, ERROR) |

## Important Notes

- **Schema Migration**: This tool only migrates data. You must create the PostgreSQL schema manually or using tools like EF Core before running the migration.
- **Foreign Keys**: The tool can automatically disable foreign key constraints during migration and re-enable them afterward.
- **Generated Columns**: PostgreSQL generated columns are automatically detected and excluded from data insertion.
- **Auto-Increment Reset**: After migration, PostgreSQL auto-increment values (SERIAL/IDENTITY columns) are automatically reset to the correct next values to prevent ID conflicts on future inserts. For databases migrated before this feature was added, use `--reset-auto-increment` to update existing databases.
- **Table Names**: The tool handles case-sensitive table name mapping between MySQL and PostgreSQL.

## License

MIT License
