# MySQL to PostgreSQL Migration Tool - Makefile
# Provides production-ready automation for setup, migration, and maintenance

# Configuration
PYTHON := python3
PIP := pip3
ENV_FILE := .env
LOG_DIR := logs
VENV_DIR := venv

# Default target
.DEFAULT_GOAL := help

# Colors for output
GREEN # Environment management
.PHONY: env-example
env-example: ## Create .env.example file
	@$(PYTHON) -c "from src.config_manager import ConfigManager; ConfigManager().create_example_env_file()"

.PHONY: env-backup
env-backup: ## Backup current .env file
	@if [ -f $(ENV_FILE) ]; then \
		cp $(ENV_FILE) $(ENV_FILE).backup.$(shell date +%Y%m%d_%H%M%S); \
		echo "$(GREEN)Environment file backed up$(NC)"; \
	else \
		echo "$(YELLOW)No .env file to backup$(NC)"; \
	fi

# Docker targets
.PHONY: docker-build
docker-build: ## Build Docker image
	@echo "$(GREEN)Building Docker image...$(NC)"
	@docker build -t $(PROJECT_NAME):latest .
	@echo "$(GREEN)Docker image built!$(NC)"

.PHONY: docker-up
docker-up: ## Start all services with Docker Compose
	@echo "$(GREEN)Starting Docker services...$(NC)"
	@$(DOCKER_COMPOSE) up -d
	@echo "$(GREEN)Services started! Waiting for health checks...$(NC)"
	@sleep 10
	@$(DOCKER_COMPOSE) ps

.PHONY: docker-down
docker-down: ## Stop all Docker services
	@echo "$(GREEN)Stopping Docker services...$(NC)"
	@$(DOCKER_COMPOSE) down
	@echo "$(GREEN)Services stopped!$(NC)"

.PHONY: docker-logs
docker-logs: ## Show Docker service logs
	@$(DOCKER_COMPOSE) logs -f

.PHONY: docker-test
docker-test: docker-up ## Run migration test in Docker environment
	@echo "$(GREEN)Running migration test in Docker...$(NC)"
	@$(DOCKER_COMPOSE) exec migrator python cli.py --test-connections
	@$(DOCKER_COMPOSE) exec migrator python cli.py --dry-run
	@echo "$(GREEN)Docker test completed!$(NC)"

.PHONY: docker-migrate
docker-migrate: docker-up ## Run actual migration in Docker environment
	@echo "$(GREEN)Running migration in Docker...$(NC)"
	@$(DOCKER_COMPOSE) exec migrator python cli.py --migrate
	@echo "$(GREEN)Docker migration completed!$(NC)"

.PHONY: docker-validate
docker-validate: ## Run validation in Docker environment
	@echo "$(GREEN)Running validation in Docker...$(NC)"
	@$(DOCKER_COMPOSE) exec migrator python cli.py --validate
	@echo "$(GREEN)Docker validation completed!$(NC)"

.PHONY: docker-shell
docker-shell: docker-up ## Open shell in migration container
	@echo "$(GREEN)Opening shell in migration container...$(NC)"
	@$(DOCKER_COMPOSE) exec migrator /bin/bash

.PHONY: docker-dev-shell
docker-dev-shell: docker-up ## Open shell in development container
	@echo "$(GREEN)Opening shell in development container...$(NC)"
	@$(DOCKER_COMPOSE) exec dev /bin/bash

.PHONY: docker-clean
docker-clean: ## Clean Docker containers and volumes
	@echo "$(GREEN)Cleaning Docker resources...$(NC)"
	@$(DOCKER_COMPOSE) down -v
	@docker system prune -f
	@echo "$(GREEN)Docker cleanup completed!$(NC)"

.PHONY: docker-reset
docker-reset: docker-clean docker-up ## Reset Docker environment (clean + restart)
	@echo "$(GREEN)Docker environment reset completed!$(NC)"
YELLOW := \033[1;33m
RED := \033[0;31m
NC := \033[0m # No Color

# Docker settings
DOCKER_COMPOSE := docker-compose
PROJECT_NAME := mysql2postgres

# Help target
.PHONY: help
help: ## Show this help message
	@echo "$(GREEN)MySQL to PostgreSQL Migration Tool$(NC)"
	@echo "$(GREEN)====================================$(NC)"
	@echo ""
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@echo ""
	@echo "Workflow examples:"
	@echo "  $(YELLOW)make setup$(NC)           - Complete environment setup"
	@echo "  $(YELLOW)make test$(NC)            - Dry-run migration (safe testing)"
	@echo "  $(YELLOW)make migrate$(NC)         - Execute actual migration"
	@echo "  $(YELLOW)make validate$(NC)        - Post-migration validation"
	@echo "  $(YELLOW)make full$(NC)            - Complete workflow (test → migrate → validate)"

# Setup & Configuration targets
.PHONY: setup
setup: install config-check ## Complete environment setup
	@echo "$(GREEN)Running setup wizard...$(NC)"
	@$(PYTHON) cli.py --setup
	@echo "$(GREEN)Setup completed!$(NC)"

.PHONY: install
install: ## Install dependencies only
	@echo "$(GREEN)Installing dependencies...$(NC)"
	@$(PIP) install -r requirements.txt
	@echo "$(GREEN)Dependencies installed!$(NC)"

.PHONY: install-dev
install-dev: install ## Install development dependencies
	@echo "$(GREEN)Installing development dependencies...$(NC)"
	@$(PIP) install pytest pytest-cov black flake8 isort
	@echo "$(GREEN)Development dependencies installed!$(NC)"

.PHONY: venv
venv: ## Create virtual environment
	@echo "$(GREEN)Creating virtual environment...$(NC)"
	@$(PYTHON) -m venv $(VENV_DIR)
	@echo "$(GREEN)Virtual environment created at $(VENV_DIR)$(NC)"
	@echo "$(YELLOW)Activate with: source $(VENV_DIR)/bin/activate$(NC)"

.PHONY: config
config: config-check ## Display current configuration
	@$(PYTHON) cli.py --config

.PHONY: config-check
config-check: ## Check if configuration exists
	@if [ ! -f $(ENV_FILE) ]; then \
		echo "$(YELLOW)Warning: $(ENV_FILE) not found. Run 'make setup' first.$(NC)"; \
		if [ ! -f .env.example ]; then \
			$(PYTHON) -c "from src.config_manager import ConfigManager; ConfigManager().create_example_env_file()"; \
		fi; \
	fi

# Migration Workflow targets
.PHONY: check
check: config-check ## System requirements and connectivity test
	@echo "$(GREEN)Testing database connections...$(NC)"
	@$(PYTHON) cli.py --test-connections

.PHONY: test
test: config-check ## Dry-run migration (safe testing)
	@echo "$(GREEN)Running migration dry-run...$(NC)"
	@$(PYTHON) cli.py --dry-run

.PHONY: migrate
migrate: config-check backup ## Execute actual migration
	@echo "$(GREEN)Starting migration...$(NC)"
	@$(PYTHON) cli.py --migrate

.PHONY: validate
validate: config-check ## Post-migration validation
	@echo "$(GREEN)Validating migration results...$(NC)"
	@$(PYTHON) cli.py --validate

.PHONY: full
full: test migrate validate ## Complete workflow (test → migrate → validate)
	@echo "$(GREEN)Full migration workflow completed!$(NC)"

.PHONY: quick
quick: migrate validate ## Quick workflow (migrate → validate)
	@echo "$(GREEN)Quick migration workflow completed!$(NC)"

# Testing targets
.PHONY: test-unit
test-unit: ## Run unit tests
	@echo "$(GREEN)Running unit tests...$(NC)"
	@$(PYTHON) -m pytest tests/ -v

.PHONY: test-coverage
test-coverage: ## Run tests with coverage
	@echo "$(GREEN)Running tests with coverage...$(NC)"
	@$(PYTHON) -m pytest tests/ --cov=src --cov-report=html --cov-report=term

.PHONY: perf-test
perf-test: config-check ## Performance testing with different batch sizes
	@echo "$(GREEN)Performance testing...$(NC)"
	@echo "Testing batch size 50..."
	@$(PYTHON) cli.py --dry-run --batch-size 50 --log-level WARNING
	@echo "Testing batch size 100..."
	@$(PYTHON) cli.py --dry-run --batch-size 100 --log-level WARNING
	@echo "Testing batch size 500..."
	@$(PYTHON) cli.py --dry-run --batch-size 500 --log-level WARNING
	@echo "Testing batch size 1000..."
	@$(PYTHON) cli.py --dry-run --batch-size 1000 --log-level WARNING

# Maintenance targets
.PHONY: clean
clean: ## Remove logs and cache files
	@echo "$(GREEN)Cleaning up...$(NC)"
	@rm -rf $(LOG_DIR)/*.log
	@rm -rf __pycache__/
	@rm -rf src/__pycache__/
	@rm -rf src/utils/__pycache__/
	@rm -rf .pytest_cache/
	@rm -rf htmlcov/
	@rm -rf .coverage
	@find . -name "*.pyc" -delete
	@find . -name "*.pyo" -delete
	@echo "$(GREEN)Cleanup completed!$(NC)"

.PHONY: clean-all
clean-all: clean ## Remove all generated files including virtual environment
	@echo "$(GREEN)Full cleanup...$(NC)"
	@rm -rf $(VENV_DIR)/
	@rm -f .env.backup.*
	@echo "$(GREEN)Full cleanup completed!$(NC)"

.PHONY: backup
backup: ## Create migration backup timestamp
	@echo "$(GREEN)Creating backup timestamp...$(NC)"
	@mkdir -p backups
	@echo "Migration backup recommended before running migration" > backups/backup_reminder_$(shell date +%Y%m%d_%H%M%S).txt
	@echo "$(YELLOW)Reminder: Backup your PostgreSQL database before migration!$(NC)"

.PHONY: logs
logs: ## Show recent migration logs
	@echo "$(GREEN)Recent migration logs:$(NC)"
	@if [ -d $(LOG_DIR) ]; then \
		ls -la $(LOG_DIR)/ | head -10; \
		echo ""; \
		echo "Latest log:"; \
		ls -t $(LOG_DIR)/*.log 2>/dev/null | head -1 | xargs tail -20 2>/dev/null || echo "No log files found"; \
	else \
		echo "No log directory found"; \
	fi

.PHONY: tail-logs
tail-logs: ## Tail the latest log file
	@echo "$(GREEN)Tailing latest log file...$(NC)"
	@ls -t $(LOG_DIR)/*.log 2>/dev/null | head -1 | xargs tail -f 2>/dev/null || echo "No log files found"

# Development targets
.PHONY: format
format: ## Format code with black and isort
	@echo "$(GREEN)Formatting code...$(NC)"
	@black src/ cli.py
	@isort src/ cli.py
	@echo "$(GREEN)Code formatted!$(NC)"

.PHONY: lint
lint: ## Run linting with flake8
	@echo "$(GREEN)Running linter...$(NC)"
	@flake8 src/ cli.py --max-line-length=100 --ignore=E203,W503

.PHONY: type-check
type-check: ## Run type checking (if mypy is installed)
	@echo "$(GREEN)Running type checker...$(NC)"
	@if command -v mypy >/dev/null 2>&1; then \
		mypy src/ --ignore-missing-imports; \
	else \
		echo "$(YELLOW)mypy not installed. Install with: pip install mypy$(NC)"; \
	fi

.PHONY: qa
qa: format lint type-check test-unit ## Run all quality assurance checks
	@echo "$(GREEN)All QA checks completed!$(NC)"

# Docker targets (optional)
.PHONY: docker-build
docker-build: ## Build Docker image
	@echo "$(GREEN)Building Docker image...$(NC)"
	@docker build -t mysql2postgres .

.PHONY: docker-run
docker-run: ## Run in Docker container
	@echo "$(GREEN)Running in Docker...$(NC)"
	@docker run -it --rm -v $(PWD)/.env:/app/.env mysql2postgres

# Utility targets
.PHONY: requirements
requirements: ## Generate requirements.txt from current environment
	@echo "$(GREEN)Generating requirements.txt...$(NC)"
	@$(PIP) freeze > requirements.txt
	@echo "$(GREEN)Requirements updated!$(NC)"

.PHONY: check-deps
check-deps: ## Check for outdated dependencies
	@echo "$(GREEN)Checking for outdated dependencies...$(NC)"
	@$(PIP) list --outdated

.PHONY: upgrade-deps
upgrade-deps: ## Upgrade all dependencies
	@echo "$(GREEN)Upgrading dependencies...$(NC)"
	@$(PIP) install --upgrade -r requirements.txt
	@echo "$(GREEN)Dependencies upgraded!$(NC)"

.PHONY: info
info: ## Show system information
	@echo "$(GREEN)System Information:$(NC)"
	@echo "Python version: $(shell $(PYTHON) --version)"
	@echo "Pip version: $(shell $(PIP) --version)"
	@echo "Current directory: $(PWD)"
	@echo "Virtual environment: $(shell echo $$VIRTUAL_ENV || echo 'Not activated')"
	@echo "Environment file: $(shell [ -f $(ENV_FILE) ] && echo 'Found' || echo 'Not found')"
	@echo "Log directory: $(shell [ -d $(LOG_DIR) ] && echo 'Exists' || echo 'Not created')"

# Batch operation targets
.PHONY: batch-small
batch-small: ## Run migration with small batch size (good for testing)
	@$(PYTHON) cli.py --migrate --batch-size 10 --log-level DEBUG

.PHONY: batch-medium
batch-medium: ## Run migration with medium batch size
	@$(PYTHON) cli.py --migrate --batch-size 100

.PHONY: batch-large
batch-large: ## Run migration with large batch size (for performance)
	@$(PYTHON) cli.py --migrate --batch-size 1000

# Advanced workflow targets
.PHONY: safe-migrate
safe-migrate: backup check test ## Safe migration with all prechecks
	@echo "$(YELLOW)All prechecks passed. Proceeding with migration...$(NC)"
	@$(MAKE) migrate
	@$(MAKE) validate

.PHONY: troubleshoot
troubleshoot: logs info ## Show troubleshooting information
	@echo "$(GREEN)Troubleshooting Information:$(NC)"
	@echo ""
	@$(MAKE) info
	@echo ""
	@$(MAKE) logs

# Status targets
.PHONY: status
status: ## Show current migration status
	@echo "$(GREEN)Migration Tool Status:$(NC)"
	@echo "Configuration: $(shell [ -f $(ENV_FILE) ] && echo '✅ Ready' || echo '❌ Missing')"
	@echo "Dependencies: $(shell $(PYTHON) -c 'import mysql.connector, psycopg, sqlalchemy' 2>/dev/null && echo '✅ Installed' || echo '❌ Missing')"
	@echo "Log files: $(shell ls $(LOG_DIR)/*.log 2>/dev/null | wc -l | tr -d ' ') files"
	@echo "Last run: $(shell ls -t $(LOG_DIR)/*.log 2>/dev/null | head -1 | xargs stat -c %y 2>/dev/null | cut -d. -f1 || echo 'Never')"

# Watch targets (requires entr or inotify-tools)
.PHONY: watch-logs
watch-logs: ## Watch log directory for changes
	@echo "$(GREEN)Watching log directory...$(NC)"
	@if command -v entr >/dev/null 2>&1; then \
		find $(LOG_DIR) -name "*.log" | entr -c tail -f; \
	else \
		echo "$(YELLOW)entr not found. Install with: brew install entr (macOS) or apt install entr (Linux)$(NC)"; \
		$(MAKE) tail-logs; \
	fi

# Environment management
.PHONY: env-example
env-example: ## Create .env.example file
	@$(PYTHON) -c "from src.config_manager import ConfigManager; ConfigManager().create_example_env_file()"

.PHONY: env-backup
env-backup: ## Backup current .env file
	@if [ -f $(ENV_FILE) ]; then \
		cp $(ENV_FILE) $(ENV_FILE).backup.$(shell date +%Y%m%d_%H%M%S); \
		echo "$(GREEN)Environment file backed up$(NC)"; \
	else \
		echo "$(YELLOW)No environment file to backup$(NC)"; \
	fi
