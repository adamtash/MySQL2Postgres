#!/bin/bash

# MySQL to PostgreSQL Migration Tool - Development Quick Start
# This script provides an easy way to get started with development

set -e

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Print banner
echo -e "${GREEN}"
echo "==============================================="
echo "   MySQL to PostgreSQL Migration Tool"
echo "             Development Setup"
echo "==============================================="
echo -e "${NC}"

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Error: Docker is not installed. Please install Docker first.${NC}"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}Error: Docker Compose is not installed. Please install Docker Compose first.${NC}"
    exit 1
fi

# Function to display options
show_menu() {
    echo -e "${YELLOW}What would you like to do?${NC}"
    echo "1) Quick Start (setup + test + migrate)"
    echo "2) Setup environment only"
    echo "3) Run test migration (dry-run)"
    echo "4) Run actual migration"
    echo "5) Run validation"
    echo "6) View logs"
    echo "7) Open development shell"
    echo "8) Clean and reset environment"
    echo "9) Stop all services"
    echo "0) Exit"
    echo ""
    read -p "Enter your choice [0-9]: " choice
}

# Function to wait for services
wait_for_services() {
    echo -e "${YELLOW}Waiting for database services to be ready...${NC}"
    sleep 15
    
    # Check if services are healthy
    if docker-compose exec mysql mysqladmin ping -h localhost --silent; then
        echo -e "${GREEN}✅ MySQL is ready${NC}"
    else
        echo -e "${RED}❌ MySQL is not ready${NC}"
        return 1
    fi
    
    if docker-compose exec postgres pg_isready -U testuser -d test_target >/dev/null 2>&1; then
        echo -e "${GREEN}✅ PostgreSQL is ready${NC}"
    else
        echo -e "${RED}❌ PostgreSQL is not ready${NC}"
        return 1
    fi
}

# Main menu loop
while true; do
    show_menu
    
    case $choice in
        1)
            echo -e "${GREEN}Starting quick setup...${NC}"
            make docker-up
            wait_for_services
            echo -e "${GREEN}Running test migration...${NC}"
            make docker-test
            echo -e "${GREEN}Running actual migration...${NC}"
            make docker-migrate
            echo -e "${GREEN}Running validation...${NC}"
            make docker-validate
            echo -e "${GREEN}✅ Quick start completed!${NC}"
            ;;
        2)
            echo -e "${GREEN}Setting up environment...${NC}"
            make docker-up
            wait_for_services
            echo -e "${GREEN}✅ Environment ready!${NC}"
            ;;
        3)
            echo -e "${GREEN}Running test migration...${NC}"
            make docker-test
            ;;
        4)
            echo -e "${GREEN}Running actual migration...${NC}"
            make docker-migrate
            ;;
        5)
            echo -e "${GREEN}Running validation...${NC}"
            make docker-validate
            ;;
        6)
            echo -e "${GREEN}Showing logs...${NC}"
            make docker-logs
            ;;
        7)
            echo -e "${GREEN}Opening development shell...${NC}"
            make docker-dev-shell
            ;;
        8)
            echo -e "${GREEN}Cleaning and resetting environment...${NC}"
            make docker-reset
            ;;
        9)
            echo -e "${GREEN}Stopping all services...${NC}"
            make docker-down
            ;;
        0)
            echo -e "${GREEN}Goodbye!${NC}"
            exit 0
            ;;
        *)
            echo -e "${RED}Invalid option. Please try again.${NC}"
            ;;
    esac
    
    echo ""
    echo -e "${YELLOW}Press Enter to continue...${NC}"
    read
    clear
done
