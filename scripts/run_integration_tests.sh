#!/bin/bash

# Integration test runner script
# This script sets up the test environment and runs integration tests

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
GREPTIMEDB_TEST_ENDPOINT="localhost:4001"
GREPTIMEDB_TEST_DATABASE="public"
DOCKER_COMPOSE_FILE="docker-compose.test.yml"
MAX_WAIT_TIME=60

echo_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

echo_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

echo_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if GreptimeDB is ready
wait_for_greptimedb() {
    echo_info "Waiting for GreptimeDB to be ready..."
    local count=0
    while [ $count -lt $MAX_WAIT_TIME ]; do
        if curl -f http://localhost:4000/health >/dev/null 2>&1; then
            echo_info "GreptimeDB is ready!"
            return 0
        fi
        echo "Waiting... ($count/$MAX_WAIT_TIME)"
        sleep 1
        count=$((count + 1))
    done
    echo_error "GreptimeDB failed to start within $MAX_WAIT_TIME seconds"
    return 1
}

# Function to cleanup
cleanup() {
    echo_info "Cleaning up test environment..."
    docker-compose -f $DOCKER_COMPOSE_FILE down -v
}

# Trap to ensure cleanup on exit
trap cleanup EXIT

# Main execution
main() {
    echo_info "Starting GreptimeDB Rust Ingester Integration Tests"
    
    # Check if Docker is running
    if ! docker info >/dev/null 2>&1; then
        echo_error "Docker is not running. Please start Docker first."
        exit 1
    fi

    # Check if docker-compose file exists
    if [ ! -f "$DOCKER_COMPOSE_FILE" ]; then
        echo_error "Docker compose file '$DOCKER_COMPOSE_FILE' not found"
        exit 1
    fi

    # Start GreptimeDB test instance
    echo_info "Starting GreptimeDB test instance..."
    docker-compose -f $DOCKER_COMPOSE_FILE up -d
    
    # Wait for GreptimeDB to be ready
    if ! wait_for_greptimedb; then
        echo_error "Failed to start GreptimeDB"
        exit 1
    fi

    # Set environment variables for tests
    export GREPTIMEDB_TEST_ENDPOINT="$GREPTIMEDB_TEST_ENDPOINT"
    export GREPTIMEDB_TEST_DATABASE="$GREPTIMEDB_TEST_DATABASE"
    export RUST_LOG=info

    # Run integration tests
    echo_info "Running integration tests..."
    
    # Option 1: Run specific integration tests
    if [ "$1" = "specific" ]; then
        echo_info "Running specific test: $2"
        cargo test --test integration --features integration-tests -- "$2"
    # Option 2: Run all integration tests
    else
        echo_info "Running all integration tests..."
        cargo test --test integration --features integration-tests
    fi
    
    local test_result=$?
    
    if [ $test_result -eq 0 ]; then
        echo_info "✅ All integration tests passed!"
    else
        echo_error "❌ Some integration tests failed!"
        exit $test_result
    fi
}

# Parse command line arguments
case "$1" in
    "help"|"-h"|"--help")
        echo "Usage: $0 [specific <test_name>]"
        echo ""
        echo "Options:"
        echo "  specific <test_name>  Run a specific integration test"
        echo "  help                  Show this help message"
        echo ""
        echo "Examples:"
        echo "  $0                                    # Run all integration tests"
        echo "  $0 specific test_low_latency_insert   # Run specific test"
        echo ""
        echo "Environment Variables:"
        echo "  GREPTIMEDB_TEST_ENDPOINT    GreptimeDB endpoint (default: localhost:4001)"
        echo "  GREPTIMEDB_TEST_DATABASE    Test database name (default: public)"
        ;;
    *)
        main "$@"
        ;;
esac
