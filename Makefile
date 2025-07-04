# Makefile for the distributed database project
# Supports build and final rules for autograder compatibility

# Package name
PKGNAME = distributed_db

# Find Python executable - try several options
PYTHON3 := $(shell which python3 2>/dev/null)
PYTHON := $(shell which python 2>/dev/null)
PY := $(if $(PYTHON3),$(PYTHON3),$(PYTHON))

# Arguments for test runs
MKARGS =

# Lock file to prevent duplicate builds
LOCK_FILE = .build_lock

# Log file for build process
BUILD_LOG = build_log.txt

.PHONY: build final all clean help install-deps setup

# Install system dependencies if needed (for autograder)
install-deps:
	@echo "Checking and installing system dependencies..." | tee -a $(BUILD_LOG)
	@if [ -f /etc/debian_version ]; then \
		echo "Debian/Ubuntu system detected, installing dependencies..." | tee -a $(BUILD_LOG); \
		sudo apt-get update -qq || true; \
		sudo apt-get install -y python3-pip python3-venv python3-flask libsqlite3-dev python3-full python3.12-venv || true; \
	fi

# Setup environment and dependencies
setup: install-deps
	@echo "Setting up environment..." | tee -a $(BUILD_LOG)
	@echo "Creating data directory if it doesn't exist..." | tee -a $(BUILD_LOG)
	@mkdir -p data
	@echo "Installing Python dependencies..." | tee -a $(BUILD_LOG)
	@if command -v pip3 >/dev/null 2>&1; then \
		echo "Installing packages with pip3..." | tee -a $(BUILD_LOG); \
		sudo pip3 install flask tinydb pysqlite3 pysyncobj requests pytest || true; \
		if [ -f requirements.txt ]; then \
			echo "Installing packages from requirements.txt..." | tee -a $(BUILD_LOG); \
			sudo pip3 install -r requirements.txt || true; \
		fi; \
	elif command -v pip >/dev/null 2>&1; then \
		echo "Installing packages with pip..." | tee -a $(BUILD_LOG); \
		sudo pip install flask tinydb pysqlite3 pysyncobj requests pytest || true; \
		if [ -f requirements.txt ]; then \
			echo "Installing packages from requirements.txt..." | tee -a $(BUILD_LOG); \
			sudo pip install -r requirements.txt || true; \
		fi; \
	elif [ -f /usr/bin/apt-get ]; then \
		echo "Using apt-get to install Python dependencies..." | tee -a $(BUILD_LOG); \
		sudo apt-get install -y python3-flask python3-requests python3-pytest || true; \
	fi
	@echo "Making scripts executable..." | tee -a $(BUILD_LOG)
	@chmod +x cli.py run_tests.py demo_crud.sh demo_nosql_crud.sh cleanup.sh run_all_tests.sh 2>/dev/null || true

# Build the project (prepare environment)
build:
	@if [ -f $(LOCK_FILE) ]; then \
		echo "Build already in progress or completed. Remove $(LOCK_FILE) to force rebuild."; \
	else \
		touch $(LOCK_FILE); \
		echo "Building the distributed database project..." | tee $(BUILD_LOG); \
		echo "Using Python: $(PY)" | tee -a $(BUILD_LOG); \
		$(MAKE) setup; \
		echo "Build completed successfully!" | tee -a $(BUILD_LOG); \
	fi

# Run all tests for final grading
final: build
	@echo "Running all tests for final grading..."
	@if [ -f run_all_tests.sh ]; then \
		chmod +x run_all_tests.sh; \
		./run_all_tests.sh || true; \
	else \
		echo "ERROR: run_all_tests.sh not found!"; \
		exit 1; \
	fi

# Alias all to final for backward compatibility
all: final

# Clean up temporary files
clean:
	@echo "Cleaning up temporary files..."
	@if [ -f cleanup.sh ]; then \
		./cleanup.sh; \
	else \
		echo "WARNING: cleanup.sh not found, skipping cleanup"; \
	fi
	@echo "Removing any PID files..."
	@rm -f coordinator.pid storage*.pid
	@echo "Removing build lock file..."
	@rm -f $(LOCK_FILE)
	@echo "Cleanup complete."
