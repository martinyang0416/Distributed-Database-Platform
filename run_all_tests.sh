#!/bin/bash
# Run all tests for the distributed database project

echo "Running all tests for the distributed database project"
echo "This will run all test modules and provide a summary at the end"
echo "Detailed logs will be saved to run_tests_log.txt"
echo

# Clear the log file
> run_tests_log.txt

# Find the best Python executable to use
if command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD=python3
else
    PYTHON_CMD=python
fi

echo "Using Python command: $PYTHON_CMD"

# Run each test module individually to ensure proper test discovery
echo "============================================================" >> run_tests_log.txt
echo "RUNNING INDIVIDUAL TEST MODULES" >> run_tests_log.txt
echo "============================================================" >> run_tests_log.txt

# Schema and CRUD tests
echo "Running Schema Definition and Basic CRUD Operations tests..."
$PYTHON_CMD run_tests.py tests.test_schema_and_crud

# Horizontal partitioning tests
echo "Running Horizontal Partitioning and Routing tests..."
$PYTHON_CMD run_tests.py tests.test_horizontal_partitioning

# Vertical partitioning tests
echo "Running Vertical Partitioning tests..."
$PYTHON_CMD run_tests.py tests.test_vertical_partitioning

# Consensus tests
echo "Running Consensus and Fault Tolerance tests..."
$PYTHON_CMD run_tests.py tests.test_consensus

# Caching tests
echo "Running Coordinator Caching tests..."
$PYTHON_CMD run_tests.py tests.test_caching

# Concurrent clients tests
echo "Running Concurrent Clients and Consistency Under Load tests..."
$PYTHON_CMD run_tests.py tests.test_concurrent_clients

echo
echo "All tests completed!"
