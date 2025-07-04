#!/usr/bin/env python3
"""
Test runner for distributed database project.
Runs all test modules and provides a summary with point values.
"""

import sys
import logging
import time
import subprocess

# Configure logging to file
log_file = "run_tests_log.txt"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def _format_test_name(test_name):
    """Format a test method name to be more readable.

    Args:
        test_name: The name of the test method (e.g., test_01_some_test_name)

    Returns:
        str: A formatted test name (e.g., "Test 01: Some Test Name")
    """
    if test_name.startswith('test_'):
        # Remove the 'test_' prefix
        name_parts = test_name[5:].split('_')
        if name_parts and name_parts[0].isdigit():
            # If the first part is a number, format it as "Test XX: Rest of Name"
            test_num = name_parts[0]
            rest_of_name = ' '.join(name_parts[1:])
            return f"Test {test_num}: {rest_of_name.replace('_', ' ').title()}"

    # If we can't parse it in the expected format, just clean it up a bit
    return test_name.replace('_', ' ').title()

# Test categories with point values
TEST_CATEGORIES = {
    "tests.test_schema_and_crud": {
        "name": "Schema Definition and Basic CRUD Operations",
        "points": 20,
        "expected_tests": 11
    },
    "tests.test_horizontal_partitioning": {
        "name": "Horizontal Partitioning and Routing",
        "points": 30,
        "expected_tests": 20
    },
    "tests.test_vertical_partitioning": {
        "name": "Vertical Partitioning",
        "points": 30,
        "expected_tests": 18
    },
    "tests.test_consensus": {
        "name": "Consensus and Fault Tolerance",
        "points": 30,
        "expected_tests": 10
    },
    "tests.test_caching": {
        "name": "Coordinator Caching",
        "points": 40,
        "expected_tests": 20
    },
    "tests.test_concurrent_clients": {
        "name": "Concurrent Clients and Consistency Under Load",
        "points": 50,
        "expected_tests": 19
    }
}

def run_test_module(module_name):
    """
    Run a single test module and return the results.

    Args:
        module_name: The name of the test module to run

    Returns:
        tuple: (success, total, results_text, test_details)
    """
    # We'll use a direct approach to run the tests
    # This is more reliable than creating a wrapper script

    # Log that we're running the test
    with open(log_file, 'w') as log:
        log.write(f"\n\n=== Running {module_name} ===\n")

    # Create a command to run the test module with output redirection
    cmd = [
        sys.executable,
        "-c",
        f"""
import sys, os, unittest, importlib, logging, json

# Redirect stdout and stderr to the log file
log_file = open('{log_file}', 'a')
sys.stdout = log_file
sys.stderr = log_file

# Disable all other logging to stdout/stderr
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)
logging.basicConfig(filename='{log_file}', level=logging.INFO)

# Custom test result class to capture test names
class DetailedTestResult(unittest.TextTestResult):
    def __init__(self, stream, descriptions, verbosity):
        super().__init__(stream, descriptions, verbosity)
        self.test_results = []

    def addSuccess(self, test):
        super().addSuccess(test)
        self.test_results.append({{'name': str(test), 'status': 'Pass'}})

    def addFailure(self, test, err):
        super().addFailure(test, err)
        self.test_results.append({{'name': str(test), 'status': 'Fail'}})

    def addError(self, test, err):
        super().addError(test, err)
        self.test_results.append({{'name': str(test), 'status': 'Error'}})

# Run the tests
try:
    # Import the module
    module = importlib.import_module('{module_name}')

    # Get all test classes in the module
    test_classes = []
    for name, obj in module.__dict__.items():
        if isinstance(obj, type) and issubclass(obj, unittest.TestCase) and obj != unittest.TestCase:
            test_classes.append(obj)

    # Create a test suite with all test classes
    suite = unittest.TestSuite()
    for test_class in test_classes:
        # Get all test methods in the class
        test_methods = [m for m in dir(test_class) if m.startswith('test_')]
        for method_name in test_methods:
            suite.addTest(test_class(method_name))

    # Use our custom test result class
    result = unittest.TextTestRunner(
        resultclass=DetailedTestResult,
        verbosity=0
    ).run(suite)

    # Print the results in a format we can parse
    print('TEST_RESULTS_START')
    print(result.testsRun)
    print(len(result.failures))
    print(len(result.errors))

    # Print detailed test results
    print(json.dumps(result.test_results))
    print('TEST_RESULTS_END')

    # Close the log file
    log_file.close()

    # Exit with success
    sys.exit(0)
except Exception as e:
    # Print the error
    print('TEST_RESULTS_START')
    print('0')
    print('0')
    print('1')
    print('[]')  # Empty test results
    print(str(e))
    print('TEST_RESULTS_END')

    # Close the log file
    log_file.close()

    # Exit with error
    sys.exit(1)
"""
    ]

    # Run the command and capture the output
    try:
        # Run the test in a separate process
        subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        # Parse the results from the log file
        with open(log_file, 'r') as f:
            log_content = f.read()

        # Find the test results in the log file
        start_marker = 'TEST_RESULTS_START'
        end_marker = 'TEST_RESULTS_END'

        if start_marker in log_content and end_marker in log_content:
            # Extract the results
            start_idx = log_content.find(start_marker) + len(start_marker)
            end_idx = log_content.find(end_marker, start_idx)
            results_text = log_content[start_idx:end_idx].strip()

            # Parse the results
            lines = results_text.split('\n')
            if len(lines) >= 4:  # We now have at least 4 lines (total, failures, errors, test_details)
                total = int(lines[0].strip())
                failures = int(lines[1].strip())
                errors = int(lines[2].strip())

                # Parse the test details JSON
                import json
                try:
                    test_details = json.loads(lines[3].strip())
                except:
                    test_details = []

                success = total - failures - errors
                return success, total, f"{success}/{total} tests passed", test_details

        # If we couldn't parse the results, try to count the tests from the log
        test_count = log_content.count('... ok')
        if test_count > 0:
            return test_count, test_count, f"{test_count}/{test_count} tests passed", []

        # If all else fails, return a default value
        return 0, 0, "ERROR - Could not determine test results", []

    except Exception as e:
        logging.error(f"Error running {module_name}: {str(e)}")
        return 0, 0, f"ERROR - {str(e)}", []

def run_all_tests(specific_modules=None):
    """
    Run all test modules or specific modules if provided.

    Args:
        specific_modules: Optional list of specific test modules to run

    Returns:
        dict: Results for each module
    """
    results = {}
    total_points = 0
    earned_points = 0

    # Determine which modules to run
    modules_to_run = specific_modules if specific_modules else TEST_CATEGORIES.keys()

    print(f"Running tests (detailed logs saved to {log_file})")
    print("-" * 60)

    for module_name in modules_to_run:
        if module_name in TEST_CATEGORIES:
            category = TEST_CATEGORIES[module_name]
            category_name = category['name']

            # Print the category name
            print(f"{category_name}:")

            # Run the tests
            success, total, results_text, test_details = run_test_module(module_name)

            # Use expected test count if available
            expected_tests = category.get('expected_tests', total)

            # For display purposes, show the expected test count
            if 'expected_tests' in category and total != expected_tests:
                logging.warning(f"Expected {expected_tests} tests for {module_name}, but found {total}")
                # Use the actual count for calculations, but display the expected count
                display_total = expected_tests
            else:
                display_total = total

            # Calculate points based on success rate
            if total > 0:
                points_earned = round((success / total) * category['points'])
            else:
                points_earned = 0

            results[module_name] = {
                "success": success,
                "total": total,
                "display_total": display_total,
                "points_earned": points_earned,
                "points_possible": category['points'],
                "results_text": results_text,
                "test_details": test_details
            }

            # Print individual test results
            if test_details:
                for test in test_details:
                    # Extract the full test method name from the test name
                    test_name = test['name']
                    if '(' in test_name and ')' in test_name:
                        # Format is typically "test_method (module.TestClass)"
                        method_name = test_name.split('(')[0].strip()
                        class_name = test_name.split('(')[1].split(')')[0].strip()

                        # Try to get the actual test method from the module
                        try:
                            import importlib
                            module_name = '.'.join(class_name.split('.')[:-1])
                            class_name = class_name.split('.')[-1]

                            # Import the module
                            module = importlib.import_module(module_name)

                            # Get the class
                            test_class = getattr(module, class_name)

                            # Get the method
                            test_method = getattr(test_class, method_name)

                            # Get the docstring
                            docstring = test_method.__doc__
                            if docstring:
                                # Use the first line of the docstring as the test name
                                readable_name = docstring.strip().split('\n')[0].strip('"')
                            else:
                                # Fall back to formatting the method name
                                readable_name = _format_test_name(method_name)
                        except Exception as e:
                            # If anything goes wrong, fall back to formatting the method name
                            logging.warning(f"Error getting test docstring: {e}")
                            readable_name = _format_test_name(method_name)
                    else:
                        # If the format is unexpected, just use the test name as is
                        readable_name = test_name

                    # Print the test name and status
                    status = test['status']
                    print(f"  {readable_name} {status}")

            # Print the summary for this module
            import re
            # Use display_total for the summary
            clean_results = re.sub(r'[\x00-\x1F\x7F-\xFF]', '', f"{success}/{display_total} tests passed")
            print(f"  Summary: {clean_results}")
            print()

            # Update totals
            total_points += category['points']
            earned_points += points_earned
        else:
            print(f"Unknown test module: {module_name}")

    return results, earned_points, total_points

def print_summary(results, earned_points, total_points):
    """Print a summary of all test results."""
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)

    for module_name, result in results.items():
        if module_name in TEST_CATEGORIES:
            category = TEST_CATEGORIES[module_name]
            print(f"{category['name']} ({category['points']} points):")
            # Use display_total for the summary
            display_total = result.get('display_total', result['total'])
            print(f"  {result['success']}/{display_total} tests passed")
            print(f"  Points earned: {result['points_earned']}/{result['points_possible']}")
            print()

    print("-" * 60)
    print(f"TOTAL POINTS: {earned_points}/{total_points}")
    print("=" * 60)

def main():
    """Main function to run tests."""
    # Clear the log file
    with open(log_file, 'w') as f:
        f.write(f"Test run started at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Check if specific test modules were requested
    specific_modules = None
    if len(sys.argv) > 1:
        specific_modules = sys.argv[1:]

    # Run tests
    results, earned_points, total_points = run_all_tests(specific_modules)

    # Print summary
    print_summary(results, earned_points, total_points)

if __name__ == "__main__":
    main()
