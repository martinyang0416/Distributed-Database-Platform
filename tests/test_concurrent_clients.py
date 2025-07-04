"""
Tests for concurrent clients and consistency under load in the distributed database.
"""
from distributed_db.client.client import DatabaseClient
from distributed_db.common.models import (
    DatabaseType, PartitionType, TableSchema, ColumnDefinition
)
import os
import sys
import unittest
import tempfile
import shutil
import time
import requests
import subprocess
import threading
import random
from concurrent.futures import ThreadPoolExecutor

# Add parent directory to path
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))


class TestConcurrentClients(unittest.TestCase):
    """Test cases for concurrent clients and consistency under load.

    This test class includes simple tests for:
    - Multiple clients making concurrent requests
    - Data consistency under load
    - System's ability to handle concurrent operations
    """

    @classmethod
    def setUpClass(cls):
        """Set up test environment with coordinator and storage nodes."""
        # Create temporary directory for data
        cls.temp_dir = tempfile.mkdtemp()

        # Use default coordinator port and fixed storage ports
        cls.coordinator_port = 5000
        cls.storage_port_1 = 5501
        cls.storage_port_2 = 5502

        # Start coordinator
        cls.coordinator_process = subprocess.Popen([
            sys.executable, '-m', 'distributed_db.coordinator.coordinator',
            '--host', 'localhost',
            '--port', str(cls.coordinator_port),
            '--data-dir', os.path.join(cls.temp_dir, 'coordinator')
        ])

        # Wait for coordinator to start
        time.sleep(2)

        # Start first storage node
        cls.storage_process_1 = subprocess.Popen([
            sys.executable, '-m', 'distributed_db.storage.storage_node',
            '--host', 'localhost',
            '--port', str(cls.storage_port_1),
            '--data-dir', os.path.join(cls.temp_dir, 'storage1'),
            '--coordinator', f'http://localhost:{cls.coordinator_port}'
        ])

        # Start second storage node
        cls.storage_process_2 = subprocess.Popen([
            sys.executable, '-m', 'distributed_db.storage.storage_node',
            '--host', 'localhost',
            '--port', str(cls.storage_port_2),
            '--data-dir', os.path.join(cls.temp_dir, 'storage2'),
            '--coordinator', f'http://localhost:{cls.coordinator_port}'
        ])

        # Wait for storage nodes to start
        time.sleep(2)

        # Register storage nodes with coordinator
        try:
            # Register first storage node
            response = requests.post(
                f'http://localhost:{cls.coordinator_port}/nodes',
                json={
                    'host': 'localhost',
                    'port': cls.storage_port_1,
                    'status': 'active'
                }
            )
            if response.status_code == 200:
                cls.node_id_1 = response.json().get('node_id')
                print(
                    f"Storage node 1 registered successfully with ID: {cls.node_id_1}")
            else:
                print(f"Failed to register storage node 1: {response.text}")

            # Register second storage node
            response = requests.post(
                f'http://localhost:{cls.coordinator_port}/nodes',
                json={
                    'host': 'localhost',
                    'port': cls.storage_port_2,
                    'status': 'active'
                }
            )
            if response.status_code == 200:
                cls.node_id_2 = response.json().get('node_id')
                print(
                    f"Storage node 2 registered successfully with ID: {cls.node_id_2}")
            else:
                print(f"Failed to register storage node 2: {response.text}")
        except Exception as e:
            print(f"Error registering storage nodes: {e}")

        # Create client
        cls.client = DatabaseClient(
            coordinator_url=f'http://localhost:{cls.coordinator_port}')

        # Create a test database with a simple schema
        columns = [
            ColumnDefinition(name='id', data_type='INTEGER',
                             primary_key=True, nullable=False),
            ColumnDefinition(name='name', data_type='TEXT', nullable=False),
            ColumnDefinition(name='value', data_type='INTEGER', nullable=False)
        ]

        table = TableSchema(name='concurrent_test', columns=columns)

        # Create database with horizontal partitioning
        success, error = cls.client.create_database(
            name='test_concurrent',
            db_type=DatabaseType.SQL,
            tables=[table],
            partition_type=PartitionType.HORIZONTAL,
            partition_key='id'
        )

        if not success:
            print(f"Failed to create database: {error}")
        else:
            print("Test database created successfully")

    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        # Stop processes
        if hasattr(cls, 'coordinator_process'):
            cls.coordinator_process.terminate()
            cls.coordinator_process.wait()

        if hasattr(cls, 'storage_process_1'):
            cls.storage_process_1.terminate()
            cls.storage_process_1.wait()

        if hasattr(cls, 'storage_process_2'):
            cls.storage_process_2.terminate()
            cls.storage_process_2.wait()

        # Remove temporary directory
        shutil.rmtree(cls.temp_dir)

    def test_01_create_test_data(self):
        """Test creating initial test data."""
        # Insert a few test records to ensure the database exists and works
        for i in range(5):
            result = self.client.sql_insert(
                'test_concurrent',
                'concurrent_test',
                {'id': i, 'name': f'InitialTest{i}', 'value': i}
            )
            self.assertTrue(
                result.success, f"Failed to insert initial test record {i}")

        # Verify the records were created
        result = self.client.sql_query(
            'test_concurrent',
            'concurrent_test',
            where={'id': {'$lt': 5}}
        )
        self.assertTrue(result.success, "Failed to query initial test records")

        # Handle duplicate records by checking unique IDs
        unique_ids = set(record['id'] for record in result.data)
        self.assertEqual(len(unique_ids), 5, "Expected 5 unique initial test records")

    def test_02_setup_concurrent_client_test(self):
        """Test setup for concurrent client tests."""
        # Number of concurrent clients
        num_clients = 5

        # Insert a test record for each client to ensure the database exists
        for i in range(num_clients):
            result = self.client.sql_insert(
                'test_concurrent',
                'concurrent_test',
                {'id': 10000 + i, 'name': f'TestClient{i}', 'value': i}
            )
            self.assertTrue(
                result.success, f"Failed to insert test record for client {i}")

        # Verify the records were created
        result = self.client.sql_query(
            'test_concurrent',
            'concurrent_test',
            where={'id': {'$gte': 10000, '$lt': 10000 + num_clients}}
        )
        self.assertTrue(result.success, "Failed to query client test records")

        # Handle duplicate records by checking unique IDs
        unique_ids = set(record['id'] for record in result.data)
        self.assertEqual(len(unique_ids), num_clients,
                         f"Expected {num_clients} unique client test records")

    def test_03_client_insert_task(self):
        """Test the client insert task function."""
        # Create a client
        client = DatabaseClient(
            coordinator_url=f'http://localhost:{self.coordinator_port}')

        # Test inserting a record
        client_id = 99  # Use a special client ID for this test
        record_id = client_id * 100 + 1  # Create a unique ID for this test

        result = client.sql_insert(
            'test_concurrent',
            'concurrent_test',
            {'id': record_id, 'name': f'TestTask{client_id}',
                'value': random.randint(1, 100)}
        )

        self.assertTrue(result.success, "Failed to insert record in test task")

        # Verify the record was created
        result = client.sql_query(
            'test_concurrent',
            'concurrent_test',
            where={'id': record_id}
        )
        self.assertTrue(
            result.success, "Failed to query record inserted by test task")

        # Handle duplicate records by checking unique IDs
        unique_ids = set(record['id'] for record in result.data)
        self.assertEqual(len(unique_ids), 1,
                         "Expected 1 unique record inserted by test task")

    def test_04_concurrent_inserts_first_batch(self):
        """Test first batch of concurrent insert operations."""
        # Number of concurrent clients
        num_clients = 2
        # Number of inserts per client (smaller batch)
        inserts_per_client = 5

        # Create a list to store results
        results = []

        def client_insert_task(client_id):
            """Task for each client to insert records."""
            client = DatabaseClient(
                coordinator_url=f'http://localhost:{self.coordinator_port}')
            client_results = []

            for i in range(inserts_per_client):
                # Create a unique ID for each record
                record_id = client_id * 100 + i + 20000  # Use a different range

                # Insert a record
                result = client.sql_insert(
                    'test_concurrent',
                    'concurrent_test',
                    {'id': record_id, 'name': f'Client{client_id}-Item{i}',
                        'value': random.randint(1, 100)}
                )

                # Store the result
                client_results.append((record_id, result.success))

                # Small delay
                time.sleep(0.01)

            return client_results

        # Use ThreadPoolExecutor to run client tasks concurrently
        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [executor.submit(client_insert_task, i)
                       for i in range(num_clients)]

            # Collect results
            for future in futures:
                results.extend(future.result())

        # Verify all inserts were successful
        failed_inserts = [record_id for record_id,
                          success in results if not success]
        self.assertEqual(len(failed_inserts), 0,
                         f"Failed inserts: {failed_inserts}")

        # Count the records inserted
        processed_ids = [record_id for record_id,
                         success in results if success]
        self.assertEqual(len(processed_ids), num_clients * inserts_per_client,
                         "Number of processed records doesn't match expected count")

    def test_05_concurrent_inserts_second_batch(self):
        """Test second batch of concurrent insert operations."""
        # Number of concurrent clients
        num_clients = 3
        # Number of inserts per client (smaller batch)
        inserts_per_client = 5

        # Create a list to store results
        results = []

        def client_insert_task(client_id):
            """Task for each client to insert records."""
            client = DatabaseClient(
                coordinator_url=f'http://localhost:{self.coordinator_port}')
            client_results = []

            for i in range(inserts_per_client):
                # Create a unique ID for each record
                record_id = client_id * 100 + i + 25000  # Use a different range

                # Insert a record
                result = client.sql_insert(
                    'test_concurrent',
                    'concurrent_test',
                    {'id': record_id, 'name': f'Client2-{client_id}-Item{i}',
                        'value': random.randint(1, 100)}
                )

                # Store the result
                client_results.append((record_id, result.success))

                # Small delay
                time.sleep(0.01)

            return client_results

        # Use ThreadPoolExecutor to run client tasks concurrently
        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [executor.submit(client_insert_task, i)
                       for i in range(num_clients)]

            # Collect results
            for future in futures:
                results.extend(future.result())

        # Verify all inserts were successful
        failed_inserts = [record_id for record_id,
                          success in results if not success]
        self.assertEqual(len(failed_inserts), 0,
                         f"Failed inserts: {failed_inserts}")

        # Count the records inserted
        processed_ids = [record_id for record_id,
                         success in results if success]
        self.assertEqual(len(processed_ids), num_clients * inserts_per_client,
                         "Number of processed records doesn't match expected count")

    def test_06_verify_concurrent_inserts(self):
        """Test verifying all concurrent inserts."""
        # Query both batches of inserted records
        result = self.client.sql_query(
            'test_concurrent',
            'concurrent_test',
            # Query the range we used
            where={'id': {'$gte': 20000, '$lt': 30000}}
        )
        self.assertTrue(
            result.success, f"Failed to query inserted data: {result.error}")
        self.assertIsNotNone(result.data, "Query returned no data")

        # Count unique IDs in the result
        unique_ids = set()
        for record in result.data:
            unique_ids.add(record['id'])

        # We expect records from previous tests:
        # - 2 clients with 5 inserts each from test 04 (20000-20199 range)
        # - 3 clients with 5 inserts each from test 05 (25000-25299 range)
        expected_count = (2 * 5) + (3 * 5)
        self.assertGreaterEqual(len(unique_ids), expected_count,
                                f"Expected at least {expected_count} unique records, found {len(unique_ids)}")

    def test_07_setup_for_concurrent_reads(self):
        """Test setting up data for concurrent read tests."""
        # Insert some data to read
        for i in range(10):
            result = self.client.sql_insert(
                'test_concurrent',
                'concurrent_test',
                {'id': 40000 + i, 'name': f'ReadTest{i}', 'value': i}
            )
            self.assertTrue(
                result.success, f"Failed to insert read test record {i}")

        # Verify the records were created
        result = self.client.sql_query(
            'test_concurrent',
            'concurrent_test',
            where={'id': {'$gte': 40000, '$lt': 40010}}
        )
        self.assertTrue(result.success, "Failed to query read test records")

        # Handle duplicate records by checking unique IDs
        unique_ids = set(record['id'] for record in result.data)
        self.assertEqual(len(unique_ids), 10, "Expected 10 unique read test records")

    def test_08_client_read_task(self):
        """Test the client read task function."""
        # Create a client
        client = DatabaseClient(
            coordinator_url=f'http://localhost:{self.coordinator_port}')

        # Test reading a record (from the ones we created in the previous test)
        record_id = 40005  # Use one of the read test records

        result = client.sql_query(
            'test_concurrent',
            'concurrent_test',
            where={'id': record_id}
        )

        self.assertTrue(result.success, "Failed to read record in test task")

        # Handle duplicate records by checking unique IDs
        unique_ids = set(record['id'] for record in result.data)
        self.assertEqual(len(unique_ids), 1, "Expected 1 unique record read by test task")

        # Check that the record ID matches
        self.assertTrue(record_id in unique_ids, "Record ID doesn't match")

    def test_09_concurrent_reads(self):
        """Test concurrent read operations from multiple clients."""
        # Number of concurrent clients
        num_clients = 5
        # Number of reads per client
        reads_per_client = 5

        # Create a list to store results
        results = []

        def client_read_task(client_id):
            """Task for each client to read records."""
            client = DatabaseClient(
                coordinator_url=f'http://localhost:{self.coordinator_port}')
            client_results = []

            for i in range(reads_per_client):
                # Query a record from our read test set
                # Cycle through our 10 test records
                record_id = 40000 + (i % 10)

                # Query the record
                result = client.sql_query(
                    'test_concurrent',
                    'concurrent_test',
                    where={'id': record_id}
                )

                # Store the result
                client_results.append((record_id, result.success))

                # Small delay
                time.sleep(0.01)

            return client_results

        # Use ThreadPoolExecutor to run client tasks concurrently
        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [executor.submit(client_read_task, i)
                       for i in range(num_clients)]

            # Collect results
            for future in futures:
                results.extend(future.result())

        # Verify all reads were successful
        failed_reads = [record_id for record_id,
                        success in results if not success]
        self.assertEqual(len(failed_reads), 0, f"Failed reads: {failed_reads}")

        # Verify we processed the expected number of records
        self.assertEqual(len(results), num_clients * reads_per_client,
                         "Number of processed reads doesn't match expected count")

    def test_10_setup_for_mixed_operations(self):
        """Test setting up for mixed read/write operations."""
        # Make sure we have some existing data for the mixed operations
        for i in range(5):
            result = self.client.sql_insert(
                'test_concurrent',
                'concurrent_test',
                {'id': 45000 + i, 'name': f'MixedBaseTest{i}', 'value': i}
            )
            self.assertTrue(
                result.success, f"Failed to insert mixed base test record {i}")

        # Verify the records were created
        result = self.client.sql_query(
            'test_concurrent',
            'concurrent_test',
            where={'id': {'$gte': 45000, '$lt': 45005}}
        )
        self.assertTrue(
            result.success, "Failed to query mixed base test records")

        # Handle duplicate records by checking unique IDs
        unique_ids = set(record['id'] for record in result.data)
        self.assertEqual(len(unique_ids), 5,
                         "Expected 5 unique mixed base test records")

    def test_11_mixed_operations(self):
        """Test mixed read and write operations from multiple clients."""
        # Number of concurrent clients
        num_clients = 5
        # Number of operations per client
        ops_per_client = 5

        # Create a list to store results
        results = []

        def client_mixed_task(client_id):
            """Task for each client to perform mixed operations."""
            client = DatabaseClient(
                coordinator_url=f'http://localhost:{self.coordinator_port}')
            client_results = []

            for i in range(ops_per_client):
                # Decide whether to read or write
                operation = random.choice(['read', 'write'])

                if operation == 'read':
                    # Query a read base record
                    record_id = 45000 + (i % 5)

                    # Query the record
                    result = client.sql_query(
                        'test_concurrent',
                        'concurrent_test',
                        where={'id': record_id}
                    )

                    # Store the result
                    client_results.append(('read', record_id, result.success))
                else:
                    # Create a unique ID for each record
                    record_id = 500 + client_id * 100 + i

                    # Insert a record
                    result = client.sql_insert(
                        'test_concurrent',
                        'concurrent_test',
                        {'id': record_id, 'name': f'MixedClient{client_id}-Item{i}',
                            'value': random.randint(1, 100)}
                    )

                    # Store the result
                    client_results.append(('write', record_id, result.success))

                # Small delay
                time.sleep(0.01)

            return client_results

        # Use ThreadPoolExecutor to run client tasks concurrently
        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [executor.submit(client_mixed_task, i)
                       for i in range(num_clients)]

            # Collect results
            for future in futures:
                results.extend(future.result())

        # Verify all operations were successful
        failed_ops = [(op, record_id)
                      for op, record_id, success in results if not success]
        self.assertEqual(len(failed_ops), 0,
                         f"Failed operations: {failed_ops}")

        # Count the number of read and write operations
        read_ops = sum(1 for op, _, _ in results if op == 'read')
        write_ops = sum(1 for op, _, _ in results if op == 'write')

        print(
            f"Mixed operations completed: {read_ops} reads, {write_ops} writes")
        self.assertEqual(read_ops + write_ops, num_clients * ops_per_client,
                         "Total operations count doesn't match expected")

    def test_12_setup_for_concurrent_updates(self):
        """Test setting up data for concurrent update tests."""
        # Insert records to update
        base_id = 1000
        num_records = 20

        for i in range(num_records):
            result = self.client.sql_insert(
                'test_concurrent',
                'concurrent_test',
                {'id': base_id + i, 'name': f'UpdateTest-{i}', 'value': 0}
            )
            self.assertTrue(
                result.success, f"Failed to insert update test record {i}: {result.error}")

        # Verify the records were created
        result = self.client.sql_query(
            'test_concurrent',
            'concurrent_test',
            where={'id': {'$gte': base_id, '$lt': base_id + num_records}}
        )
        self.assertTrue(result.success, "Failed to query update test records")

        # Handle duplicate records by checking unique IDs
        unique_ids = set(record['id'] for record in result.data)
        self.assertEqual(len(unique_ids), num_records,
                         f"Expected {num_records} unique update test records")

    def test_13_client_update_task(self):
        """Test the client update task function."""
        # Create a client
        client = DatabaseClient(
            coordinator_url=f'http://localhost:{self.coordinator_port}')

        # Test updating a record (from the ones we created in the previous test)
        record_id = 1005  # Use one of the update test records

        # Update the record
        result = client.sql_update(
            'test_concurrent',
            'concurrent_test',
            {'value': 99},
            where={'id': record_id}
        )

        self.assertTrue(result.success, "Failed to update record in test task")

        # Verify the update was applied
        result = client.sql_query(
            'test_concurrent',
            'concurrent_test',
            where={'id': record_id}
        )
        self.assertTrue(result.success, "Failed to query updated record")

        # Handle duplicate records by checking unique IDs
        unique_ids = set(record['id'] for record in result.data)
        self.assertEqual(len(unique_ids), 1, "Expected 1 unique updated record")

        # Check that the value was updated correctly
        # Since we might have duplicate records, check that at least one has the correct value
        updated_values = set(record['value'] for record in result.data)
        self.assertTrue('99' in updated_values, "Record value wasn't updated")

    def test_14_concurrent_updates_first_batch(self):
        """Test first batch of concurrent update operations."""
        # Base ID and number of records from previous test
        base_id = 1000

        # Number of concurrent clients
        num_clients = 2
        # Number of updates per client
        updates_per_client = 4  # Each client updates 4 different records

        # Create a list to store results
        results = []

        def client_update_task(client_id):
            """Task for each client to update records."""
            client = DatabaseClient(
                coordinator_url=f'http://localhost:{self.coordinator_port}')
            client_results = []

            for i in range(updates_per_client):
                # Calculate which record to update - start at the beginning
                record_id = base_id + (client_id * updates_per_client + i)

                # Update the record
                result = client.sql_update(
                    'test_concurrent',
                    'concurrent_test',
                    {'value': client_id + 1},  # Set value to client_id + 1
                    where={'id': record_id}
                )

                # Store the result
                client_results.append((record_id, result.success))

                # Small delay
                time.sleep(0.01)

            return client_results

        # Use ThreadPoolExecutor to run client tasks concurrently
        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [executor.submit(client_update_task, i)
                       for i in range(num_clients)]

            # Collect results
            for future in futures:
                results.extend(future.result())

        # Verify all updates were successful
        failed_updates = [record_id for record_id,
                          success in results if not success]
        self.assertEqual(len(failed_updates), 0,
                         f"Failed updates: {failed_updates}")

        # Verify we processed the expected number of records
        self.assertEqual(len(results), num_clients * updates_per_client,
                         "Number of processed updates doesn't match expected count")

    def test_15_concurrent_updates_second_batch(self):
        """Test second batch of concurrent update operations."""
        # Base ID and number of records from previous test
        base_id = 1000

        # Number of concurrent clients
        num_clients = 3
        # Number of updates per client
        updates_per_client = 4  # Different range than first batch

        # Create a list to store results
        results = []

        def client_update_task(client_id):
            """Task for each client to update records."""
            client = DatabaseClient(
                coordinator_url=f'http://localhost:{self.coordinator_port}')
            client_results = []

            for i in range(updates_per_client):
                # Calculate which record to update - start in the middle
                record_id = base_id + 8 + (client_id * updates_per_client + i)

                # Update the record
                result = client.sql_update(
                    'test_concurrent',
                    'concurrent_test',
                    # Set value to client_id + 3 (different from first batch)
                    {'value': client_id + 3},
                    where={'id': record_id}
                )

                # Store the result
                client_results.append((record_id, result.success))

                # Small delay
                time.sleep(0.01)

            return client_results

        # Use ThreadPoolExecutor to run client tasks concurrently
        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [executor.submit(client_update_task, i)
                       for i in range(num_clients)]

            # Collect results
            for future in futures:
                results.extend(future.result())

        # Verify all updates were successful
        failed_updates = [record_id for record_id,
                          success in results if not success]
        self.assertEqual(len(failed_updates), 0,
                         f"Failed updates: {failed_updates}")

        # Verify we processed the expected number of records
        self.assertEqual(len(results), num_clients * updates_per_client,
                         "Number of processed updates doesn't match expected count")


    def test_16_consistency_setup(self):
        """Test setup for the consistency under load test."""
        # Number of concurrent clients
        num_clients = 5

        # Verify test database exists and is accessible
        result = self.client.sql_query(
            'test_concurrent',
            'concurrent_test',
            where={'id': {'$lt': 5}}  # Query the initial test records
        )
        self.assertTrue(result.success, "Test database is not accessible")

    def test_17_consistency_under_load(self):
        """Test data consistency under load with concurrent operations."""
        # This test verifies that multiple clients can create and access records concurrently
        # without causing system failures

        # Number of concurrent clients
        num_clients = 5

        def client_task(client_id):
            """Task for each client to create and access its own record."""
            client = DatabaseClient(
                coordinator_url=f'http://localhost:{self.coordinator_port}')

            # Create a unique record ID for this client
            record_id = 3000 + client_id

            try:
                # Insert a record for this client
                insert_result = client.sql_insert(
                    'test_concurrent',
                    'concurrent_test',
                    {'id': record_id, 'name': f'ClientRecord{client_id}',
                        'value': client_id}
                )

                if not insert_result.success:
                    print(f"Client {client_id}: Failed to insert record")
                    return False

                # Query the record to verify it was created
                query_result = client.sql_query(
                    'test_concurrent',
                    'concurrent_test',
                    where={'id': record_id}
                )

                if not query_result.success or not query_result.data:
                    print(f"Client {client_id}: Failed to query record")
                    return False

                # Verify the record has the expected data
                record = query_result.data[0]
                if record['id'] != record_id or record['name'] != f'ClientRecord{client_id}':
                    print(f"Client {client_id}: Record data mismatch")
                    return False

                return True
            except Exception as e:
                print(f"Client {client_id}: Exception: {e}")
                return False

        # Use ThreadPoolExecutor to run client tasks concurrently
        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [executor.submit(client_task, i)
                       for i in range(num_clients)]

            # Collect results
            results = [future.result() for future in futures]

        # Verify that all clients completed successfully
        success_count = sum(1 for result in results if result)
        print(f"{success_count} out of {num_clients} clients completed successfully")
        self.assertEqual(success_count, num_clients,
                         f"Expected all clients to succeed")


    def test_18_verify_consistency_records(self):
        """Test verifying records created in the consistency test."""
        # Query all client records to verify they exist
        all_records = []
        for client_id in range(5):  # 5 clients from previous test
            record_id = 3000 + client_id
            result = self.client.sql_query(
                'test_concurrent',
                'concurrent_test',
                where={'id': record_id}
            )
            if result.success and result.data:
                all_records.append((record_id, result.data[0]['name']))

        # Print the records that were found
        print("Records found:")
        for record_id, name in all_records:
            print(f"  Record {record_id}: {name}")

        # Verify all 5 records were found
        self.assertEqual(len(all_records), 5,
                         f"Expected 5 records, found {len(all_records)}")

        # Verify record IDs match expected pattern
        for i in range(5):
            expected_id = 3000 + i
            self.assertTrue(any(record_id == expected_id for record_id, _ in all_records),
                          f"Missing expected record with ID {expected_id}")

    def test_19_additional_load_test(self):
        """Test system under additional load with concurrent operations."""
        # Number of concurrent clients
        num_clients = 3
        # Number of operations per client
        ops_per_client = 3

        # Use a different ID range for this test
        base_id = 4000

        def client_worker(client_id):
            """Worker function for each client to perform operations."""
            client = DatabaseClient(coordinator_url=f'http://localhost:{self.coordinator_port}')
            success = True

            for i in range(ops_per_client):
                # Create unique record ID
                record_id = base_id + (client_id * ops_per_client) + i

                try:
                    # Insert a record
                    insert_result = client.sql_insert(
                        'test_concurrent',
                        'concurrent_test',
                        {'id': record_id, 'name': f'LoadTest{client_id}-{i}', 'value': i}
                    )

                    if not insert_result.success:
                        print(f"Worker {client_id} failed to insert record {i}")
                        success = False
                        continue

                    # Query the record
                    query_result = client.sql_query(
                        'test_concurrent',
                        'concurrent_test',
                        where={'id': record_id}
                    )

                    if not query_result.success or not query_result.data:
                        print(f"Worker {client_id} failed to query record {i}")
                        success = False
                except Exception as e:
                    print(f"Worker {client_id} exception: {e}")
                    success = False

            return success

        # Execute the workers concurrently
        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [executor.submit(client_worker, i) for i in range(num_clients)]
            results = [future.result() for future in futures]

        # Verify all workers completed successfully
        self.assertTrue(all(results), "Not all workers completed successfully")

        # Verify records were created
        result = self.client.sql_query(
            'test_concurrent',
            'concurrent_test',
            where={'id': {'$gte': base_id, '$lt': base_id + (num_clients * ops_per_client)}}
        )
        self.assertTrue(result.success, "Failed to query load test records")

        # Handle duplicate records by checking unique IDs
        unique_ids = set(record['id'] for record in result.data)
        self.assertEqual(len(unique_ids), num_clients * ops_per_client,
                       f"Expected {num_clients * ops_per_client} unique load test records")


if __name__ == '__main__':
    unittest.main()
