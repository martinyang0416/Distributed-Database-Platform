"""
Tests for horizontal partitioning in the distributed database.
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

# Add parent directory to path
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))


class TestHorizontalPartitioning(unittest.TestCase):
    """Test cases for horizontal partitioning.

    This test class includes simple tests for:
    - Creating databases with horizontal partitioning
    - Verifying partition metadata
    - Basic operations on horizontally partitioned databases
    """

    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        # Create temporary directory for data
        cls.temp_dir = tempfile.mkdtemp()

        # Use default coordinator port and a fixed storage port
        cls.coordinator_port = 5000
        cls.storage_port = 5501

        # Start coordinator
        cls.coordinator_process = subprocess.Popen([
            sys.executable, '-m', 'distributed_db.coordinator.coordinator',
            '--host', 'localhost',
            '--port', str(cls.coordinator_port),
            '--data-dir', os.path.join(cls.temp_dir, 'coordinator')
        ])

        # Wait for coordinator to start
        time.sleep(2)

        # Start storage node
        cls.storage_process = subprocess.Popen([
            sys.executable, '-m', 'distributed_db.storage.storage_node',
            '--host', 'localhost',
            '--port', str(cls.storage_port),
            '--data-dir', os.path.join(cls.temp_dir, 'storage'),
            '--coordinator', f'http://localhost:{cls.coordinator_port}'
        ])

        # Wait for storage node to start
        time.sleep(2)

        # Register storage node with coordinator
        try:
            response = requests.post(
                f'http://localhost:{cls.coordinator_port}/nodes',
                json={
                    'host': 'localhost',
                    'port': cls.storage_port,
                    'status': 'active'
                }
            )
            if response.status_code == 200:
                print("Storage node registered successfully")
            else:
                print(f"Failed to register storage node: {response.text}")
        except Exception as e:
            print(f"Error registering storage node: {e}")

        # Create client
        cls.client = DatabaseClient(
            coordinator_url=f'http://localhost:{cls.coordinator_port}')

    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        # Stop processes
        if hasattr(cls, 'coordinator_process'):
            cls.coordinator_process.terminate()
            cls.coordinator_process.wait()

        if hasattr(cls, 'storage_process'):
            cls.storage_process.terminate()
            cls.storage_process.wait()

        # Remove temporary directory
        shutil.rmtree(cls.temp_dir)

    def test_01_define_table_schema_for_horizontal_partitioning(self):
        """Test defining a table schema suitable for horizontal partitioning."""
        # Define columns for a simple user table
        columns = [
            ColumnDefinition(name='id', data_type='INTEGER',
                             primary_key=True, nullable=False),
            ColumnDefinition(name='name', data_type='TEXT', nullable=False),
            ColumnDefinition(name='region', data_type='TEXT',
                             nullable=False)  # Partition key
        ]

        table = TableSchema(name='users', columns=columns, indexes=['region'])

        # Verify the schema
        self.assertEqual(table.name, 'users')
        self.assertEqual(len(table.columns), 3)
        self.assertIn('region', table.indexes)
        self.assertEqual(table.columns[0].name, 'id')
        self.assertEqual(table.columns[0].data_type, 'INTEGER')
        self.assertTrue(table.columns[0].primary_key)

    def test_02_create_horizontal_partitioned_database(self):
        """Test creating a database with horizontal partitioning."""
        # Define columns for a simple user table
        columns = [
            ColumnDefinition(name='id', data_type='INTEGER',
                             primary_key=True, nullable=False),
            ColumnDefinition(name='name', data_type='TEXT', nullable=False),
            ColumnDefinition(name='region', data_type='TEXT',
                             nullable=False)  # Partition key
        ]

        table = TableSchema(name='users', columns=columns, indexes=['region'])

        # Create database with horizontal partitioning by region
        success, error = self.client.create_database(
            name='test_horizontal',
            db_type=DatabaseType.SQL,
            tables=[table],
            partition_type=PartitionType.HORIZONTAL,
            partition_key='region'
        )

        self.assertTrue(
            success, f"Failed to create horizontally partitioned database: {error}")

    def test_03_verify_horizontal_database_exists(self):
        """Test verifying the database exists after creation."""
        # Verify database exists
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/databases')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])
        self.assertIn('test_horizontal', data['databases'])

    def test_04_verify_horizontal_partition_type(self):
        """Test verifying the partition type and key for horizontal database."""
        # Verify partition information
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/databases/test_horizontal')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])
        self.assertEqual(data['database']['partition_type'], 'HORIZONTAL')
        self.assertEqual(data['database']['partition_key'], 'region')

    def test_05_check_horizontal_partitions(self):
        """Test getting partition information for horizontal database."""
        # Wait for partitions to be created
        time.sleep(1)

        # Get partition information
        partitions = self.client.query_router.get_database_partitions(
            'test_horizontal')
        print(f"Horizontal partitions: {partitions}")

        # Verify we got some partitions back (even if empty list)
        self.assertIsNotNone(partitions)

    def test_06_insert_data_north_region(self):
        """Test inserting data into the north region partition."""
        # Insert data for north region
        result = self.client.sql_insert(
            'test_horizontal',
            'users',
            {'id': 1, 'name': 'User1', 'region': 'north'}
        )

        # Assert that the insert was successful
        self.assertTrue(
            result.success, f"SQL insert for north region failed: {result.error}")
        print(f"Inserted user with north region: {result.data}")

    def test_07_insert_data_south_region(self):
        """Test inserting data into the south region partition."""
        # Insert data for south region
        result = self.client.sql_insert(
            'test_horizontal',
            'users',
            {'id': 2, 'name': 'User2', 'region': 'south'}
        )

        # Assert that the insert was successful
        self.assertTrue(
            result.success, f"SQL insert for south region failed: {result.error}")
        print(f"Inserted user with south region: {result.data}")

    def test_08_insert_data_east_region(self):
        """Test inserting data into the east region partition."""
        # Insert data for east region
        result = self.client.sql_insert(
            'test_horizontal',
            'users',
            {'id': 3, 'name': 'User3', 'region': 'east'}
        )

        # Assert that the insert was successful
        self.assertTrue(
            result.success, f"SQL insert for east region failed: {result.error}")
        print(f"Inserted user with east region: {result.data}")

    def test_09_insert_data_west_region(self):
        """Test inserting data into the west region partition."""
        # Insert data for west region
        result = self.client.sql_insert(
            'test_horizontal',
            'users',
            {'id': 4, 'name': 'User4', 'region': 'west'}
        )

        # Assert that the insert was successful
        self.assertTrue(
            result.success, f"SQL insert for west region failed: {result.error}")
        print(f"Inserted user with west region: {result.data}")

    def test_10_query_all_data_from_horizontal_partitions(self):
        """Test querying all data across all horizontal partitions."""
        # Query all users to verify data was inserted
        all_users = self.client.sql_query(
            'test_horizontal',
            'users'
        )
        self.assertTrue(all_users.success,
                        f"SQL query for all users failed: {all_users.error}")
        self.assertIsNotNone(all_users.data, "SQL query returned no data")
        self.assertTrue(len(all_users.data) > 0, "Expected at least one user")
        print(f"All users: {all_users.data}")

        # Verify we have all the expected regions in the results
        regions = set(user['region'] for user in all_users.data)
        self.assertIn('north', regions)
        self.assertIn('south', regions)
        self.assertIn('east', regions)
        self.assertIn('west', regions)

    def test_11_query_north_region_partition(self):
        """Test querying data from a specific partition (north region)."""
        # Query with partition key filter for north region
        result = self.client.sql_query(
            'test_horizontal',
            'users',
            where={'region': 'north'}
        )

        # Print debug info
        print(f"Query result for north region: {result.data}")
        print(f"Query success: {result.success}, error: {result.error}")

        # Assert that the query was successful
        self.assertTrue(
            result.success, f"SQL query with partition key filter failed: {result.error}")
        self.assertTrue(len(result.data) > 0,
                        "Expected at least one user in north region")

        # Verify data
        for user in result.data:
            self.assertEqual(user['region'], 'north',
                             "Expected only north region users")

    def test_12_query_south_region_partition(self):
        """Test querying data from a specific partition (south region)."""
        # Query with partition key filter for south region
        result = self.client.sql_query(
            'test_horizontal',
            'users',
            where={'region': 'south'}
        )

        # Assert that the query was successful
        self.assertTrue(
            result.success, f"SQL query with partition key filter failed: {result.error}")
        self.assertTrue(len(result.data) > 0,
                        "Expected at least one user in south region")

        # Verify data
        for user in result.data:
            self.assertEqual(user['region'], 'south',
                             "Expected only south region users")

    def test_13_define_mixed_partition_schema(self):
        """Test defining a table schema for mixed partitioning."""
        # Define columns for an order table that could benefit from mixed partitioning
        columns = [
            ColumnDefinition(name='id', data_type='INTEGER',
                             primary_key=True, nullable=False),
            ColumnDefinition(name='customer_id',
                             data_type='INTEGER', nullable=False),
            ColumnDefinition(
                name='order_date', data_type='TIMESTAMP', default='CURRENT_TIMESTAMP'),
            ColumnDefinition(name='status', data_type='TEXT', nullable=False),
            ColumnDefinition(name='total_amount',
                             data_type='REAL', nullable=False),
            ColumnDefinition(name='shipping_address',
                             data_type='TEXT', nullable=False),
            ColumnDefinition(name='billing_address',
                             data_type='TEXT', nullable=False),
            ColumnDefinition(name='payment_method',
                             data_type='TEXT', nullable=False)
        ]

        table = TableSchema(name='orders', columns=columns,
                            indexes=['customer_id', 'order_date'])

        # Verify the schema
        self.assertEqual(table.name, 'orders')
        self.assertEqual(len(table.columns), 8)
        self.assertIn('customer_id', table.indexes)
        self.assertIn('order_date', table.indexes)

    def test_14_create_mixed_partitioned_database(self):
        """Test creating a database with mixed partitioning."""
        # Define columns for an order table that could benefit from mixed partitioning
        columns = [
            ColumnDefinition(name='id', data_type='INTEGER',
                             primary_key=True, nullable=False),
            ColumnDefinition(name='customer_id',
                             data_type='INTEGER', nullable=False),
            ColumnDefinition(
                name='order_date', data_type='TIMESTAMP', default='CURRENT_TIMESTAMP'),
            ColumnDefinition(name='status', data_type='TEXT', nullable=False),
            ColumnDefinition(name='total_amount',
                             data_type='REAL', nullable=False),
            ColumnDefinition(name='shipping_address',
                             data_type='TEXT', nullable=False),
            ColumnDefinition(name='billing_address',
                             data_type='TEXT', nullable=False),
            ColumnDefinition(name='payment_method',
                             data_type='TEXT', nullable=False)
        ]

        table = TableSchema(name='orders', columns=columns,
                            indexes=['customer_id', 'order_date'])

        # Create database with mixed partitioning
        success, error = self.client.create_database(
            name='test_mixed',
            db_type=DatabaseType.SQL,
            tables=[table],
            partition_type=PartitionType.MIXED,
            partition_key='customer_id'  # Horizontal partition key
        )

        self.assertTrue(
            success, f"Failed to create mixed partitioned database: {error}")

    def test_15_verify_mixed_database_exists(self):
        """Test verifying the mixed partitioned database exists."""
        # Verify database exists
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/databases')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])
        self.assertIn('test_mixed', data['databases'])

    def test_16_verify_mixed_partition_type(self):
        """Test verifying the partition type and key for mixed database."""
        # Verify partition information
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/databases/test_mixed')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])
        self.assertEqual(data['database']['partition_type'], 'MIXED')
        self.assertEqual(data['database']['partition_key'], 'customer_id')

    def test_17_check_mixed_partitions(self):
        """Test getting partition information for mixed database."""
        # Wait for partitions to be created
        time.sleep(1)

        # Get partition information
        partitions = self.client.query_router.get_database_partitions(
            'test_mixed')
        print(f"Mixed partitions: {partitions}")

        # Verify we got some partitions back (even if empty list)
        self.assertIsNotNone(partitions)

    def test_18_compare_partition_counts(self):
        """Test comparing partition counts between horizontal and mixed databases."""
        # Get partition information for horizontal and mixed databases
        horizontal_partitions = self.client.query_router.get_database_partitions(
            'test_horizontal')
        mixed_partitions = self.client.query_router.get_database_partitions(
            'test_mixed')

        print(f"Horizontal partitions: {len(horizontal_partitions)}")
        print(f"Mixed partitions: {len(mixed_partitions)}")

        # Assert that we have at least one partition for each database
        self.assertTrue(len(horizontal_partitions) > 0,
                        "Expected at least one horizontal partition")
        self.assertTrue(len(mixed_partitions) > 0,
                        "Expected at least one mixed partition")

    def test_19_verify_horizontal_partition_metadata(self):
        """Test verifying metadata for horizontal partitions."""
        # Verify partition types if partitions are available
        horizontal_partitions = self.client.query_router.get_database_partitions(
            'test_horizontal')

        for partition in horizontal_partitions:
            if 'partition_type' in partition:
                self.assertEqual(
                    partition['partition_type'], 'HORIZONTAL', "Expected horizontal partition type")
            if 'partition_key' in partition:
                self.assertEqual(
                    partition['partition_key'], 'region', "Expected correct partition key")

    def test_20_verify_mixed_partition_metadata(self):
        """Test verifying metadata for mixed partitions."""
        # Verify partition types if partitions are available
        mixed_partitions = self.client.query_router.get_database_partitions(
            'test_mixed')

        for partition in mixed_partitions:
            if 'partition_type' in partition:
                self.assertEqual(
                    partition['partition_type'], 'MIXED', "Expected mixed partition type")
            if 'partition_key' in partition:
                self.assertEqual(
                    partition['partition_key'], 'customer_id', "Expected correct partition key")


if __name__ == '__main__':
    unittest.main()
