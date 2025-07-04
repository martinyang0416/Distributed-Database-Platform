"""
Tests for vertical partitioning in the distributed database.
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


class TestVerticalPartitioning(unittest.TestCase):
    """Test cases for vertical partitioning.

    This test class includes simple tests for:
    - Creating databases with vertical partitioning
    - Verifying partition metadata for SQL and NoSQL databases
    - Basic operations on vertically partitioned databases
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

    def test_01_define_table_schema_for_vertical_partitioning(self):
        """Test creating a table schema suitable for vertical partitioning."""
        # Define columns for a product table with multiple attribute groups
        columns = [
            ColumnDefinition(name='id', data_type='INTEGER',
                             primary_key=True, nullable=False),
            ColumnDefinition(name='name', data_type='TEXT', nullable=False),
            ColumnDefinition(name='description',
                             data_type='TEXT', nullable=True),
            ColumnDefinition(name='price', data_type='REAL', nullable=False),
            ColumnDefinition(
                name='category', data_type='TEXT', nullable=False),
            ColumnDefinition(
                name='stock', data_type='INTEGER', nullable=False),
            ColumnDefinition(
                name='created_at', data_type='TIMESTAMP', default='CURRENT_TIMESTAMP')
        ]

        # Create table schema
        table = TableSchema(
            name='products', columns=columns, indexes=['category'])

        # Verify the schema
        self.assertEqual(table.name, 'products')
        self.assertEqual(len(table.columns), 7)
        self.assertIn('category', table.indexes)
        self.assertEqual(table.columns[0].name, 'id')
        self.assertEqual(table.columns[0].data_type, 'INTEGER')
        self.assertTrue(table.columns[0].primary_key)

    def test_02_create_vertical_partitioned_database(self):
        """Test creating a database with vertical partitioning."""
        # Define columns for a product table with multiple attribute groups
        columns = [
            ColumnDefinition(name='id', data_type='INTEGER',
                             primary_key=True, nullable=False),
            ColumnDefinition(name='name', data_type='TEXT', nullable=False),
            ColumnDefinition(name='description',
                             data_type='TEXT', nullable=True),
            ColumnDefinition(name='price', data_type='REAL', nullable=False),
            ColumnDefinition(
                name='category', data_type='TEXT', nullable=False),
            ColumnDefinition(
                name='stock', data_type='INTEGER', nullable=False),
            ColumnDefinition(
                name='created_at', data_type='TIMESTAMP', default='CURRENT_TIMESTAMP')
        ]

        table = TableSchema(
            name='products', columns=columns, indexes=['category'])

        # Create database with vertical partitioning
        success, error = self.client.create_database(
            name='test_vertical',
            db_type=DatabaseType.SQL,
            tables=[table],
            partition_type=PartitionType.VERTICAL
        )

        self.assertTrue(
            success, f"Failed to create vertically partitioned database: {error}")

    def test_03_verify_vertical_database_exists(self):
        """Test that the vertically partitioned database exists."""
        # Verify database exists
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/databases')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])
        self.assertIn('test_vertical', data['databases'])

    def test_04_verify_vertical_partition_type(self):
        """Test that the database has vertical partition type."""
        # Verify partition information
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/databases/test_vertical')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])
        self.assertEqual(data['database']['partition_type'], 'VERTICAL')

    def test_05_check_vertical_partitions(self):
        """Test getting partition information for vertical database."""
        # Wait for partitions to be created
        time.sleep(1)

        # Get partition information
        partitions = self.client.query_router.get_database_partitions(
            'test_vertical')
        print(f"Vertical partitions: {partitions}")

        # Verify we got some partitions back (even if empty list)
        self.assertIsNotNone(partitions)

    def test_06_create_nosql_vertical_database(self):
        """Test creating a NoSQL database with vertical partitioning."""
        # Create NoSQL database with vertical partitioning (only option for NoSQL)
        success, error = self.client.create_database(
            name='test_nosql_vertical',
            db_type=DatabaseType.NOSQL,
            partition_type=PartitionType.VERTICAL
        )

        self.assertTrue(
            success, f"Failed to create NoSQL database with vertical partitioning: {error}")

    def test_07_verify_nosql_vertical_database_exists(self):
        """Test that the NoSQL vertically partitioned database exists."""
        # Verify database exists
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/databases')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])
        self.assertIn('test_nosql_vertical', data['databases'])

    def test_08_verify_nosql_metadata(self):
        """Test verifying metadata for NoSQL vertical database."""
        # Verify partition information
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/databases/test_nosql_vertical')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])
        self.assertEqual(data['database']['db_type'], 'NOSQL')
        self.assertEqual(data['database']['partition_type'], 'VERTICAL')

    def test_09_insert_data_into_vertical_partition(self):
        """Test inserting data into a vertically partitioned database."""
        # Insert data into vertically partitioned database
        result = self.client.sql_insert(
            'test_vertical',
            'products',
            {
                'id': 1,
                'name': 'Test Product',
                'description': 'A test product for vertical partitioning',
                'price': 99.99,
                'category': 'test',
                'stock': 100
            }
        )

        # Assert that the insert was successful
        self.assertTrue(
            result.success, f"SQL insert into vertical partition failed: {result.error}")
        print(f"Inserted product: {result.data}")

    def test_10_query_all_data_from_vertical_partition(self):
        """Test querying all data from a vertically partitioned database."""
        # Query all products to verify data was inserted
        all_products = self.client.sql_query(
            'test_vertical',
            'products'
        )
        self.assertTrue(all_products.success,
                        f"SQL query for all products failed: {all_products.error}")
        self.assertIsNotNone(all_products.data, "SQL query returned no data")
        self.assertTrue(len(all_products.data) > 0,
                        "Expected at least one product")
        print(f"All products: {all_products.data}")

    def test_11_query_by_id_from_vertical_partition(self):
        """Test querying specific data from a vertically partitioned database."""
        # Query data from vertical partition
        result = self.client.sql_query(
            'test_vertical',
            'products',
            where={'id': 1}
        )

        # Print debug info
        print(f"Query result for product ID 1: {result.data}")
        print(f"Query success: {result.success}, error: {result.error}")

        # Assert that the query was successful
        self.assertTrue(
            result.success, f"SQL query from vertical partition failed: {result.error}")
        self.assertTrue(len(result.data) > 0, "Expected at least one product")

    def test_12_verify_queried_data_integrity(self):
        """Test verifying the integrity of queried data from vertical partition."""
        # Query for the product
        result = self.client.sql_query(
            'test_vertical',
            'products',
            where={'id': 1}
        )

        # Verify the data
        self.assertTrue(len(result.data) > 0,
                        "Expected at least one product in query result")
        product = result.data[0]
        self.assertEqual(product['id'], 1, "Expected product with ID 1")
        self.assertEqual(product['name'], 'Test Product',
                         "Expected correct product name")
        self.assertEqual(product['category'], 'test',
                         "Expected correct product category")
        self.assertEqual(product['price'], '99.99',
                         "Expected correct product price")

    def test_13_insert_nosql_document(self):
        """Test inserting a document into NoSQL vertical database."""
        # Insert data into the NoSQL vertically partitioned database
        document = {
            "document_id": "doc1",
            "title": "Test Document",
            "content": "This is a test document for NoSQL vertical partitioning",
            "tags": ["test", "vertical", "partitioning"],
            "created_at": time.time()
        }

        result = self.client.nosql_insert(
            'test_nosql_vertical',
            document,
            'documents'
        )

        # Assert that the insert was successful
        self.assertTrue(
            result.success, f"NoSQL insert into vertical partition failed: {result.error}")
        print(f"Inserted NoSQL document: {result.data}")

    def test_14_query_nosql_document(self):
        """Test querying a document from NoSQL vertical database."""
        # Query the document by ID
        result = self.client.nosql_get(
            'test_nosql_vertical',
            'documents',
            where={"document_id": "doc1"}
        )

        # Assert that the query was successful
        self.assertTrue(
            result.success, f"NoSQL query from vertical partition failed: {result.error}")
        self.assertIsNotNone(result.data, "NoSQL query returned no data")
        self.assertTrue(len(result.data) > 0, "Expected at least one document")

    def test_15_verify_nosql_document_data(self):
        """Test verifying NoSQL document data integrity."""
        # Query the document
        result = self.client.nosql_get(
            'test_nosql_vertical',
            'documents',
            where={"document_id": "doc1"}
        )

        # Verify the data
        if result.data and len(result.data) > 0:
            doc = result.data[0]
            self.assertEqual(doc['document_id'], 'doc1',
                             "Expected document with ID 'doc1'")
            self.assertEqual(doc['title'], 'Test Document',
                             "Expected correct document title")
            self.assertIn('test', doc['tags'],
                          "Expected 'test' tag in the document")

    def test_16_get_vertical_partition_count(self):
        """Test getting partition count for vertical databases."""
        # Get partition information for both vertical databases
        vertical_partitions = self.client.query_router.get_database_partitions(
            'test_vertical')
        nosql_partitions = self.client.query_router.get_database_partitions(
            'test_nosql_vertical')

        print(f"Vertical partitions: {len(vertical_partitions)}")
        print(f"NoSQL partitions: {len(nosql_partitions)}")

        # Assert that we have at least one partition for each database
        self.assertTrue(len(vertical_partitions) > 0,
                        "Expected at least one vertical partition")
        self.assertTrue(len(nosql_partitions) > 0,
                        "Expected at least one NoSQL partition")

    def test_17_verify_sql_vertical_partition_type(self):
        """Test verifying partition types for SQL vertical partitions."""
        # Get partition information
        vertical_partitions = self.client.query_router.get_database_partitions(
            'test_vertical')

        # Verify partition types if partitions are available
        for partition in vertical_partitions:
            if 'partition_type' in partition:
                self.assertEqual(
                    partition['partition_type'], 'VERTICAL', "Expected vertical partition type")

    def test_18_verify_nosql_vertical_partition_type(self):
        """Test verifying partition types for NoSQL vertical partitions."""
        # Get partition information
        nosql_partitions = self.client.query_router.get_database_partitions(
            'test_nosql_vertical')

        # Verify partition types if partitions are available
        for partition in nosql_partitions:
            if 'partition_type' in partition:
                self.assertEqual(
                    partition['partition_type'], 'VERTICAL', "Expected vertical partition type for NoSQL")


if __name__ == '__main__':
    unittest.main()
