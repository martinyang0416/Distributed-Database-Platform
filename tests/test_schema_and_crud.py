"""
Tests for schema definition, schema modifications, and basic CRUD operations.
"""
import os
import sys
import unittest
import tempfile
import shutil
import time
import requests
import subprocess

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from distributed_db.common.models import (
    DatabaseType, PartitionType, TableSchema, ColumnDefinition
)
from distributed_db.client.client import DatabaseClient


class TestSchemaAndCRUD(unittest.TestCase):
    """Test cases for schema definition, schema modifications, and basic CRUD operations.

    This test class includes tests for:
    - Creating SQL and NoSQL databases
    - Creating databases with different partition types
    - Creating databases with complex schemas and data types
    - Creating databases with multiple tables and relationships
    - Basic CRUD operations on SQL and NoSQL databases
    - Deleting databases
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
        cls.client = DatabaseClient(coordinator_url=f'http://localhost:{cls.coordinator_port}')

        # Initialize document ID for NoSQL tests
        cls.doc_id = None

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



    def test_01_create_sql_database(self):
        """Test creating a SQL database with schema."""
        # Define columns
        columns = [
            ColumnDefinition(name='id', data_type='INTEGER', primary_key=True, nullable=False),
            ColumnDefinition(name='name', data_type='TEXT', nullable=False),
            ColumnDefinition(name='age', data_type='INTEGER', nullable=True)
        ]

        # Define table
        table = TableSchema(name='users', columns=columns, indexes=['name'])

        # Create database
        success, error = self.client.create_database(
            name='test_sql_db',
            db_type=DatabaseType.SQL,
            tables=[table],
            partition_type=PartitionType.HORIZONTAL,
            partition_key='id'
        )

        # Verify database was created
        self.assertTrue(success, f"Failed to create SQL database: {error}")

        # Verify database exists by listing databases
        response = requests.get(f'http://localhost:{self.coordinator_port}/databases')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])
        self.assertIn('test_sql_db', data['databases'])

    def test_02_create_nosql_database(self):
        """Test creating a NoSQL database."""
        # Create database
        success, error = self.client.create_database(
            name='test_nosql_db',
            db_type=DatabaseType.NOSQL
        )

        # Verify database was created
        self.assertTrue(success, f"Failed to create NoSQL database: {error}")

        # Verify database exists by listing databases
        response = requests.get(f'http://localhost:{self.coordinator_port}/databases')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])
        self.assertIn('test_nosql_db', data['databases'])

    def test_03_get_database_schema(self):
        """Test retrieving database schema."""
        # Get SQL database schema
        response = requests.get(f'http://localhost:{self.coordinator_port}/databases/test_sql_db')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])
        self.assertEqual(data['database']['name'], 'test_sql_db')
        self.assertEqual(data['database']['db_type'], 'SQL')

        # Get NoSQL database schema
        response = requests.get(f'http://localhost:{self.coordinator_port}/databases/test_nosql_db')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])
        self.assertEqual(data['database']['name'], 'test_nosql_db')
        self.assertEqual(data['database']['db_type'], 'NOSQL')

    def test_04_storage_node_status(self):
        """Test storage node status."""
        # Get storage node status
        response = requests.get(f'http://localhost:{self.storage_port}/status')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])
        self.assertEqual(data['host'], 'localhost')
        self.assertEqual(data['port'], self.storage_port)

        # Verify databases are listed
        self.assertIn('sql', data['databases'])
        self.assertIn('nosql', data['databases'])

        # Verify node is registered with coordinator
        response = requests.get(f'http://localhost:{self.coordinator_port}/nodes')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])
        self.assertTrue(len(data['nodes']) > 0)

    def test_05_sql_crud_operations(self):
        """Test SQL CRUD operations."""
        # Create a test database with a users table
        columns = [
            ColumnDefinition(name='id', data_type='INTEGER', primary_key=True, nullable=False),
            ColumnDefinition(name='name', data_type='TEXT', nullable=False),
            ColumnDefinition(name='email', data_type='TEXT', nullable=True)
        ]

        table = TableSchema(name='users', columns=columns, indexes=['name'])

        success, error = self.client.create_database(
            name='test_sql_crud',
            db_type=DatabaseType.SQL,
            tables=[table],
            partition_type=PartitionType.HORIZONTAL,
            partition_key='id'
        )

        self.assertTrue(success, f"Failed to create SQL database: {error}")

        # Insert data
        result = self.client.sql_insert(
            'test_sql_crud',
            'users',
            {'id': 1, 'name': 'Alice', 'email': 'alice@example.com'}
        )

        # Use proper assertion to mark test as failed if insert fails
        self.assertTrue(result.success, f"SQL insert failed: {result.error}")
        self.assertIsNotNone(result.data, "SQL insert returned no data")

        # Insert another record
        result = self.client.sql_insert(
            'test_sql_crud',
            'users',
            {'id': 2, 'name': 'Bob', 'email': 'bob@example.com'}
        )

        self.assertTrue(result.success, f"SQL insert failed: {result.error}")
        self.assertIsNotNone(result.data, "SQL insert returned no data")

        # Query data
        result = self.client.sql_query(
            'test_sql_crud',
            'users'
        )

        self.assertTrue(result.success, f"SQL query failed: {result.error}")
        self.assertIsNotNone(result.data, "SQL query returned no data")
        self.assertTrue(len(result.data) > 0, "Expected at least one row")
        print(f"Query result: {result.data}")

        # Update data
        result = self.client.sql_update(
            'test_sql_crud',
            'users',
            values={'email': 'alice.new@example.com'},
            where={'id': 1}
        )

        self.assertTrue(result.success, f"SQL update failed: {result.error}")

        # Query to verify update
        result = self.client.sql_query(
            'test_sql_crud',
            'users',
            where={'id': 1}
        )

        self.assertTrue(result.success, f"SQL query after update failed: {result.error}")
        self.assertIsNotNone(result.data, "SQL query after update returned no data")
        self.assertTrue(len(result.data) > 0, "Expected at least one row after update")
        self.assertEqual(result.data[0]['email'], 'alice.new@example.com', "Update did not change the email field")

        # Delete data
        result = self.client.sql_delete(
            'test_sql_crud',
            'users',
            where={'id': 1}
        )

        self.assertTrue(result.success, f"SQL delete failed: {result.error}")

        # Sleep briefly to allow cache invalidation to take effect
        import time
        time.sleep(0.5)

        # Query to verify delete - use a direct query to the storage node to bypass cache
        try:
            import requests
            response = requests.post(
                f'http://localhost:{self.storage_port}/query/test_sql_crud',
                json={
                    'operation': 'query',
                    'table': 'users',
                    'columns': ['*'],
                    'where': {'id': 1}
                }
            )

            self.assertEqual(response.status_code, 200, f"Query failed with status code {response.status_code}")
            result_data = response.json()
            self.assertTrue(result_data.get('success', False), f"Query failed: {result_data.get('error')}")
            data = result_data.get('data', [])
            self.assertEqual(len(data), 0, "Expected 0 rows with id=1 after delete")
        except Exception as e:
            self.fail(f"Error querying storage node: {e}")

    def test_06_nosql_crud_operations(self):
        """Test NoSQL CRUD operations."""
        # Create a test NoSQL database
        success, error = self.client.create_database(
            name='test_nosql_crud',
            db_type=DatabaseType.NOSQL
        )

        self.assertTrue(success, f"Failed to create NoSQL database: {error}")

        # Insert document
        doc = {
            'name': 'Charlie',
            'email': 'charlie@example.com',
            'age': 35,
            'tags': ['developer', 'python', 'nosql']
        }

        result = self.client.nosql_insert(
            'test_nosql_crud',
            doc,
            collection='users'
        )

        self.assertTrue(result.success, f"NoSQL insert failed: {result.error}")
        self.assertIsNotNone(result.data, "NoSQL insert returned no data")

        # Store document ID for later tests
        doc_id = None
        if result.data and 'id' in result.data:
            doc_id = result.data['id']
            print(f"Inserted document with ID: {doc_id}")
            self.assertIsNotNone(doc_id, "NoSQL insert did not return a document ID")

        # Insert another document
        doc2 = {
            'name': 'David',
            'email': 'david@example.com',
            'age': 28,
            'tags': ['designer', 'ui/ux']
        }

        result = self.client.nosql_insert(
            'test_nosql_crud',
            doc2,
            collection='users'
        )

        self.assertTrue(result.success, f"Second NoSQL insert failed: {result.error}")
        self.assertIsNotNone(result.data, "Second NoSQL insert returned no data")

        # Query documents
        result = self.client.nosql_get(
            'test_nosql_crud',
            collection='users'
        )

        self.assertTrue(result.success, f"NoSQL query failed: {result.error}")
        self.assertIsNotNone(result.data, "NoSQL query returned no data")
        self.assertTrue(len(result.data) > 0, "Expected at least one document")
        print(f"Query result: {result.data}")

        # Update document if we have a document ID
        self.assertIsNotNone(doc_id, "Document ID is required for update test")

        result = self.client.nosql_update(
            'test_nosql_crud',
            {'age': 36, 'tags': ['developer', 'python', 'nosql', 'mongodb']},
            doc_id=doc_id,
            collection='users'
        )

        self.assertTrue(result.success, f"NoSQL update failed: {result.error}")

        # The update operation returns the updated document, so we can verify it directly
        self.assertIsNotNone(result.data, "NoSQL update returned no data")
        self.assertEqual(result.data['age'], 36, "Update did not change the age field")
        self.assertEqual(len(result.data['tags']), 4, "Update did not change the tags field")

        # Delete document
        result = self.client.nosql_delete(
            'test_nosql_crud',
            doc_id=doc_id,
            collection='users'
        )

        self.assertTrue(result.success, f"NoSQL delete failed: {result.error}")

        # Query to verify delete
        result = self.client.nosql_get(
            'test_nosql_crud',
            collection='users',
            doc_id=doc_id
        )

        self.assertTrue(result.success, f"NoSQL query after delete failed: {result.error}")
        self.assertIsNone(result.data, "Expected no document after delete")

    def test_07_complex_schema_creation(self):
        """Test creating a database with a complex schema."""
        # Create a database with multiple tables and relationships
        # Users table
        user_columns = [
            ColumnDefinition(name='id', data_type='INTEGER', primary_key=True, nullable=False),
            ColumnDefinition(name='username', data_type='TEXT', nullable=False),
            ColumnDefinition(name='email', data_type='TEXT', nullable=False),
            ColumnDefinition(name='created_at', data_type='TIMESTAMP', default='CURRENT_TIMESTAMP')
        ]

        users_table = TableSchema(name='users', columns=user_columns, indexes=['username', 'email'])

        # Posts table with foreign key to users
        post_columns = [
            ColumnDefinition(name='id', data_type='INTEGER', primary_key=True, nullable=False),
            ColumnDefinition(name='user_id', data_type='INTEGER', nullable=False),
            ColumnDefinition(name='title', data_type='TEXT', nullable=False),
            ColumnDefinition(name='content', data_type='TEXT', nullable=False),
            ColumnDefinition(name='created_at', data_type='TIMESTAMP', default='CURRENT_TIMESTAMP')
        ]

        posts_table = TableSchema(name='posts', columns=post_columns, indexes=['user_id', 'created_at'])

        # Comments table with foreign keys to users and posts
        comment_columns = [
            ColumnDefinition(name='id', data_type='INTEGER', primary_key=True, nullable=False),
            ColumnDefinition(name='post_id', data_type='INTEGER', nullable=False),
            ColumnDefinition(name='user_id', data_type='INTEGER', nullable=False),
            ColumnDefinition(name='content', data_type='TEXT', nullable=False),
            ColumnDefinition(name='created_at', data_type='TIMESTAMP', default='CURRENT_TIMESTAMP')
        ]

        comments_table = TableSchema(name='comments', columns=comment_columns, indexes=['post_id', 'user_id'])

        # Create the database
        success, error = self.client.create_database(
            name='test_complex_schema',
            db_type=DatabaseType.SQL,
            tables=[users_table, posts_table, comments_table],
            partition_type=PartitionType.HORIZONTAL,
            partition_key='id'
        )

        self.assertTrue(success, f"Failed to create complex schema database: {error}")

        # Verify database was created
        response = requests.get(f'http://localhost:{self.coordinator_port}/databases/test_complex_schema')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])

        # Verify tables were created
        db_info = data['database']
        if 'tables' in db_info:
            table_names = [table['name'] for table in db_info['tables']]
            self.assertIn('users', table_names)
            self.assertIn('posts', table_names)
            self.assertIn('comments', table_names)

    def test_08_delete_database(self):
        """Test deleting a database."""
        # Create a database to delete
        success, error = self.client.create_database(
            name='test_delete_db',
            db_type=DatabaseType.SQL,
            tables=[
                TableSchema(
                    name='test',
                    columns=[
                        ColumnDefinition(name='id', data_type='INTEGER', primary_key=True)
                    ]
                )
            ]
        )

        self.assertTrue(success, f"Failed to create database for deletion test: {error}")

        # Verify database exists
        response = requests.get(f'http://localhost:{self.coordinator_port}/databases')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertIn('test_delete_db', data['databases'])

        # Delete the database
        success, error = self.client.delete_database('test_delete_db')
        self.assertTrue(success, f"Failed to delete database: {error}")

        # Verify database was deleted
        response = requests.get(f'http://localhost:{self.coordinator_port}/databases')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertNotIn('test_delete_db', data['databases'])

    def test_09_create_database_with_multiple_partition_types(self):
        """Test creating databases with different partition types."""
        # Create a database with horizontal partitioning
        columns = [
            ColumnDefinition(name='id', data_type='INTEGER', primary_key=True, nullable=False),
            ColumnDefinition(name='name', data_type='TEXT', nullable=False),
            ColumnDefinition(name='email', data_type='TEXT', nullable=True)
        ]

        table = TableSchema(name='users', columns=columns, indexes=['name'])

        # Make sure the partition key is a valid column in the table
        success, error = self.client.create_database(
            name='test_horizontal',
            db_type=DatabaseType.SQL,
            tables=[table],
            partition_type=PartitionType.HORIZONTAL,
            partition_key='id'  # This is a valid column in the table
        )

        self.assertTrue(success, f"Failed to create horizontally partitioned database: {error}")

        # Create a database with vertical partitioning
        success, error = self.client.create_database(
            name='test_vertical',
            db_type=DatabaseType.SQL,
            tables=[table],
            partition_type=PartitionType.VERTICAL
        )

        self.assertTrue(success, f"Failed to create vertically partitioned database: {error}")

        # Create a database with mixed partitioning
        success, error = self.client.create_database(
            name='test_mixed',
            db_type=DatabaseType.SQL,
            tables=[table],
            partition_type=PartitionType.MIXED,
            partition_key='id'
        )

        self.assertTrue(success, f"Failed to create mixed partitioned database: {error}")

        # Verify all databases were created
        response = requests.get(f'http://localhost:{self.coordinator_port}/databases')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])
        self.assertIn('test_horizontal', data['databases'])
        self.assertIn('test_vertical', data['databases'])
        self.assertIn('test_mixed', data['databases'])

    def test_10_create_database_with_complex_schema(self):
        """Test creating a database with a complex schema including various data types."""
        # Define columns with various data types
        columns = [
            ColumnDefinition(name='id', data_type='INTEGER', primary_key=True, nullable=False),
            ColumnDefinition(name='name', data_type='TEXT', nullable=False),
            ColumnDefinition(name='age', data_type='INTEGER', nullable=True),
            ColumnDefinition(name='salary', data_type='REAL', nullable=True),
            ColumnDefinition(name='is_active', data_type='BOOLEAN', default='TRUE'),
            ColumnDefinition(name='created_at', data_type='TIMESTAMP', default='CURRENT_TIMESTAMP'),
            ColumnDefinition(name='data', data_type='BLOB', nullable=True)
        ]

        table = TableSchema(name='employees', columns=columns, indexes=['name', 'age'])

        success, error = self.client.create_database(
            name='test_complex_types',
            db_type=DatabaseType.SQL,
            tables=[table],
            partition_type=PartitionType.HORIZONTAL,
            partition_key='id'
        )

        self.assertTrue(success, f"Failed to create database with complex types: {error}")

        # Verify database was created
        response = requests.get(f'http://localhost:{self.coordinator_port}/databases/test_complex_types')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])

        # Verify column types
        if 'tables' in data['database']:
            for table in data['database']['tables']:
                if table['name'] == 'employees':
                    column_types = {col['name']: col['data_type'] for col in table['columns']}
                    self.assertEqual(column_types['id'], 'INTEGER')
                    self.assertEqual(column_types['name'], 'TEXT')
                    self.assertEqual(column_types['age'], 'INTEGER')
                    self.assertEqual(column_types['salary'], 'REAL')
                    self.assertEqual(column_types['is_active'], 'BOOLEAN')
                    self.assertEqual(column_types['created_at'], 'TIMESTAMP')
                    self.assertEqual(column_types['data'], 'BLOB')

    def test_11_create_database_with_multiple_tables(self):
        """Test creating a database with multiple tables and relationships."""
        # Users table
        user_columns = [
            ColumnDefinition(name='id', data_type='INTEGER', primary_key=True, nullable=False),
            ColumnDefinition(name='username', data_type='TEXT', nullable=False),
            ColumnDefinition(name='email', data_type='TEXT', nullable=False)
        ]

        users_table = TableSchema(name='users', columns=user_columns, indexes=['username', 'email'])

        # Products table
        product_columns = [
            ColumnDefinition(name='id', data_type='INTEGER', primary_key=True, nullable=False),
            ColumnDefinition(name='name', data_type='TEXT', nullable=False),
            ColumnDefinition(name='price', data_type='REAL', nullable=False),
            ColumnDefinition(name='description', data_type='TEXT', nullable=True)
        ]

        products_table = TableSchema(name='products', columns=product_columns, indexes=['name'])

        # Orders table with foreign keys
        order_columns = [
            ColumnDefinition(name='id', data_type='INTEGER', primary_key=True, nullable=False),
            ColumnDefinition(name='user_id', data_type='INTEGER', nullable=False),
            ColumnDefinition(name='order_date', data_type='TIMESTAMP', default='CURRENT_TIMESTAMP'),
            ColumnDefinition(name='status', data_type='TEXT', nullable=False)
        ]

        orders_table = TableSchema(name='orders', columns=order_columns, indexes=['user_id', 'order_date'])

        # Order items table with foreign keys
        order_item_columns = [
            ColumnDefinition(name='id', data_type='INTEGER', primary_key=True, nullable=False),
            ColumnDefinition(name='order_id', data_type='INTEGER', nullable=False),
            ColumnDefinition(name='product_id', data_type='INTEGER', nullable=False),
            ColumnDefinition(name='quantity', data_type='INTEGER', nullable=False),
            ColumnDefinition(name='price', data_type='REAL', nullable=False)
        ]

        order_items_table = TableSchema(name='order_items', columns=order_item_columns, indexes=['order_id', 'product_id'])

        # Create the database
        success, error = self.client.create_database(
            name='test_ecommerce',
            db_type=DatabaseType.SQL,
            tables=[users_table, products_table, orders_table, order_items_table],
            partition_type=PartitionType.HORIZONTAL,
            partition_key='id'
        )

        self.assertTrue(success, f"Failed to create e-commerce database: {error}")

        # Verify database was created
        response = requests.get(f'http://localhost:{self.coordinator_port}/databases/test_ecommerce')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])

        # Verify tables were created
        if 'tables' in data['database']:
            table_names = [table['name'] for table in data['database']['tables']]
            self.assertIn('users', table_names)
            self.assertIn('products', table_names)
            self.assertIn('orders', table_names)
            self.assertIn('order_items', table_names)


if __name__ == '__main__':
    unittest.main()
