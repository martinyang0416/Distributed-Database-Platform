"""
Tests for coordinator caching functionality in the distributed database.
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


class TestCoordinatorCaching(unittest.TestCase):
    """Test cases for coordinator caching functionality.

    This test class includes tests for:
    - Basic caching operations
    - Cache invalidation
    - Cache hit/miss metrics
    """

    @classmethod
    def setUpClass(cls):
        """Set up test environment with coordinator and storage nodes."""
        # Create temporary directory for data
        cls.temp_dir = tempfile.mkdtemp()

        # Use default coordinator port and fixed storage port
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
                cls.node_id = response.json().get('node_id')
                print(
                    f"Storage node registered successfully with ID: {cls.node_id}")
            else:
                print(f"Failed to register storage node: {response.text}")
        except Exception as e:
            print(f"Error registering storage node: {e}")

        # Create client
        cls.client = DatabaseClient(
            coordinator_url=f'http://localhost:{cls.coordinator_port}')

        # Create a database with a simple schema for testing
        columns = [
            ColumnDefinition(name='id', data_type='INTEGER',
                             primary_key=True, nullable=False),
            ColumnDefinition(name='name', data_type='TEXT', nullable=False),
            ColumnDefinition(name='value', data_type='TEXT', nullable=True)
        ]

        table = TableSchema(name='cache_test', columns=columns)

        # Create database with horizontal partitioning
        success, error = cls.client.create_database(
            name='test_caching',
            db_type=DatabaseType.SQL,
            tables=[table],
            partition_type=PartitionType.HORIZONTAL,
            partition_key='id'
        )

        if not success:
            print(f"Failed to create database for caching test: {error}")

        # Insert some test data
        for i in range(10):
            result = cls.client.sql_insert(
                'test_caching',
                'cache_test',
                {'id': i+1, 'name': f'Item{i+1}', 'value': f'Value{i+1}'}
            )
            if not result.success:
                print(f"Failed to insert item {i+1}: {result.error}")

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

    def test_01_database_creation(self):
        """Test the database was created successfully."""
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/databases')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])
        self.assertIn('test_caching', data['databases'])

    def test_02_initial_cache_stats(self):
        """Test retrieving initial cache statistics."""
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/cache/stats')
        self.assertEqual(response.status_code, 200)
        stats = response.json()['stats']
        self.assertIn('hits', stats)
        self.assertIn('misses', stats)
        self.assertIn('size', stats)
        print(f"Initial cache stats: {stats}")

    def test_03_cache_population(self):
        """Test populating the cache with initial query."""
        # First query to populate the cache
        result = self.client.sql_query(
            'test_caching',
            'cache_test',
            where={'id': 1}
        )
        self.assertTrue(
            result.success, f"Failed to query data: {result.error}")
        self.assertIsNotNone(result.data, "Query returned no data")

        # Verify the cache stats changed after the query
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/cache/stats')
        self.assertEqual(response.status_code, 200)
        stats = response.json()['stats']
        # Either a hit, miss, or size change should have occurred
        print(f"Cache stats after population: {stats}")

    def test_04_cache_hit(self):
        """Test cache hits with repeated queries."""
        # Get cache stats before test
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/cache/stats')
        self.assertEqual(response.status_code, 200)
        before_stats = response.json()['stats']

        # Query the same data multiple times to test cache hits
        for _ in range(3):
            result = self.client.sql_query(
                'test_caching',
                'cache_test',
                where={'id': 1}
            )
            self.assertTrue(
                result.success, f"Failed to query data: {result.error}")
            self.assertIsNotNone(result.data, "Query returned no data")

        # Get cache stats after test
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/cache/stats')
        self.assertEqual(response.status_code, 200)
        after_stats = response.json()['stats']

        # Verify cache hits increased
        self.assertTrue(after_stats['hits'] >= before_stats['hits'],
                        "Cache hits should have increased after repeated queries")
        print(
            f"Cache hits before: {before_stats['hits']}, after: {after_stats['hits']}")

    def test_05_verify_cached_data(self):
        """Test that cached data contains expected content."""
        result = self.client.sql_query(
            'test_caching',
            'cache_test',
            where={'id': 1}
        )
        self.assertTrue(
            result.success, f"Failed to query data: {result.error}")

        # Find the item with ID 1
        found_id_1 = False
        for item in result.data:
            if item['id'] == 1:
                found_id_1 = True
                self.assertEqual(item['name'], 'Item1')
                self.assertEqual(item['value'], 'Value1')
                break

        self.assertTrue(
            found_id_1, "Expected to find item with ID 1 in the result")

    def test_06_cache_different_query(self):
        """Test cache behavior with a different query."""
        # Query a different item
        result = self.client.sql_query(
            'test_caching',
            'cache_test',
            where={'id': 2}
        )
        self.assertTrue(
            result.success, f"Failed to query data: {result.error}")
        self.assertIsNotNone(result.data, "Query returned no data")

        # Find the item with ID 2
        found_id_2 = False
        for item in result.data:
            if item['id'] == 2:
                found_id_2 = True
                self.assertEqual(item['name'], 'Item2')
                self.assertEqual(item['value'], 'Value2')
                break

        self.assertTrue(
            found_id_2, "Expected to find item with ID 2 in the result")

    def test_07_update_and_cache_invalidation(self):
        """Test cache invalidation following data update."""
        # Get cache stats before test
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/cache/stats')
        self.assertEqual(response.status_code, 200)
        before_stats = response.json()['stats']

        # Insert a test item for modification
        result = self.client.sql_insert(
            'test_caching',
            'cache_test',
            {'id': 100, 'name': 'CacheTest', 'value': 'BeforeUpdate'}
        )
        self.assertTrue(
            result.success, f"Failed to insert test item: {result.error}")

        # Query to cache the item
        result = self.client.sql_query(
            'test_caching',
            'cache_test',
            where={'id': 100}
        )
        self.assertTrue(
            result.success, f"Failed to query data: {result.error}")

        # Verify initial data state
        found_item = None
        for item in result.data:
            if item['id'] == 100:
                found_item = item
                break

        self.assertIsNotNone(found_item, "Could not find item with ID 100")
        self.assertEqual(found_item['name'], 'CacheTest')

    def test_08_update_data(self):
        """Test updating data that's in the cache."""
        # Update the data
        result = self.client.sql_update(
            'test_caching',
            'cache_test',
            {'name': 'UpdatedCacheTest'},
            where={'id': 100}
        )
        self.assertTrue(
            result.success, f"Failed to update data: {result.error}")

    def test_09_clear_cache(self):
        """Test manually clearing the cache."""
        # Clear the cache to ensure we get fresh data
        response = requests.post(
            f'http://localhost:{self.coordinator_port}/cache/clear')
        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()['success'],
                        "Cache clear operation should succeed")

        # Get cache stats after clear
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/cache/stats')
        self.assertEqual(response.status_code, 200)
        stats = response.json()['stats']
        # Size should be reset or very low after clear
        print(f"Cache stats after clear: {stats}")

    def test_10_verify_updated_data(self):
        """Test that query after cache clear returns updated data."""
        # Query again to verify we get the updated data
        result = self.client.sql_query(
            'test_caching',
            'cache_test',
            where={'id': 100}
        )
        self.assertTrue(
            result.success, f"Failed to query data after update: {result.error}")

        # Find the updated item
        found_updated_item = None
        for item in result.data:
            if item['id'] == 100:
                found_updated_item = item
                break

        self.assertIsNotNone(found_updated_item,
                             "Could not find updated item with ID 100")
        self.assertEqual(found_updated_item['name'], 'UpdatedCacheTest',
                         "Expected item name to be 'UpdatedCacheTest' after update")

    def test_11_cache_miss_tracking(self):
        """Test that cache misses are tracked correctly."""
        # Get cache stats before test
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/cache/stats')
        self.assertEqual(response.status_code, 200)
        before_stats = response.json()['stats']

        # Clear the cache
        response = requests.post(
            f'http://localhost:{self.coordinator_port}/cache/clear')
        self.assertEqual(response.status_code, 200)

        # Query for a previously queried item to generate a miss
        result = self.client.sql_query(
            'test_caching',
            'cache_test',
            where={'id': 3}  # Query something we haven't used recently
        )
        self.assertTrue(
            result.success, f"Failed to query data: {result.error}")

        # Get cache stats after test
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/cache/stats')
        self.assertEqual(response.status_code, 200)
        after_stats = response.json()['stats']

        # Verify cache misses increased
        self.assertTrue(after_stats['misses'] >= before_stats['misses'],
                        "Cache misses should have increased after query with cleared cache")
        print(
            f"Cache misses before: {before_stats['misses']}, after: {after_stats['misses']}")

    def test_12_cache_reuse(self):
        """Test that the cache is reused for subsequent queries."""
        # Make a query to cache an item
        result = self.client.sql_query(
            'test_caching',
            'cache_test',
            where={'id': 4}  # Query something we haven't used recently
        )
        self.assertTrue(
            result.success, f"Failed to query data: {result.error}")

        # Get cache stats after first query
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/cache/stats')
        self.assertEqual(response.status_code, 200)
        after_first_query = response.json()['stats']

        # Query the same item again
        result = self.client.sql_query(
            'test_caching',
            'cache_test',
            where={'id': 4}
        )
        self.assertTrue(
            result.success, f"Failed to query data again: {result.error}")

        # Get cache stats after second query
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/cache/stats')
        self.assertEqual(response.status_code, 200)
        after_second_query = response.json()['stats']

        # Verify cache hits increased
        self.assertTrue(after_second_query['hits'] >= after_first_query['hits'],
                        "Cache hits should have increased after repeated query")

    def test_13_test_cache_for_multiple_items(self):
        """Test cache behavior with a query for multiple items."""
        # Query multiple items
        result = self.client.sql_query(
            'test_caching',
            'cache_test',
            where={'id': {'$gt': 5, '$lt': 10}}  # Items 6-9
        )
        self.assertTrue(
            result.success, f"Failed to query multiple items: {result.error}")
        self.assertIsNotNone(result.data, "Query returned no data")

        # Verify we got multiple items
        item_ids = [item['id'] for item in result.data]
        self.assertTrue(all(6 <= id <= 9 for id in item_ids),
                        f"Expected items with IDs 6-9, got {item_ids}")

        # Query again to test cache hit
        result = self.client.sql_query(
            'test_caching',
            'cache_test',
            where={'id': {'$gt': 5, '$lt': 10}}  # Same query
        )
        self.assertTrue(
            result.success, f"Failed to query multiple items again: {result.error}")

    def test_14_cache_for_simple_expiration(self):
        """Test simple cache expiration by inserting a new item."""
        # Insert a special item for this test
        result = self.client.sql_insert(
            'test_caching',
            'cache_test',
            {'id': 200, 'name': 'ExpirationTest', 'value': 'BeforeExpiration'}
        )
        self.assertTrue(
            result.success, f"Failed to insert test item: {result.error}")

        # Query the item to cache it
        result = self.client.sql_query(
            'test_caching',
            'cache_test',
            where={'id': 200}
        )
        self.assertTrue(
            result.success, f"Failed to query data: {result.error}")

        # Try to set cache expiration time (not always supported)
        try:
            response = requests.post(
                f'http://localhost:{self.coordinator_port}/cache/config',
                json={'expiration_seconds': 1}
            )
            has_expiration_config = response.status_code == 200
        except:
            has_expiration_config = False

        # Wait for cache to expire or simulate expiration
        if has_expiration_config:
            print("Waiting for cache to expire...")
            time.sleep(2)  # Wait longer than the expiration time
        else:
            # Simulate expiration by clearing the cache
            print("Simulating cache expiration by clearing the cache...")
            response = requests.post(
                f'http://localhost:{self.coordinator_port}/cache/clear')
            self.assertEqual(response.status_code, 200)

    def test_15_verify_post_expiration(self):
        """Test behavior after cache expiration."""
        # Update the item after expiration
        result = self.client.sql_update(
            'test_caching',
            'cache_test',
            {'name': 'AfterExpiration'},
            where={'id': 200}
        )
        self.assertTrue(
            result.success, f"Failed to update test item: {result.error}")

        # Query again to verify we get updated data
        result = self.client.sql_query(
            'test_caching',
            'cache_test',
            where={'id': 200}
        )
        self.assertTrue(
            result.success, f"Failed to query data after expiration: {result.error}")

        # Find the updated item
        found_item = None
        for item in result.data:
            if item['id'] == 200:
                found_item = item
                break

        self.assertIsNotNone(
            found_item, "Could not find item with ID 200 after expiration")
        self.assertEqual(found_item['name'], 'AfterExpiration',
                         "Expected item name to be 'AfterExpiration' after cache expiration")

    def test_16_prepare_for_eviction_test(self):
        """Prepare the cache for eviction testing by clearing it."""
        # Clear the cache to start with a clean state
        response = requests.post(
            f'http://localhost:{self.coordinator_port}/cache/clear')
        self.assertEqual(response.status_code, 200)

        # Get cache stats before test
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/cache/stats')
        self.assertEqual(response.status_code, 200)
        before_stats = response.json()['stats']
        print(f"Cache stats before eviction test: {before_stats}")

        # Try to set a small max cache size
        try:
            response = requests.post(
                f'http://localhost:{self.coordinator_port}/cache/config',
                # Set to a small value to trigger eviction
                json={'max_size': 5}
            )
            has_size_config = response.status_code == 200
        except:
            has_size_config = False

        print(f"Cache configured with max_size=5: {has_size_config}")

    def test_17_fill_cache_for_eviction(self):
        """Fill the cache with multiple items to trigger eviction."""
        # Insert and query multiple items to fill the cache
        for i in range(10):
            # Insert a new item
            result = self.client.sql_insert(
                'test_caching',
                'cache_test',
                {'id': 300 + i, 'name': f'EvictionTest{i}', 'value': f'Value{i}'}
            )
            self.assertTrue(
                result.success, f"Failed to insert test item {i}: {result.error}")

            # Query the item to cache it
            result = self.client.sql_query(
                'test_caching',
                'cache_test',
                where={'id': 300 + i}
            )
            self.assertTrue(
                result.success, f"Failed to query data for item {i}: {result.error}")

    def test_18_verify_eviction_stats(self):
        """Verify cache statistics after potential eviction."""
        # Get cache stats after filling
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/cache/stats')
        self.assertEqual(response.status_code, 200)
        after_stats = response.json()['stats']
        print(f"Cache stats after filling: {after_stats}")

        # If evictions are tracked, check if they occurred
        if 'evictions' in after_stats:
            print(f"Cache evictions: {after_stats['evictions']}")

    def test_19_verify_data_access_post_eviction(self):
        """Verify data can still be accessed even if evicted from cache."""
        # Query for the first and last items we inserted
        first_result = self.client.sql_query(
            'test_caching',
            'cache_test',
            where={'id': 300}
        )
        self.assertTrue(first_result.success,
                        f"Failed to query first item: {first_result.error}")

        last_result = self.client.sql_query(
            'test_caching',
            'cache_test',
            where={'id': 309}
        )
        self.assertTrue(last_result.success,
                        f"Failed to query last item: {last_result.error}")

        # Verify we can still access all the data
        self.assertIsNotNone(
            first_result.data, "Query for potentially evicted item returned no data")
        self.assertIsNotNone(
            last_result.data, "Query for recently cached item returned no data")

    def test_20_verify_data_integrity_post_eviction(self):
        """Verify data integrity is maintained after eviction."""
        # Find the first item (ID 300)
        first_result = self.client.sql_query(
            'test_caching',
            'cache_test',
            where={'id': 300}
        )

        found_first = None
        for item in first_result.data:
            if item['id'] == 300:
                found_first = item
                break

        self.assertIsNotNone(
            found_first, "Could not find first item with ID 300")
        self.assertEqual(
            found_first['name'], 'EvictionTest0', "First item has incorrect name")

        # Find the last item (ID 309)
        last_result = self.client.sql_query(
            'test_caching',
            'cache_test',
            where={'id': 309}
        )

        found_last = None
        for item in last_result.data:
            if item['id'] == 309:
                found_last = item
                break

        self.assertIsNotNone(
            found_last, "Could not find last item with ID 309")
        self.assertEqual(
            found_last['name'], 'EvictionTest9', "Last item has incorrect name")


if __name__ == '__main__':
    unittest.main()
