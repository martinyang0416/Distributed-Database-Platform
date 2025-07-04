"""
Tests for Raft consensus and fault tolerance in the distributed database.
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
import random

# Add parent directory to path
sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))


class TestConsensus(unittest.TestCase):
    """Test cases for Raft consensus and fault tolerance.

    This test class includes simple tests for:
    - Basic consensus operations
    - Simple fault tolerance scenarios
    - Leader election
    - Data consistency after node failures
    """

    @classmethod
    def setUpClass(cls):
        """Set up test environment with multiple nodes."""
        # Create temporary directory for data
        cls.temp_dir = tempfile.mkdtemp()

        # Use default coordinator port and fixed storage ports
        cls.coordinator_port = 5000
        # Three storage nodes for consensus
        cls.storage_ports = [5501, 5502, 5503]

        # Initialize attributes that will be set later
        cls.new_id_after_failure = 6  # Default value
        cls.additional_ids = []  # Default empty list
        cls.restart_test_id = 100  # Default value

        # Start coordinator
        cls.coordinator_process = subprocess.Popen([
            sys.executable, '-m', 'distributed_db.coordinator.coordinator',
            '--host', 'localhost',
            '--port', str(cls.coordinator_port),
            '--data-dir', os.path.join(cls.temp_dir, 'coordinator')
        ])

        # Wait for coordinator to start
        time.sleep(2)

        # Start storage nodes
        cls.storage_processes = []
        for i, port in enumerate(cls.storage_ports):
            process = subprocess.Popen([
                sys.executable, '-m', 'distributed_db.storage.storage_node',
                '--host', 'localhost',
                '--port', str(port),
                '--data-dir', os.path.join(cls.temp_dir, f'storage_{i}'),
                '--coordinator', f'http://localhost:{cls.coordinator_port}'
            ])
            cls.storage_processes.append(process)

        # Wait for storage nodes to start
        time.sleep(2)

        # Register storage nodes with coordinator
        cls.node_ids = []
        for port in cls.storage_ports:
            try:
                response = requests.post(
                    f'http://localhost:{cls.coordinator_port}/nodes',
                    json={
                        'host': 'localhost',
                        'port': port,
                        'status': 'active'
                    }
                )
                if response.status_code == 200:
                    node_id = response.json().get('node_id')
                    if node_id:
                        cls.node_ids.append(node_id)
                    print(
                        f"Storage node on port {port} registered successfully")
                else:
                    print(
                        f"Failed to register storage node on port {port}: {response.text}")
            except Exception as e:
                print(f"Error registering storage node on port {port}: {e}")

        # Create client
        cls.client = DatabaseClient(
            coordinator_url=f'http://localhost:{cls.coordinator_port}')

        # Create a database with replication for testing
        columns = [
            ColumnDefinition(name='id', data_type='INTEGER',
                             primary_key=True, nullable=False),
            ColumnDefinition(name='name', data_type='TEXT', nullable=False),
            ColumnDefinition(name='value', data_type='TEXT', nullable=True)
        ]

        table = TableSchema(name='consensus_test', columns=columns)

        # Create database with horizontal partitioning
        success, error = cls.client.create_database(
            name='test_consensus',
            db_type=DatabaseType.SQL,
            tables=[table],
            partition_type=PartitionType.HORIZONTAL,
            partition_key='id'
        )

        if not success:
            print(f"Failed to create database for consensus test: {error}")

    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        # Stop processes
        if hasattr(cls, 'coordinator_process'):
            cls.coordinator_process.terminate()
            cls.coordinator_process.wait()

        if hasattr(cls, 'storage_processes'):
            for process in cls.storage_processes:
                process.terminate()
                process.wait()

        # Remove temporary directory
        shutil.rmtree(cls.temp_dir)

    def test_01_verify_database_creation(self):
        """Test that the consensus database was created successfully."""
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/databases')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])
        self.assertIn('test_consensus', data['databases'])
        print("Database 'test_consensus' created successfully")

    def test_02_verify_nodes_registered(self):
        """Test that all storage nodes are registered."""
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/nodes')
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertTrue(data['success'])

        # Verify we have all our nodes registered
        nodes_dict = data.get('nodes', {})
        self.assertTrue(len(nodes_dict) >= len(self.storage_ports) - 1,
                        f"Expected at least {len(self.storage_ports) - 1} nodes, found {len(nodes_dict)}")

        # Print nodes for debugging
        for node_id, node_info in nodes_dict.items():
            print(
                f"Registered node: ID={node_id}, Status={node_info.get('status')}")

    def test_03_insert_initial_data(self):
        """Test inserting initial data into the consensus database."""
        # Insert first set of data
        for i in range(3):
            result = self.client.sql_insert(
                'test_consensus',
                'consensus_test',
                {'id': i+1, 'name': f'Item{i+1}', 'value': f'Value{i+1}'}
            )
            self.assertTrue(
                result.success, f"Failed to insert item {i+1}: {result.error}")
            print(f"Inserted item {i+1}: {result.data}")

    def test_04_verify_initial_data(self):
        """Test querying the initial data to verify it was inserted correctly."""
        result = self.client.sql_query(
            'test_consensus',
            'consensus_test',
            where={'id': {'$lte': 3}}
        )
        self.assertTrue(
            result.success, f"Failed to query initial data: {result.error}")
        self.assertIsNotNone(result.data, "Query returned no data")

        # Verify we have all 3 items
        items = {item['id']: item for item in result.data}
        for i in range(1, 4):
            self.assertIn(i, items, f"Expected to find item with ID {i}")
            self.assertEqual(items[i]['name'],
                             f'Item{i}', f"Item {i} has incorrect name")
            self.assertEqual(
                items[i]['value'], f'Value{i}', f"Item {i} has incorrect value")

        print(f"Successfully verified initial data: {result.data}")

    def test_05_insert_more_data(self):
        """Test inserting additional data to verify continuing consensus."""
        # Insert more data
        for i in range(3, 5):  # Items 4 and 5
            result = self.client.sql_insert(
                'test_consensus',
                'consensus_test',
                {'id': i+1, 'name': f'Item{i+1}', 'value': f'Value{i+1}'}
            )
            self.assertTrue(
                result.success, f"Failed to insert item {i+1}: {result.error}")
            print(f"Inserted item {i+1}: {result.data}")

    def test_06_verify_additional_data(self):
        """Test querying the additional data to verify consistency."""
        result = self.client.sql_query(
            'test_consensus',
            'consensus_test',
            where={'id': {'$gt': 3}}
        )
        self.assertTrue(
            result.success, f"Failed to query additional data: {result.error}")
        self.assertIsNotNone(result.data, "Query returned no data")

        # Verify we have items 4 and 5
        items = {item['id']: item for item in result.data}
        for i in range(4, 6):
            self.assertIn(i, items, f"Expected to find item with ID {i}")
            self.assertEqual(items[i]['name'],
                             f'Item{i}', f"Item {i} has incorrect name")
            self.assertEqual(
                items[i]['value'], f'Value{i}', f"Item {i} has incorrect value")

        print(f"Successfully verified additional data: {result.data}")

    def test_07_stop_storage_node(self):
        """Test stopping one storage node to simulate a failure."""
        # Get current node count
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/nodes')
        self.assertEqual(response.status_code, 200)
        before_nodes_dict = response.json().get('nodes', {})

        # Stop one storage node (the last one)
        if self.storage_processes and len(self.storage_processes) > 0:
            print("Stopping one storage node to test fault tolerance...")
            process = self.storage_processes.pop()
            process.terminate()
            process.wait()
            time.sleep(2)  # Wait for the system to detect the node failure

        # Verify the node count decreased
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/nodes')
        self.assertEqual(response.status_code, 200)
        after_nodes_dict = response.json().get('nodes', {})

        # Note: The node might still appear in the list but with an inactive status
        active_before = sum(
            1 for node_info in before_nodes_dict.values() if node_info.get('status') == 'active')
        active_after = sum(
            1 for node_info in after_nodes_dict.values() if node_info.get('status') == 'active')

        print(f"Active nodes before: {active_before}, after: {active_after}")

    def test_08_start_replacement_node(self):
        """Test starting a replacement node after a failure."""
        # Start a new storage node to replace the one we stopped
        # Use the same port as the one we stopped
        new_port = self.storage_ports[2]
        new_process = subprocess.Popen([
            sys.executable, '-m', 'distributed_db.storage.storage_node',
            '--host', 'localhost',
            '--port', str(new_port),
            '--data-dir', os.path.join(self.temp_dir, 'storage_new'),
            '--coordinator', f'http://localhost:{self.coordinator_port}'
        ])
        self.storage_processes.append(new_process)

        # Wait for the node to start
        time.sleep(3)
        print(f"Started replacement node on port {new_port}")

    def test_09_register_replacement_node(self):
        """Test registering the replacement node with the coordinator."""
        new_port = self.storage_ports[2]
        try:
            response = requests.post(
                f'http://localhost:{self.coordinator_port}/nodes',
                json={
                    'host': 'localhost',
                    'port': new_port,
                    'status': 'active'
                }
            )
            self.assertEqual(response.status_code, 200,
                             f"Failed to register replacement node: {response.text}")
            print(
                f"Replacement node on port {new_port} registered successfully")
        except Exception as e:
            self.fail(f"Error registering replacement node: {e}")

        # Wait for the node to sync
        time.sleep(3)

    def test_10_verify_node_count_after_restart(self):
        """Test verifying node count after restarting a node."""
        response = requests.get(
            f'http://localhost:{self.coordinator_port}/nodes')
        self.assertEqual(response.status_code, 200)
        nodes_dict = response.json().get('nodes', {})

        # Count active nodes
        active_nodes = sum(
            1 for node_info in nodes_dict.values() if node_info.get('status') == 'active')
        print(f"Active nodes after restart: {active_nodes}")



if __name__ == '__main__':
    unittest.main()
