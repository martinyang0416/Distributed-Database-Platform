"""
Main storage node service for the distributed database system.
"""
import os
import json
import threading
import sqlite3
from typing import Dict, List, Any, Optional, Tuple
from flask import Flask, request, jsonify, send_file
import requests
from distributed_db.common.utils import get_logger, generate_id, ensure_directory_exists
from distributed_db.common.models import (
    DatabaseType, PartitionType, QueryResult
)
from distributed_db.common.config import (
    DEFAULT_STORAGE_PORT_START, DB_STORAGE_PATH,
    SQL_DB_EXTENSION, NOSQL_DB_EXTENSION
)
from distributed_db.storage.sql_storage import SQLStorage
from distributed_db.storage.nosql_storage import NoSQLStorage
from distributed_db.storage.partition_manager import PartitionManager
from distributed_db.storage.replication_manager import ReplicationManager

logger = get_logger(__name__)


class StorageNode:
    """
    Main storage node service for the distributed database system.
    Responsible for:
    - Storing and retrieving data
    - Managing partitions
    - Replicating data
    """

    def __init__(
        self,
        host: str,
        port: int,
        data_dir: str,
        coordinator_url: str,
        node_id: Optional[str] = None
    ):
        """
        Initialize the storage node.

        Args:
            host: Host to bind to
            port: Port to bind to
            data_dir: Directory to store data
            coordinator_url: URL of the coordinator
            node_id: ID of this node (generated if not provided)
        """
        self.host = host
        self.port = port
        self.data_dir = data_dir
        self.coordinator_url = coordinator_url
        self.node_id = node_id or generate_id()

        # Ensure data directory exists
        ensure_directory_exists(data_dir)

        # Initialize storage engines
        self.sql_storage = SQLStorage(os.path.join(data_dir, "sql"))
        self.nosql_storage = NoSQLStorage(os.path.join(data_dir, "nosql"))

        # Initialize partition manager
        self.partition_manager = PartitionManager(data_dir)

        # Initialize replication manager
        self.replication_manager = ReplicationManager(data_dir, self.node_id, coordinator_url)

        # Initialize Flask app
        self.app = Flask(__name__)
        self._setup_routes()

        # Register with coordinator
        self._register_with_coordinator()

        logger.info(f"Storage node initialized at {host}:{port} with ID {self.node_id}")

    def _setup_routes(self):
        """Set up Flask routes."""
        # Database management
        self.app.route('/databases', methods=['GET'])(self.list_databases)
        self.app.route('/databases', methods=['POST'])(self.create_database)
        self.app.route('/databases/<db_name>', methods=['GET'])(self.get_database)
        self.app.route('/databases/<db_name>', methods=['DELETE'])(self.delete_database)

        # Database file access (for replication)
        self.app.route('/database/<db_name>/file', methods=['GET'])(self.get_database_file)
        self.app.route('/database/<db_name>/type', methods=['GET'])(self.get_database_type)

        # Query execution
        self.app.route('/query/<db_name>', methods=['POST'])(self.execute_query)

        # Partition management
        self.app.route('/partitions', methods=['GET'])(self.list_partitions)
        self.app.route('/partitions/<db_name>', methods=['GET'])(self.get_database_partitions)
        self.app.route('/partitions', methods=['POST'])(self.create_partition)
        self.app.route('/partitions/<partition_id>', methods=['DELETE'])(self.delete_partition)

        # Replication management
        self.app.route('/replications', methods=['GET'])(self.list_replications)
        self.app.route('/replications', methods=['POST'])(self.start_replication)
        self.app.route('/replications/<task_id>', methods=['GET'])(self.get_replication_status)
        self.app.route('/replications/<task_id>', methods=['DELETE'])(self.cancel_replication)

        # Node status
        self.app.route('/status', methods=['GET'])(self.get_status)

    def start(self):
        """Start the storage node service."""
        logger.info(f"Starting storage node service at {self.host}:{self.port}")
        self.app.run(host=self.host, port=self.port)

    def _register_with_coordinator(self):
        """Register this node with the coordinator."""
        try:
            data = {
                'id': self.node_id,
                'host': self.host,
                'port': self.port,
                'status': 'active'
            }

            response = requests.post(f"{self.coordinator_url}/nodes", json=data)
            response.raise_for_status()

            logger.info(f"Registered with coordinator at {self.coordinator_url}")

        except Exception as e:
            logger.error(f"Failed to register with coordinator: {e}")

    # Route handlers

    def list_databases(self):
        """List all databases."""
        sql_dbs = self._get_sql_databases()
        nosql_dbs = self._get_nosql_databases()

        return jsonify({
            "success": True,
            "databases": {
                "sql": sql_dbs,
                "nosql": nosql_dbs
            }
        })

    def create_database(self):
        """Create a new database."""
        try:
            data = request.json

            # Validate request
            if not data or 'name' not in data or 'db_type' not in data:
                return jsonify({"success": False, "error": "Missing required fields"}), 400

            db_name = data['name']
            db_type = data['db_type'].lower()

            if db_type == 'sql':
                # Create SQL database
                if 'schema' not in data:
                    return jsonify({"success": False, "error": "Schema is required for SQL databases"}), 400

                success, error = self.sql_storage.create_database(db_name, data['schema'])

            elif db_type == 'nosql':
                # Create NoSQL database
                success, error = self.nosql_storage.create_database(db_name)

            else:
                return jsonify({"success": False, "error": f"Invalid database type: {db_type}"}), 400

            if success:
                return jsonify({"success": True, "message": f"Database '{db_name}' created successfully"})
            else:
                return jsonify({"success": False, "error": error}), 400

        except Exception as e:
            logger.error(f"Error creating database: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    def get_database(self, db_name):
        """Get database information."""
        # Check if SQL database
        sql_path = os.path.join(self.sql_storage.data_dir, f"{db_name}{SQL_DB_EXTENSION}")
        if os.path.exists(sql_path):
            tables = self.sql_storage.get_tables(db_name)

            table_schemas = {}
            for table in tables:
                table_schemas[table] = self.sql_storage.get_table_schema(db_name, table)

            return jsonify({
                "success": True,
                "database": {
                    "name": db_name,
                    "type": "sql",
                    "tables": table_schemas
                }
            })

        # Check if NoSQL database
        nosql_path = os.path.join(self.nosql_storage.data_dir, f"{db_name}{NOSQL_DB_EXTENSION}")
        if os.path.exists(nosql_path):
            collections = self.nosql_storage.get_collections(db_name)

            collection_schemas = {}
            for collection in collections:
                collection_schemas[collection] = self.nosql_storage.get_collection_schema(db_name, collection)

            return jsonify({
                "success": True,
                "database": {
                    "name": db_name,
                    "type": "nosql",
                    "collections": collection_schemas
                }
            })

        return jsonify({"success": False, "error": f"Database '{db_name}' not found"}), 404

    def delete_database(self, db_name):
        """Delete a database."""
        # Check if SQL database
        sql_path = os.path.join(self.sql_storage.data_dir, f"{db_name}{SQL_DB_EXTENSION}")
        if os.path.exists(sql_path):
            success, error = self.sql_storage.delete_database(db_name)

            if success:
                return jsonify({"success": True, "message": f"SQL database '{db_name}' deleted successfully"})
            else:
                return jsonify({"success": False, "error": error}), 500

        # Check if NoSQL database
        nosql_path = os.path.join(self.nosql_storage.data_dir, f"{db_name}{NOSQL_DB_EXTENSION}")
        if os.path.exists(nosql_path):
            success, error = self.nosql_storage.delete_database(db_name)

            if success:
                return jsonify({"success": True, "message": f"NoSQL database '{db_name}' deleted successfully"})
            else:
                return jsonify({"success": False, "error": error}), 500

        return jsonify({"success": False, "error": f"Database '{db_name}' not found"}), 404

    def get_database_file(self, db_name):
        """Get database file (for replication)."""
        # Check if SQL database
        sql_path = os.path.join(self.sql_storage.data_dir, f"{db_name}{SQL_DB_EXTENSION}")
        if os.path.exists(sql_path):
            return send_file(sql_path, as_attachment=True, download_name=f"{db_name}{SQL_DB_EXTENSION}")

        # Check if NoSQL database
        nosql_path = os.path.join(self.nosql_storage.data_dir, f"{db_name}{NOSQL_DB_EXTENSION}")
        if os.path.exists(nosql_path):
            return send_file(nosql_path, as_attachment=True, download_name=f"{db_name}{NOSQL_DB_EXTENSION}")

        return jsonify({"success": False, "error": f"Database '{db_name}' not found"}), 404

    def get_database_type(self, db_name):
        """Get database type."""
        # Check if SQL database
        sql_path = os.path.join(self.sql_storage.data_dir, f"{db_name}{SQL_DB_EXTENSION}")
        if os.path.exists(sql_path):
            return jsonify({"success": True, "type": "sql"})

        # Check if NoSQL database
        nosql_path = os.path.join(self.nosql_storage.data_dir, f"{db_name}{NOSQL_DB_EXTENSION}")
        if os.path.exists(nosql_path):
            return jsonify({"success": True, "type": "nosql"})

        return jsonify({"success": False, "error": f"Database '{db_name}' not found"}), 404

    def execute_query(self, db_name):
        """Execute a query on a database."""
        try:
            query_data = request.json
            logger.info(f"Received query for database '{db_name}': {query_data}")

            # Validate request
            if not query_data or 'operation' not in query_data:
                return jsonify({"success": False, "error": "Missing required fields"}), 400

            # Get partitions for this database
            partitions = self.partition_manager.get_database_partitions(db_name)
            logger.info(f"Partitions for database '{db_name}': {partitions}")

            # Always try to execute the query, even if we don't have partitions for this database
            # This is necessary for vertical partitioning where each node might have different columns

            # Check if SQL database
            sql_path = os.path.join(self.sql_storage.data_dir, f"{db_name}{SQL_DB_EXTENSION}")
            if os.path.exists(sql_path):
                logger.info(f"Executing SQL query on database '{db_name}'")
                result = self.sql_storage.execute_query(db_name, query_data)
                logger.info(f"SQL query result: {result.data}")
                return jsonify({
                    "success": result.success,
                    "data": result.data,
                    "error": result.error,
                    "affected_rows": result.affected_rows
                })

            # Check if NoSQL database
            nosql_path = os.path.join(self.nosql_storage.data_dir, f"{db_name}{NOSQL_DB_EXTENSION}")
            if os.path.exists(nosql_path):
                logger.info(f"Executing NoSQL query on database '{db_name}'")
                result = self.nosql_storage.execute_query(db_name, query_data)
                logger.info(f"NoSQL query result: {result.data}")
                return jsonify({
                    "success": result.success,
                    "data": result.data,
                    "error": result.error,
                    "affected_rows": result.affected_rows
                })

            # If we get here, the database doesn't exist on this node
            # For insert operations, create the database and execute the query
            operation = query_data.get('operation', '').lower()

            if operation == 'insert':
                # For insert operations, create the database and execute the query
                logger.info(f"Database '{db_name}' doesn't exist, creating it for insert operation")

                # Determine database type based on partitions
                db_type = None
                if isinstance(partitions, dict):
                    for partition in partitions.values():
                        # Check for NoSQL database type
                        if partition.get('partition_type') == 'VERTICAL' and 'collection' in query_data:
                            db_type = 'nosql'
                            break

                # If we have a 'collection' in the query data, it's likely a NoSQL operation
                if 'collection' in query_data:
                    db_type = 'nosql'
                # Default to SQL if not specified
                elif db_type is None:
                    db_type = 'sql'

                if db_type == 'sql':
                    # Create SQL database
                    os.makedirs(os.path.dirname(sql_path), exist_ok=True)
                    conn = sqlite3.connect(sql_path)
                    conn.close()
                    logger.info(f"Created SQL database '{db_name}'")

                    # Execute the query
                    result = self.sql_storage.execute_query(db_name, query_data)
                    logger.info(f"SQL query result after creating database: {result.data}")
                    return jsonify({
                        "success": result.success,
                        "data": result.data,
                        "error": result.error,
                        "affected_rows": result.affected_rows
                    })
                else:
                    # Create NoSQL database properly using the NoSQL storage implementation
                    success, error = self.nosql_storage.create_database(db_name)
                    if not success:
                        logger.error(f"Failed to create NoSQL database '{db_name}': {error}")
                        return jsonify({
                            "success": False,
                            "error": f"Failed to create NoSQL database: {error}"
                        }), 500

                    logger.info(f"Created NoSQL database '{db_name}'")

                    # Execute the query
                    result = self.nosql_storage.execute_query(db_name, query_data)
                    logger.info(f"NoSQL query result after creating database: {result.data}")
                    return jsonify({
                        "success": result.success,
                        "data": result.data,
                        "error": result.error,
                        "affected_rows": result.affected_rows
                    })
            elif operation == 'query' or operation == 'get':
                # For query operations, return empty result set
                logger.info(f"Database '{db_name}' doesn't exist, returning empty result set for query operation")
                return jsonify({
                    "success": True,
                    "data": [],
                    "error": None,
                    "affected_rows": 0
                })
            elif operation == 'update' or operation == 'delete':
                # For update/delete operations, return success with 0 affected rows
                logger.info(f"Database '{db_name}' doesn't exist, returning success for update/delete operation")
                return jsonify({
                    "success": True,
                    "data": None,
                    "error": None,
                    "affected_rows": 0
                })
            else:
                # For unknown operations, return error
                logger.error(f"Unsupported operation: {operation}")
                return jsonify({"success": False, "error": f"Unsupported operation: {operation}"}), 400

        except Exception as e:
            logger.error(f"Error executing query: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    def list_partitions(self):
        """List all partitions."""
        partitions = self.partition_manager.list_partitions()
        return jsonify({"success": True, "partitions": partitions})

    def get_database_partitions(self, db_name):
        """Get partitions for a specific database."""
        partitions = self.partition_manager.get_database_partitions(db_name)
        return jsonify({"success": True, "partitions": partitions})

    def create_partition(self):
        """Create a new partition."""
        try:
            data = request.json

            # Validate request
            if not data or 'id' not in data or 'database_name' not in data or 'partition_type' not in data:
                return jsonify({"success": False, "error": "Missing required fields"}), 400

            # Parse partition type
            try:
                partition_type = PartitionType(data['partition_type'])
            except ValueError:
                return jsonify({"success": False, "error": f"Invalid partition type: {data['partition_type']}"}), 400

            # Create partition
            partition_info = {k: v for k, v in data.items() if k not in ['id', 'database_name', 'partition_type']}

            success, error = self.partition_manager.create_partition(
                data['id'],
                data['database_name'],
                partition_type,
                partition_info
            )

            if success:
                return jsonify({"success": True, "message": f"Partition '{data['id']}' created successfully"})
            else:
                return jsonify({"success": False, "error": error}), 400

        except Exception as e:
            logger.error(f"Error creating partition: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    def delete_partition(self, partition_id):
        """Delete a partition."""
        success, error = self.partition_manager.delete_partition(partition_id)

        if success:
            return jsonify({"success": True, "message": f"Partition '{partition_id}' deleted successfully"})
        else:
            return jsonify({"success": False, "error": error}), 404 if "does not exist" in error else 500

    def list_replications(self):
        """List all replication tasks."""
        tasks = self.replication_manager.list_replication_tasks()
        return jsonify({"success": True, "tasks": tasks})

    def start_replication(self):
        """Start a replication task."""
        try:
            data = request.json

            # Validate request
            if not data or 'db_name' not in data or 'source_node' not in data:
                return jsonify({"success": False, "error": "Missing required fields"}), 400

            # Start replication
            success, error = self.replication_manager.replicate_database(
                data['db_name'],
                data['source_node']
            )

            if success:
                return jsonify({"success": True, "message": f"Replication of database '{data['db_name']}' started"})
            else:
                return jsonify({"success": False, "error": error}), 400

        except Exception as e:
            logger.error(f"Error starting replication: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    def get_replication_status(self, task_id):
        """Get the status of a replication task."""
        task_info = self.replication_manager.get_replication_status(task_id)

        if task_info:
            return jsonify({"success": True, "task": task_info})
        else:
            return jsonify({"success": False, "error": f"Replication task '{task_id}' not found"}), 404

    def cancel_replication(self, task_id):
        """Cancel a replication task."""
        success, error = self.replication_manager.cancel_replication(task_id)

        if success:
            return jsonify({"success": True, "message": f"Replication task '{task_id}' cancelled"})
        else:
            return jsonify({"success": False, "error": error}), 404 if "not found" in error else 400

    def get_status(self):
        """Get node status."""
        sql_dbs = self._get_sql_databases()
        nosql_dbs = self._get_nosql_databases()
        partitions = self.partition_manager.list_partitions()

        return jsonify({
            "success": True,
            "node_id": self.node_id,
            "host": self.host,
            "port": self.port,
            "databases": {
                "sql": sql_dbs,
                "nosql": nosql_dbs
            },
            "partitions": len(partitions)
        })

    # Helper methods

    def _get_sql_databases(self) -> List[str]:
        """
        Get a list of SQL databases.

        Returns:
            List of database names
        """
        databases = []

        if os.path.exists(self.sql_storage.data_dir):
            for filename in os.listdir(self.sql_storage.data_dir):
                if filename.endswith(SQL_DB_EXTENSION):
                    databases.append(filename[:-len(SQL_DB_EXTENSION)])

        return databases

    def _get_nosql_databases(self) -> List[str]:
        """
        Get a list of NoSQL databases.

        Returns:
            List of database names
        """
        databases = []

        if os.path.exists(self.nosql_storage.data_dir):
            for filename in os.listdir(self.nosql_storage.data_dir):
                if filename.endswith(NOSQL_DB_EXTENSION):
                    databases.append(filename[:-len(NOSQL_DB_EXTENSION)])

        return databases


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description='Start a storage node')
    parser.add_argument('--host', default='localhost', help='Host to bind to')
    parser.add_argument('--port', type=int, default=0, help='Port to bind to (0 for auto)')
    parser.add_argument('--data-dir', default=DB_STORAGE_PATH, help='Directory to store data')
    parser.add_argument('--coordinator', default='http://localhost:5000', help='Coordinator URL')
    parser.add_argument('--node-id', help='Node ID (generated if not provided)')

    args = parser.parse_args()

    # Auto-assign port if not specified
    if args.port == 0:
        import socket

        # Find an available port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            args.port = s.getsockname()[1]

    # Create data directory
    os.makedirs(args.data_dir, exist_ok=True)

    # Start storage node
    node = StorageNode(
        host=args.host,
        port=args.port,
        data_dir=args.data_dir,
        coordinator_url=args.coordinator,
        node_id=args.node_id
    )

    node.start()


if __name__ == "__main__":
    main()
