"""
Main coordinator service for the distributed database system.
"""
import os
import json
import threading
from typing import Dict, List, Optional, Any, Tuple
from flask import Flask, request, jsonify
from distributed_db.common.utils import get_logger, generate_id, ensure_directory_exists
from distributed_db.common.models import (
    DatabaseType, PartitionType, DatabaseSchema, TableSchema,
    ColumnDefinition, StorageNode, Partition, QueryResult
)
from distributed_db.common.config import (
    COORDINATOR_HOST, COORDINATOR_PORT, DEFAULT_STORAGE_PORT_START,
    DEFAULT_REPLICATION_FACTOR, DB_STORAGE_PATH
)
from distributed_db.coordinator.schema_manager import SchemaManager
from distributed_db.coordinator.cache_manager import CacheManager
from distributed_db.coordinator.consensus import ConsensusManager

logger = get_logger(__name__)


class Coordinator:
    """
    Main coordinator service for the distributed database system.
    Responsible for:
    - Managing database schemas
    - Routing client requests
    - Caching popular content
    - Managing consensus for distributed operations
    """

    def __init__(
        self,
        host: str = COORDINATOR_HOST,
        port: int = COORDINATOR_PORT,
        data_dir: str = DB_STORAGE_PATH,
        replication_factor: int = DEFAULT_REPLICATION_FACTOR
    ):
        """
        Initialize the coordinator.

        Args:
            host: Host to bind to
            port: Port to bind to
            data_dir: Directory to store data
            replication_factor: Number of replicas for each partition
        """
        self.host = host
        self.port = port
        self.data_dir = data_dir
        self.replication_factor = replication_factor

        # Ensure data directory exists
        ensure_directory_exists(data_dir)
        ensure_directory_exists(os.path.join(data_dir, "schemas"))

        # Initialize consensus manager
        self.consensus_address = f"{host}:{port}"
        self.consensus_manager = ConsensusManager(self.consensus_address, [])

        # Initialize schema manager
        self.schema_manager = SchemaManager(
            self.consensus_manager,
            schema_dir=os.path.join(data_dir, "schemas")
        )

        # Initialize cache manager
        self.cache_manager = CacheManager()

        # Initialize Flask app
        self.app = Flask(__name__)
        self._setup_routes()

        logger.info(f"Coordinator initialized at {host}:{port}")

    def _setup_routes(self):
        """Set up Flask routes."""
        # Database management
        self.app.route('/databases', methods=['GET'])(self.list_databases)
        self.app.route('/databases', methods=['POST'])(self.create_database)
        self.app.route('/databases/<db_name>', methods=['GET'])(self.get_database)
        self.app.route('/databases/<db_name>', methods=['DELETE'])(self.delete_database)

        # Storage node management
        self.app.route('/nodes', methods=['GET'])(self.list_storage_nodes)
        self.app.route('/nodes', methods=['POST'])(self.register_storage_node)
        self.app.route('/nodes/<node_id>', methods=['GET'])(self.get_storage_node)
        self.app.route('/nodes/<node_id>', methods=['DELETE'])(self.unregister_storage_node)

        # Query routing
        self.app.route('/query/<db_name>', methods=['POST'])(self.route_query)

        # Partition management
        self.app.route('/partitions', methods=['GET'])(self.list_partitions)
        self.app.route('/partitions/<db_name>', methods=['GET'])(self.get_database_partitions)

        # Cache management
        self.app.route('/cache/stats', methods=['GET'])(self.get_cache_stats)
        self.app.route('/cache/clear', methods=['POST'])(self.clear_cache)

        # Cluster management
        self.app.route('/cluster/join', methods=['POST'])(self.join_cluster)
        self.app.route('/cluster/status', methods=['GET'])(self.get_cluster_status)

    def start(self):
        """Start the coordinator service."""
        logger.info(f"Starting coordinator service at {self.host}:{self.port}")
        self.app.run(host=self.host, port=self.port)

    # Route handlers

    def list_databases(self):
        """List all databases."""
        # Get databases from consensus manager instead of schema manager
        databases = self.consensus_manager.get_all_databases()
        database_names = list(databases.keys())
        return jsonify({"success": True, "databases": database_names})

    def create_database(self):
        """Create a new database."""
        try:
            data = request.json

            # Validate request
            if not data or 'name' not in data or 'db_type' not in data:
                return jsonify({"success": False, "error": "Missing required fields"}), 400

            # Parse database type
            try:
                db_type = DatabaseType(data['db_type'])
            except ValueError:
                return jsonify({"success": False, "error": f"Invalid database type: {data['db_type']}"}), 400

            # Parse partition type
            partition_type = PartitionType.HORIZONTAL
            if 'partition_type' in data:
                try:
                    partition_type = PartitionType(data['partition_type'])
                except ValueError:
                    return jsonify({"success": False, "error": f"Invalid partition type: {data['partition_type']}"}), 400

            # Create tables for SQL databases
            tables = None
            if db_type == DatabaseType.SQL and 'tables' in data:
                tables = []
                for table_data in data['tables']:
                    if 'name' not in table_data or 'columns' not in table_data:
                        return jsonify({"success": False, "error": "Missing required fields for table"}), 400

                    columns = []
                    for column_data in table_data['columns']:
                        if 'name' not in column_data or 'data_type' not in column_data:
                            return jsonify({"success": False, "error": "Missing required fields for column"}), 400

                        column = ColumnDefinition(
                            name=column_data['name'],
                            data_type=column_data['data_type'],
                            primary_key=column_data.get('primary_key', False),
                            nullable=column_data.get('nullable', True),
                            default=column_data.get('default')
                        )
                        columns.append(column)

                    table = TableSchema(
                        name=table_data['name'],
                        columns=columns,
                        indexes=table_data.get('indexes', [])
                    )
                    tables.append(table)

            # Create database schema
            db_schema = DatabaseSchema(
                name=data['name'],
                db_type=db_type,
                tables=tables,
                partition_type=partition_type,
                partition_key=data.get('partition_key')
            )

            # Add to consensus store directly
            # We need to initialize the consensus manager's databases if it doesn't exist yet
            if not hasattr(self.consensus_manager, '_ConsensusManager__databases'):
                self.consensus_manager._ConsensusManager__databases = {}

            # Check if database already exists
            if data['name'] in self.consensus_manager._ConsensusManager__databases:
                return jsonify({"success": False, "error": f"Database '{data['name']}' already exists"}), 400

            # Create database info
            db_info = {
                'name': data['name'],
                'db_type': db_type.value,
                'partition_type': partition_type.value,
                'partition_key': data.get('partition_key'),
                'tables': []
            }

            # Add tables if present
            if tables:
                for table in tables:
                    table_dict = {
                        'name': table.name,
                        'columns': [],
                        'indexes': table.indexes or []
                    }

                    for column in table.columns:
                        column_dict = {
                            'name': column.name,
                            'data_type': column.data_type,
                            'primary_key': column.primary_key,
                            'nullable': column.nullable,
                            'default': column.default
                        }
                        table_dict['columns'].append(column_dict)

                    db_info['tables'].append(table_dict)

            # Add to consensus store
            self.consensus_manager._ConsensusManager__databases[data['name']] = db_info
            logger.info(f"Added database {data['name']} to consensus store")

            # Create initial partitions
            self._create_initial_partitions(db_schema)

            return jsonify({"success": True, "message": f"Database '{data['name']}' created successfully"})

        except Exception as e:
            logger.error(f"Error creating database: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    def get_database(self, db_name):
        """Get database schema."""
        # Get database from consensus manager
        if not hasattr(self.consensus_manager, '_ConsensusManager__databases'):
            return jsonify({"success": False, "error": f"Database '{db_name}' not found"}), 404

        db_info = self.consensus_manager._ConsensusManager__databases.get(db_name)

        if db_info is None:
            return jsonify({"success": False, "error": f"Database '{db_name}' not found"}), 404

        # Return the database info directly
        return jsonify({"success": True, "database": db_info})

    def delete_database(self, db_name):
        """Delete a database."""
        # Check if database exists in consensus manager
        if not hasattr(self.consensus_manager, '_ConsensusManager__databases') or db_name not in self.consensus_manager._ConsensusManager__databases:
            error = f"Database '{db_name}' does not exist"
            return jsonify({"success": False, "error": error}), 404

        # Remove from consensus store
        del self.consensus_manager._ConsensusManager__databases[db_name]
        logger.info(f"Removed database {db_name} from consensus store")

        # Also remove associated partitions if they exist
        if hasattr(self.consensus_manager, '_ConsensusManager__partitions'):
            partitions_to_remove = []
            for partition_id, partition_info in self.consensus_manager._ConsensusManager__partitions.items():
                if partition_info.get('database_name') == db_name:
                    partitions_to_remove.append(partition_id)

            for partition_id in partitions_to_remove:
                del self.consensus_manager._ConsensusManager__partitions[partition_id]
                logger.info(f"Removed partition {partition_id} for database {db_name}")

        # Clear cache entries for this database
        self.cache_manager.invalidate_by_prefix(f"db:{db_name}:")

        return jsonify({"success": True, "message": f"Database '{db_name}' deleted successfully"})

    def list_storage_nodes(self):
        """List all storage nodes."""
        nodes = self.consensus_manager.get_all_storage_nodes()
        return jsonify({"success": True, "nodes": nodes})

    def register_storage_node(self):
        """Register a new storage node."""
        try:
            data = request.json

            # Validate request
            if not data or 'host' not in data or 'port' not in data:
                return jsonify({"success": False, "error": "Missing required fields"}), 400

            # Generate node ID if not provided
            node_id = data.get('id', generate_id())

            # Create node info
            node_info = {
                'id': node_id,
                'host': data['host'],
                'port': data['port'],
                'status': data.get('status', 'active'),
                'databases': data.get('databases', [])
            }

            # Add to consensus store
            # We need to initialize the consensus manager's storage nodes if it doesn't exist yet
            if not hasattr(self.consensus_manager, '_ConsensusManager__storage_nodes'):
                self.consensus_manager._ConsensusManager__storage_nodes = {}

            # Check if node already exists
            if node_id in self.consensus_manager._ConsensusManager__storage_nodes:
                return jsonify({"success": False, "error": f"Storage node with ID '{node_id}' already exists"}), 400

            # Directly add to the storage nodes dictionary
            self.consensus_manager._ConsensusManager__storage_nodes[node_id] = node_info
            logger.info(f"Added storage node {node_id} to consensus store")

            return jsonify({
                "success": True,
                "message": f"Storage node registered successfully",
                "node_id": node_id
            })

        except Exception as e:
            logger.error(f"Error registering storage node: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    def get_storage_node(self, node_id):
        """Get storage node information."""
        node_info = self.consensus_manager.get_storage_node(node_id)

        if node_info is None:
            return jsonify({"success": False, "error": f"Storage node '{node_id}' not found"}), 404

        return jsonify({"success": True, "node": node_info})

    def unregister_storage_node(self, node_id):
        """Unregister a storage node."""
        node_info = self.consensus_manager.get_storage_node(node_id)

        if node_info is None:
            return jsonify({"success": False, "error": f"Storage node '{node_id}' not found"}), 404

        success = self.consensus_manager.remove_storage_node(node_id)

        if success:
            return jsonify({"success": True, "message": f"Storage node '{node_id}' unregistered successfully"})
        else:
            return jsonify({"success": False, "error": "Failed to unregister storage node"}), 500

    def route_query(self, db_name):
        """Route a query to the appropriate storage nodes."""
        try:
            data = request.json

            # Validate request
            if not data or 'operation' not in data:
                return jsonify({"success": False, "error": "Missing required fields"}), 400

            # Check if database exists
            db_schema = self.schema_manager.get_database_schema(db_name)
            if db_schema is None:
                return jsonify({"success": False, "error": f"Database '{db_name}' not found"}), 404

            # Check cache for read operations
            operation = data['operation'].lower()
            if operation == 'get' or operation == 'query':
                cache_key = f"db:{db_name}:{hash(json.dumps(data, sort_keys=True))}"
                cached_result = self.cache_manager.get(cache_key)

                if cached_result is not None:
                    logger.debug(f"Cache hit for query on database '{db_name}'")
                    return jsonify({"success": True, "data": cached_result, "cached": True})

            # Get partitions for this database
            partitions = self.consensus_manager.get_database_partitions(db_name)

            if not partitions:
                return jsonify({"success": False, "error": f"No partitions found for database '{db_name}'"}), 404

            # Determine which partitions to query based on the operation and data
            target_partitions = self._get_target_partitions(db_schema, partitions, data)

            if not target_partitions:
                # If no specific partitions are found, use all partitions
                target_partitions = list(partitions.keys())

            # Get storage nodes for the partitions
            storage_nodes = {}
            for partition_id in target_partitions:
                partition_info = partitions[partition_id]
                for node_id in partition_info.get('storage_nodes', []):
                    node_info = self.consensus_manager.get_storage_node(node_id)
                    if node_info and node_info.get('status') == 'active':
                        storage_nodes[node_id] = node_info

            if not storage_nodes:
                return jsonify({"success": False, "error": "No active storage nodes found for the query"}), 503

            # Execute query on storage nodes
            results = self._execute_query(db_name, data, storage_nodes)

            # Cache result for read operations
            if (operation == 'get' or operation == 'query') and results.get('success', False):
                cache_key = f"db:{db_name}:{hash(json.dumps(data, sort_keys=True))}"
                self.cache_manager.set(cache_key, results.get('data'), ttl=60)  # Cache for 1 minute

            return jsonify(results)

        except Exception as e:
            logger.error(f"Error routing query: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    def list_partitions(self):
        """List all partitions."""
        partitions = self.consensus_manager.get_all_partitions()
        return jsonify({"success": True, "partitions": partitions})

    def get_database_partitions(self, db_name):
        """Get partitions for a specific database."""
        # Check if database exists
        db_schema = self.schema_manager.get_database_schema(db_name)
        if db_schema is None:
            return jsonify({"success": False, "error": f"Database '{db_name}' not found"}), 404

        partitions = self.consensus_manager.get_database_partitions(db_name)
        return jsonify({"success": True, "partitions": partitions})

    def get_cache_stats(self):
        """Get cache statistics."""
        stats = self.cache_manager.get_stats()
        return jsonify({"success": True, "stats": stats})

    def clear_cache(self):
        """Clear the cache."""
        self.cache_manager.clear()
        return jsonify({"success": True, "message": "Cache cleared successfully"})

    def join_cluster(self):
        """Join a cluster."""
        try:
            data = request.json

            # Validate request
            if not data or 'coordinator_address' not in data:
                return jsonify({"success": False, "error": "Missing required fields"}), 400

            coordinator_address = data['coordinator_address']

            # Add to Raft cluster
            self.consensus_manager.add_node(coordinator_address)

            return jsonify({
                "success": True,
                "message": f"Joined cluster with coordinator at {coordinator_address}"
            })

        except Exception as e:
            logger.error(f"Error joining cluster: {e}")
            return jsonify({"success": False, "error": str(e)}), 500

    def get_cluster_status(self):
        """Get cluster status."""
        is_leader = self.consensus_manager.is_leader()
        leader = self.consensus_manager._getLeader()

        # Convert leader to string if it's not None
        leader_str = str(leader) if leader is not None else None

        return jsonify({
            "success": True,
            "is_leader": is_leader,
            "leader": leader_str,
            "address": self.consensus_address
        })

    # Helper methods

    def _create_initial_partitions(self, db_schema: DatabaseSchema) -> None:
        """
        Create initial partitions for a new database.

        Args:
            db_schema: Database schema
        """
        # Get all active storage nodes
        storage_nodes = self.consensus_manager.get_all_storage_nodes()
        active_nodes = [
            node_id for node_id, node_info in storage_nodes.items()
            if node_info.get('status') == 'active'
        ]

        if not active_nodes:
            logger.warning(f"No active storage nodes found for creating partitions for '{db_schema.name}'")
            return

        # Create partitions based on database type and partition type
        if db_schema.db_type == DatabaseType.SQL:
            if db_schema.partition_type == PartitionType.HORIZONTAL:
                # Create a single partition for now (can be split later)
                partition_id = generate_id()
                partition_info = {
                    'id': partition_id,
                    'database_name': db_schema.name,
                    'partition_type': PartitionType.HORIZONTAL.value,
                    'partition_key': db_schema.partition_key,
                    'partition_value': None,  # Full range
                    'storage_nodes': active_nodes[:self.replication_factor]
                }
                self.consensus_manager.add_partition(partition_id, partition_info)
                logger.info(f"Created horizontal partition for database '{db_schema.name}'")

            elif db_schema.partition_type == PartitionType.VERTICAL:
                # Create one partition per table
                for table in db_schema.tables:
                    partition_id = generate_id()
                    partition_info = {
                        'id': partition_id,
                        'database_name': db_schema.name,
                        'partition_type': PartitionType.VERTICAL.value,
                        'partition_key': None,
                        'columns': None,  # All columns
                        'table_name': table.name,
                        'storage_nodes': active_nodes[:self.replication_factor]
                    }
                    self.consensus_manager.add_partition(partition_id, partition_info)
                logger.info(f"Created vertical partitions for database '{db_schema.name}'")

            elif db_schema.partition_type == PartitionType.MIXED:
                # Create one partition per table for now
                for table in db_schema.tables:
                    partition_id = generate_id()
                    partition_info = {
                        'id': partition_id,
                        'database_name': db_schema.name,
                        'partition_type': PartitionType.MIXED.value,
                        'partition_key': db_schema.partition_key,
                        'partition_value': None,  # Full range
                        'columns': None,  # All columns
                        'table_name': table.name,
                        'storage_nodes': active_nodes[:self.replication_factor]
                    }
                    self.consensus_manager.add_partition(partition_id, partition_info)
                logger.info(f"Created mixed partitions for database '{db_schema.name}'")

        elif db_schema.db_type == DatabaseType.NOSQL:
            # For NoSQL, create a single partition
            partition_id = generate_id()
            partition_info = {
                'id': partition_id,
                'database_name': db_schema.name,
                'partition_type': PartitionType.VERTICAL.value,  # NoSQL only supports vertical partitioning
                'partition_key': None,
                'columns': None,  # All fields
                'storage_nodes': active_nodes[:self.replication_factor]
            }
            self.consensus_manager.add_partition(partition_id, partition_info)
            logger.info(f"Created partition for NoSQL database '{db_schema.name}'")

    def _get_target_partitions(
        self,
        db_schema: DatabaseSchema,
        partitions: Dict[str, Dict],
        query_data: Dict
    ) -> List[str]:
        """
        Determine which partitions to query based on the data.

        Args:
            db_schema: Database schema
            partitions: Dictionary of partitions
            query_data: Query data

        Returns:
            List of partition IDs to query
        """
        # For SQL databases
        if db_schema.db_type == DatabaseType.SQL:
            table_name = query_data.get('table')

            # If no table specified, return all partitions
            if not table_name:
                return list(partitions.keys())

            # Get partition type from database schema
            partition_type = db_schema.partition_type

            # For horizontal partitioning
            if partition_type == PartitionType.HORIZONTAL:
                partition_key = db_schema.partition_key
                where_clause = query_data.get('where', {})

                # If query has a filter on the partition key, use it to select partitions
                if partition_key and partition_key in where_clause:
                    target_partitions = []
                    for partition_id, partition_info in partitions.items():
                        partition_value = partition_info.get('partition_value')
                        # If partition value is None, it contains all values
                        if partition_value is None:
                            target_partitions.append(partition_id)
                        # If partition value matches the query value, include it
                        elif where_clause[partition_key] == partition_value:
                            target_partitions.append(partition_id)
                    return target_partitions
                # If no filter on partition key, include all partitions
                else:
                    return list(partitions.keys())

            # For vertical partitioning
            elif partition_type == PartitionType.VERTICAL:
                # For vertical partitioning, we need to query all partitions
                # that contain the table
                target_partitions = []
                for partition_id, partition_info in partitions.items():
                    if partition_info.get('table_name') == table_name or partition_info.get('table_name') is None:
                        target_partitions.append(partition_id)
                return target_partitions

            # For mixed partitioning
            elif partition_type == PartitionType.MIXED:
                # For mixed partitioning, we use both horizontal and vertical strategies
                partition_key = db_schema.partition_key
                where_clause = query_data.get('where', {})

                # If query has a filter on the partition key, use it to select partitions
                if partition_key and partition_key in where_clause:
                    target_partitions = []
                    for partition_id, partition_info in partitions.items():
                        if partition_info.get('table_name') == table_name or partition_info.get('table_name') is None:
                            partition_value = partition_info.get('partition_value')
                            # If partition value is None, it contains all values
                            if partition_value is None:
                                target_partitions.append(partition_id)
                            # If partition value matches the query value, include it
                            elif where_clause[partition_key] == partition_value:
                                target_partitions.append(partition_id)
                    return target_partitions
                # If no filter on partition key, include all partitions for the table
                else:
                    target_partitions = []
                    for partition_id, partition_info in partitions.items():
                        if partition_info.get('table_name') == table_name or partition_info.get('table_name') is None:
                            target_partitions.append(partition_id)
                    return target_partitions

            # Default: return all partitions
            return list(partitions.keys())

        # For NoSQL databases
        else:
            # NoSQL databases only support vertical partitioning
            # Return all partitions for the database
            return list(partitions.keys())

    def _execute_query(
        self,
        db_name: str,
        query_data: Dict,
        storage_nodes: Dict[str, Dict]
    ) -> Dict:
        """
        Execute a query on storage nodes.

        Args:
            db_name: Database name
            query_data: Query data
            storage_nodes: Dictionary of storage nodes

        Returns:
            Query result
        """
        # For read operations, query all nodes and combine results
        operation = query_data.get('operation', '').lower()
        if operation == 'get' or operation == 'query':
            all_data = []
            success = False
            error = None

            # Query each storage node
            for node_id, node_info in storage_nodes.items():
                # Construct URL
                url = f"http://{node_info['host']}:{node_info['port']}/query/{db_name}"

                # Make request
                try:
                    import requests
                    response = requests.post(url, json=query_data, timeout=5)
                    response.raise_for_status()
                    result = response.json()

                    if result.get('success', False):
                        success = True
                        if 'data' in result and result['data']:
                            all_data.extend(result['data'])
                    else:
                        error = result.get('error')
                        logger.warning(f"Query failed on node {node_id}: {error}")

                except Exception as e:
                    logger.error(f"Error executing query on storage node {node_id}: {e}")
                    error = str(e)

            # Return combined results
            if success:
                # Special case for NoSQL get by ID - return the first non-None result
                if operation == 'get' and 'id' in query_data and query_data['id'] is not None:
                    for item in all_data:
                        if item is not None and isinstance(item, dict):
                            logger.info(f"NoSQL get by ID result: {item}")
                            return {"success": True, "data": item}
                    # If no non-None results, return None
                    return {"success": True, "data": None}
                else:
                    logger.info(f"Combined query results: {all_data}")
                    return {"success": True, "data": all_data}
            else:
                logger.error(f"Failed to execute query on any node: {error}")
                return {"success": False, "error": error or "Failed to execute query on any node"}

        # For delete operations, send to all nodes to ensure consistency
        elif operation == 'delete':
            success = False
            error = None
            affected_rows = 0

            # Send delete to each storage node
            for node_id, node_info in storage_nodes.items():
                # Construct URL
                url = f"http://{node_info['host']}:{node_info['port']}/query/{db_name}"

                # Make request
                try:
                    import requests
                    response = requests.post(url, json=query_data, timeout=5)
                    response.raise_for_status()
                    result = response.json()

                    if result.get('success', False):
                        success = True
                        affected_rows += result.get('affected_rows', 0)
                    else:
                        error = result.get('error')
                        logger.warning(f"Delete failed on node {node_id}: {error}")

                except Exception as e:
                    logger.error(f"Error executing delete on storage node {node_id}: {e}")
                    error = str(e)

            # Invalidate cache for this database
            table_name = query_data.get('table')
            where_clause = query_data.get('where', {})
            if table_name and where_clause:
                # Construct a cache key pattern for invalidation
                cache_pattern = f"db:{db_name}:"

                # Invalidate all cache entries for this database
                invalidated = self.cache_manager.invalidate_by_prefix(cache_pattern)
                logger.info(f"Invalidated {invalidated} cache entries for database '{db_name}'")

            # Return combined results
            if success:
                logger.info(f"Delete operation affected {affected_rows} rows")
                return {"success": True, "affected_rows": affected_rows}
            else:
                logger.error(f"Failed to execute delete on any node: {error}")
                return {"success": False, "error": error or "Failed to execute delete on any node"}

        # For update operations, we need to handle NoSQL updates specially
        elif operation == 'update':
            # Check if this is a NoSQL update with a document ID
            is_nosql = 'document' in query_data and 'id' in query_data

            if is_nosql:
                # For NoSQL updates, we need to forward to the node and then get the updated document
                node_id, node_info = next(iter(storage_nodes.items()))

                # Construct URL
                url = f"http://{node_info['host']}:{node_info['port']}/query/{db_name}"

                # Make request
                try:
                    import requests
                    response = requests.post(url, json=query_data, timeout=5)
                    response.raise_for_status()
                    result = response.json()

                    # If the update was successful, return the updated document
                    if result.get('success', False):
                        # Invalidate cache for this database
                        cache_pattern = f"db:{db_name}:"
                        invalidated = self.cache_manager.invalidate_by_prefix(cache_pattern)
                        logger.info(f"Invalidated {invalidated} cache entries for database '{db_name}'")

                        return result
                    else:
                        logger.error(f"Failed to execute NoSQL update: {result.get('error')}")
                        return result
                except Exception as e:
                    logger.error(f"Error executing NoSQL update on storage node {node_id}: {e}")
                    return {"success": False, "error": f"Failed to execute NoSQL update: {str(e)}"}
            else:
                # For SQL updates, forward to the first node
                node_id, node_info = next(iter(storage_nodes.items()))

                # Construct URL
                url = f"http://{node_info['host']}:{node_info['port']}/query/{db_name}"

                # Make request
                try:
                    import requests
                    response = requests.post(url, json=query_data, timeout=5)
                    response.raise_for_status()
                    return response.json()
                except Exception as e:
                    logger.error(f"Error executing query on storage node {node_id}: {e}")
                    return {"success": False, "error": f"Failed to execute query: {str(e)}"}

        # For other write operations, forward to the first node
        else:
            node_id, node_info = next(iter(storage_nodes.items()))

            # Construct URL
            url = f"http://{node_info['host']}:{node_info['port']}/query/{db_name}"

            # Make request
            try:
                import requests
                response = requests.post(url, json=query_data, timeout=5)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                logger.error(f"Error executing query on storage node {node_id}: {e}")
                return {"success": False, "error": f"Failed to execute query: {str(e)}"}


def main():
    """Main entry point."""
    coordinator = Coordinator()
    coordinator.start()


if __name__ == "__main__":
    main()
