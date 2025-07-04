#!/usr/bin/env python3
"""
Command-line interface for the distributed database system.
Provides commands for starting the coordinator, storage nodes, and interacting with the database.
"""
import os
import sys
import argparse
import time
import json
from typing import Dict, List, Any, Optional

from distributed_db.coordinator.coordinator import Coordinator
from distributed_db.storage.storage_node import StorageNode
from distributed_db.client.client import DatabaseClient
from distributed_db.common.models import (
    DatabaseType, PartitionType, TableSchema, ColumnDefinition, QueryResult
)
from distributed_db.common.config import (
    COORDINATOR_HOST, COORDINATOR_PORT, DEFAULT_STORAGE_PORT_START,
    DB_STORAGE_PATH
)
from distributed_db.common.utils import get_logger, ensure_directory_exists

logger = get_logger(__name__)


def start_coordinator(args):
    """Start the coordinator service."""
    print(f"Starting coordinator at {args.host}:{args.port}...")

    # Ensure data directory exists
    ensure_directory_exists(args.data_dir)

    # Create and start coordinator
    coordinator = Coordinator(
        host=args.host,
        port=args.port,
        data_dir=args.data_dir,
        replication_factor=args.replication_factor
    )

    coordinator.start()


def start_storage_node(args):
    """Start a storage node."""
    print(f"Starting storage node at {args.host}:{args.port}...")

    # Ensure data directory exists
    ensure_directory_exists(args.data_dir)

    # Create and start storage node
    node = StorageNode(
        host=args.host,
        port=args.port,
        data_dir=args.data_dir,
        coordinator_url=args.coordinator,
        node_id=args.node_id
    )

    node.start()


def list_databases(args):
    """List all databases in the system."""
    client = DatabaseClient(args.coordinator)
    databases = client.list_databases()

    if not databases:
        print("No databases found.")
    else:
        print("Available databases:")
        for db_name in databases:
            # Get database details
            db_info = client.get_database(db_name)
            if db_info:
                db_type = db_info.get('db_type', 'unknown')
                print(f"  - {db_name} ({db_type})")
            else:
                print(f"  - {db_name}")


def create_database(args):
    """Create a new database."""
    client = DatabaseClient(args.coordinator)

    # Parse schema file if provided
    tables = []
    if args.schema_file:
        try:
            with open(args.schema_file, 'r') as f:
                schema_data = json.load(f)

            for table_data in schema_data.get('tables', []):
                columns = []
                for col in table_data.get('columns', []):
                    columns.append(ColumnDefinition(
                        name=col['name'],
                        data_type=col['data_type'],
                        primary_key=col.get('primary_key', False),
                        nullable=col.get('nullable', True),
                        default=col.get('default')
                    ))

                tables.append(TableSchema(
                    name=table_data['name'],
                    columns=columns,
                    indexes=table_data.get('indexes', [])
                ))
        except Exception as e:
            print(f"Error parsing schema file: {e}")
            return

    # Create database
    db_type = DatabaseType.SQL if args.type.upper() == 'SQL' else DatabaseType.NOSQL
    partition_type = PartitionType.HORIZONTAL
    if args.partition_type.upper() == 'VERTICAL':
        partition_type = PartitionType.VERTICAL
    elif args.partition_type.upper() == 'MIXED':
        partition_type = PartitionType.MIXED

    success, error = client.create_database(
        name=args.name,
        db_type=db_type,
        tables=tables,
        partition_type=partition_type,
        partition_key=args.partition_key
    )

    if success:
        print(f"Database '{args.name}' created successfully.")
    else:
        print(f"Error creating database: {error}")


def execute_query(args):
    """Execute a SQL or NoSQL query."""
    client = DatabaseClient(args.coordinator)

    if args.query_file:
        try:
            with open(args.query_file, 'r') as f:
                query_data = json.load(f)

            # Execute query based on operation type
            if args.operation == 'sql':
                operation = query_data.get('operation', '').lower()

                if operation == 'query' or operation == 'get':
                    result = client.sql_query(
                        args.db_name,
                        query_data.get('table'),
                        query_data.get('columns'),
                        query_data.get('where')
                    )
                elif operation == 'insert':
                    result = client.sql_insert(
                        args.db_name,
                        query_data.get('table'),
                        query_data.get('values', {})
                    )
                elif operation == 'update':
                    result = client.sql_update(
                        args.db_name,
                        query_data.get('table'),
                        query_data.get('values', {}),
                        query_data.get('where')
                    )
                elif operation == 'delete':
                    result = client.sql_delete(
                        args.db_name,
                        query_data.get('table'),
                        query_data.get('where')
                    )
                else:
                    print(f"Unsupported SQL operation: {operation}")
                    return
            else:  # NoSQL
                operation = query_data.get('operation', '').lower()

                if operation == 'get':
                    result = client.nosql_get(
                        args.db_name,
                        query_data.get('collection', 'default'),
                        query_data.get('id'),
                        query_data.get('where')
                    )
                elif operation == 'insert':
                    result = client.nosql_insert(
                        args.db_name,
                        query_data.get('document', {}),
                        query_data.get('collection', 'default')
                    )
                elif operation == 'update':
                    result = client.nosql_update(
                        args.db_name,
                        query_data.get('document', {}),
                        query_data.get('id'),
                        query_data.get('where'),
                        query_data.get('collection', 'default')
                    )
                elif operation == 'delete':
                    result = client.nosql_delete(
                        args.db_name,
                        query_data.get('id'),
                        query_data.get('where'),
                        query_data.get('collection', 'default')
                    )
                else:
                    print(f"Unsupported NoSQL operation: {operation}")
                    return

            # Display results
            if result.success:
                print("Query results:")
                if result.data:
                    if isinstance(result.data, list):
                        for item in result.data:
                            print(json.dumps(item, indent=2))
                    else:
                        print(json.dumps(result.data, indent=2))
                else:
                    print("No data returned")

                if hasattr(result, 'affected_rows') and result.affected_rows is not None:
                    print(f"Affected rows: {result.affected_rows}")
            else:
                print(f"Query error: {result.error}")

        except Exception as e:
            print(f"Error executing query: {e}")
            return
    else:
        # Interactive mode
        if args.operation == 'sql':
            # Simple SQL query
            table = input("Table name: ")
            columns_str = input("Columns (comma-separated, empty for all): ")
            columns = [col.strip() for col in columns_str.split(',')] if columns_str.strip() else None

            where_str = input("WHERE conditions (JSON format, empty for none): ")
            where = json.loads(where_str) if where_str.strip() else None

            result = client.sql_query(args.db_name, table, columns, where)
        else:
            # Simple NoSQL query
            collection = input("Collection name (default if empty): ") or 'default'
            doc_id = input("Document ID (empty for none): ") or None

            where_str = input("WHERE conditions (JSON format, empty for none): ")
            where = json.loads(where_str) if where_str.strip() else None

            result = client.nosql_get(args.db_name, collection, doc_id, where)

        # Display results
        if result.success:
            print("Query results:")
            if result.data:
                if isinstance(result.data, list):
                    for item in result.data:
                        print(json.dumps(item, indent=2))
                else:
                    print(json.dumps(result.data, indent=2))
            else:
                print("No data returned")

            if hasattr(result, 'affected_rows') and result.affected_rows is not None:
                print(f"Affected rows: {result.affected_rows}")
        else:
            print(f"Query error: {result.error}")


def main():
    """Main entry point for the CLI."""
    parser = argparse.ArgumentParser(
        description='Distributed Database CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start coordinator
  python cli.py coordinator --host localhost --port 5000

  # Start storage node
  python cli.py storage-node --host localhost --port 5100 --coordinator http://localhost:5000

  # List databases
  python cli.py list-databases --coordinator http://localhost:5000

  # Create SQL database
  python cli.py create-database --coordinator http://localhost:5000 --name mydb --type sql --schema-file schema.json

  # Execute SQL query
  python cli.py query --coordinator http://localhost:5000 --db-name mydb --operation sql
"""
    )

    subparsers = parser.add_subparsers(dest='command', help='Command to execute')

    # Coordinator command
    coord_parser = subparsers.add_parser('coordinator', help='Start coordinator service')
    coord_parser.add_argument('--host', default=COORDINATOR_HOST, help='Host to bind to')
    coord_parser.add_argument('--port', type=int, default=COORDINATOR_PORT, help='Port to bind to')
    coord_parser.add_argument('--data-dir', default=DB_STORAGE_PATH, help='Directory to store data')
    coord_parser.add_argument('--replication-factor', type=int, default=3, help='Replication factor')

    # Storage node command
    node_parser = subparsers.add_parser('storage-node', help='Start storage node')
    node_parser.add_argument('--host', default='localhost', help='Host to bind to')
    node_parser.add_argument('--port', type=int, default=DEFAULT_STORAGE_PORT_START, help='Port to bind to')
    node_parser.add_argument('--data-dir', default=DB_STORAGE_PATH, help='Directory to store data')
    node_parser.add_argument('--coordinator', default=f'http://{COORDINATOR_HOST}:{COORDINATOR_PORT}', help='Coordinator URL')
    node_parser.add_argument('--node-id', help='Node ID (generated if not provided)')

    # List databases command
    list_parser = subparsers.add_parser('list-databases', help='List all databases')
    list_parser.add_argument('--coordinator', default=f'http://{COORDINATOR_HOST}:{COORDINATOR_PORT}', help='Coordinator URL')

    # Create database command
    create_parser = subparsers.add_parser('create-database', help='Create a new database')
    create_parser.add_argument('--coordinator', default=f'http://{COORDINATOR_HOST}:{COORDINATOR_PORT}', help='Coordinator URL')
    create_parser.add_argument('--name', required=True, help='Database name')
    create_parser.add_argument('--type', choices=['sql', 'nosql'], default='sql', help='Database type')
    create_parser.add_argument('--schema-file', help='Path to schema JSON file')
    create_parser.add_argument('--partition-type', choices=['horizontal', 'vertical', 'mixed'], default='horizontal', help='Partition type')
    create_parser.add_argument('--partition-key', help='Partition key')

    # Query command
    query_parser = subparsers.add_parser('query', help='Execute a query')
    query_parser.add_argument('--coordinator', default=f'http://{COORDINATOR_HOST}:{COORDINATOR_PORT}', help='Coordinator URL')
    query_parser.add_argument('--db-name', required=True, help='Database name')
    query_parser.add_argument('--operation', choices=['sql', 'nosql'], required=True, help='Operation type')
    query_parser.add_argument('--query-file', help='Path to query JSON file')

    # Parse arguments
    args = parser.parse_args()

    # Execute command
    if args.command == 'coordinator':
        start_coordinator(args)
    elif args.command == 'storage-node':
        start_storage_node(args)
    elif args.command == 'list-databases':
        list_databases(args)
    elif args.command == 'create-database':
        create_database(args)
    elif args.command == 'query':
        execute_query(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
