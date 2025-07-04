"""
Client interface for the distributed database system.
"""
from typing import Dict, List, Any, Optional, Tuple, Union
import requests
from distributed_db.common.utils import get_logger
from distributed_db.common.models import (
    DatabaseType, PartitionType, DatabaseSchema, TableSchema,
    ColumnDefinition, QueryResult
)
from distributed_db.client.query_router import QueryRouter

logger = get_logger(__name__)


class DatabaseClient:
    """
    Client interface for the distributed database system.
    """

    def __init__(self, coordinator_url: str):
        """
        Initialize the database client.

        Args:
            coordinator_url: URL of the coordinator
        """
        self.coordinator_url = coordinator_url
        self.query_router = QueryRouter(coordinator_url)
        logger.info(f"Database client initialized with coordinator URL {coordinator_url}")

    # Database management

    def create_database(
        self,
        name: str,
        db_type: DatabaseType,
        tables: Optional[List[TableSchema]] = None,
        partition_type: Optional[PartitionType] = None,
        partition_key: Optional[str] = None
    ) -> Tuple[bool, Optional[str]]:
        """
        Create a new database.

        Args:
            name: Name of the database
            db_type: Type of database (SQL or NoSQL)
            tables: List of table schemas (for SQL databases)
            partition_type: Type of partitioning
            partition_key: Key to partition by

        Returns:
            Tuple of (success, error_message)
        """
        # Set default partition type based on database type
        if partition_type is None:
            if db_type == DatabaseType.SQL:
                partition_type = PartitionType.HORIZONTAL
            else:
                partition_type = PartitionType.VERTICAL

        # Prepare request data
        data = {
            'name': name,
            'db_type': db_type.value,
            'partition_type': partition_type.value
        }

        # Only include partition_key if it's provided and not None
        if partition_key is not None:
            data['partition_key'] = partition_key

        # Add tables for SQL databases
        if db_type == DatabaseType.SQL and tables:
            data['tables'] = []

            for table in tables:
                table_data = {
                    'name': table.name,
                    'columns': [],
                    'indexes': table.indexes or []
                }

                for column in table.columns:
                    column_data = {
                        'name': column.name,
                        'data_type': column.data_type,
                        'primary_key': column.primary_key,
                        'nullable': column.nullable,
                        'default': column.default
                    }
                    table_data['columns'].append(column_data)

                data['tables'].append(table_data)

        # Send request to coordinator
        try:
            response = requests.post(f"{self.coordinator_url}/databases", json=data)
            response.raise_for_status()
            result = response.json()

            if result.get('success', False):
                logger.info(f"Created database '{name}'")

                # Wait for the database to be fully ready (partitions created)
                import time
                max_retries = 3
                for retry in range(max_retries):
                    try:
                        # Check if the database has partitions
                        partitions = self.query_router.get_database_partitions(name)
                        if partitions:
                            logger.info(f"Database '{name}' is ready with {len(partitions)} partitions")
                            break

                        # If no partitions yet, wait and retry
                        logger.info(f"Waiting for database '{name}' to be fully ready (attempt {retry + 1}/{max_retries})")
                        time.sleep(1)
                    except Exception as e:
                        logger.warning(f"Error checking database readiness: {e}")
                        time.sleep(1)

                return True, None
            else:
                error = result.get('error', 'Unknown error')
                logger.error(f"Failed to create database '{name}': {error}")
                return False, error

        except Exception as e:
            logger.error(f"Error creating database '{name}': {e}")
            return False, str(e)

    def list_databases(self) -> List[str]:
        """
        List all databases.

        Returns:
            List of database names
        """
        try:
            response = requests.get(f"{self.coordinator_url}/databases")
            response.raise_for_status()
            result = response.json()

            if result.get('success', False):
                return result.get('databases', [])
            else:
                logger.error(f"Failed to list databases: {result.get('error')}")
                return []

        except Exception as e:
            logger.error(f"Error listing databases: {e}")
            return []

    def get_database(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get database information.

        Args:
            name: Name of the database

        Returns:
            Database information if found, None otherwise
        """
        try:
            response = requests.get(f"{self.coordinator_url}/databases/{name}")

            if response.status_code == 404:
                logger.error(f"Database '{name}' not found")
                return None

            response.raise_for_status()
            result = response.json()

            if result.get('success', False):
                return result.get('database')
            else:
                logger.error(f"Failed to get database '{name}': {result.get('error')}")
                return None

        except Exception as e:
            logger.error(f"Error getting database '{name}': {e}")
            return None

    def delete_database(self, name: str) -> Tuple[bool, Optional[str]]:
        """
        Delete a database.

        Args:
            name: Name of the database

        Returns:
            Tuple of (success, error_message)
        """
        try:
            response = requests.delete(f"{self.coordinator_url}/databases/{name}")

            if response.status_code == 404:
                return False, f"Database '{name}' not found"

            response.raise_for_status()
            result = response.json()

            if result.get('success', False):
                logger.info(f"Deleted database '{name}'")
                return True, None
            else:
                error = result.get('error', 'Unknown error')
                logger.error(f"Failed to delete database '{name}': {error}")
                return False, error

        except Exception as e:
            logger.error(f"Error deleting database '{name}': {e}")
            return False, str(e)

    # SQL database operations

    def sql_query(
        self,
        db_name: str,
        table: str,
        columns: List[str] = None,
        where: Dict[str, Any] = None,
        order_by: str = None,
        limit: int = None
    ) -> QueryResult:
        """
        Execute a SQL query.

        Args:
            db_name: Name of the database
            table: Name of the table
            columns: List of columns to select (None for all)
            where: Dictionary of column-value pairs for WHERE clause
            order_by: Column to order by
            limit: Maximum number of rows to return

        Returns:
            Query result
        """
        query_data = {
            'operation': 'query',
            'table': table,
            'columns': columns or ['*'],
            'where': where or {},
            'order_by': order_by,
            'limit': limit
        }

        result = self.query_router.route_query(db_name, query_data)

        if result.get('success', False):
            data = result.get('data', [])
            logger.info(f"Query result from coordinator: {data}")
            return QueryResult(
                success=True,
                data=data,
                affected_rows=result.get('affected_rows', 0)
            )
        else:
            error = result.get('error', 'Unknown error')
            logger.error(f"Query error from coordinator: {error}")
            return QueryResult(
                success=False,
                error=error
            )

    def sql_insert(
        self,
        db_name: str,
        table: str,
        values: Dict[str, Any]
    ) -> QueryResult:
        """
        Insert a row into a SQL table.

        Args:
            db_name: Name of the database
            table: Name of the table
            values: Dictionary of column-value pairs

        Returns:
            Query result
        """
        query_data = {
            'operation': 'insert',
            'table': table,
            'values': values
        }

        result = self.query_router.route_query(db_name, query_data)

        if result.get('success', False):
            return QueryResult(
                success=True,
                data=result.get('data'),
                affected_rows=result.get('affected_rows', 0)
            )
        else:
            return QueryResult(
                success=False,
                error=result.get('error', 'Unknown error')
            )

    def sql_update(
        self,
        db_name: str,
        table: str,
        values: Dict[str, Any],
        where: Dict[str, Any] = None
    ) -> QueryResult:
        """
        Update rows in a SQL table.

        Args:
            db_name: Name of the database
            table: Name of the table
            values: Dictionary of column-value pairs to update
            where: Dictionary of column-value pairs for WHERE clause

        Returns:
            Query result
        """
        query_data = {
            'operation': 'update',
            'table': table,
            'values': values,
            'where': where or {}
        }

        result = self.query_router.route_query(db_name, query_data)

        if result.get('success', False):
            return QueryResult(
                success=True,
                affected_rows=result.get('affected_rows', 0)
            )
        else:
            return QueryResult(
                success=False,
                error=result.get('error', 'Unknown error')
            )

    def sql_delete(
        self,
        db_name: str,
        table: str,
        where: Dict[str, Any] = None
    ) -> QueryResult:
        """
        Delete rows from a SQL table.

        Args:
            db_name: Name of the database
            table: Name of the table
            where: Dictionary of column-value pairs for WHERE clause

        Returns:
            Query result
        """
        query_data = {
            'operation': 'delete',
            'table': table,
            'where': where or {}
        }

        result = self.query_router.route_query(db_name, query_data)

        if result.get('success', False):
            return QueryResult(
                success=True,
                affected_rows=result.get('affected_rows', 0)
            )
        else:
            return QueryResult(
                success=False,
                error=result.get('error', 'Unknown error')
            )

    # NoSQL database operations

    def nosql_get(
        self,
        db_name: str,
        collection: str = 'default',
        doc_id: Any = None,
        where: Dict[str, Any] = None
    ) -> QueryResult:
        """
        Get documents from a NoSQL collection.

        Args:
            db_name: Name of the database
            collection: Name of the collection
            doc_id: Document ID (if getting a specific document)
            where: Dictionary of field-value pairs to filter by

        Returns:
            Query result
        """
        query_data = {
            'operation': 'get',
            'collection': collection,
            'id': doc_id,
            'where': where or {}
        }

        result = self.query_router.route_query(db_name, query_data)

        # Add debug logging
        logger.info(f"NoSQL get result from coordinator: {result}")

        if result.get('success', False):
            data = result.get('data')
            logger.info(f"NoSQL get data: {data}")
            return QueryResult(
                success=True,
                data=data
            )
        else:
            return QueryResult(
                success=False,
                error=result.get('error', 'Unknown error')
            )

    def nosql_insert(
        self,
        db_name: str,
        document: Dict[str, Any],
        collection: str = 'default'
    ) -> QueryResult:
        """
        Insert a document into a NoSQL collection.

        Args:
            db_name: Name of the database
            document: Document to insert
            collection: Name of the collection

        Returns:
            Query result
        """
        query_data = {
            'operation': 'insert',
            'collection': collection,
            'document': document
        }

        result = self.query_router.route_query(db_name, query_data)

        if result.get('success', False):
            return QueryResult(
                success=True,
                data=result.get('data'),
                affected_rows=1
            )
        else:
            return QueryResult(
                success=False,
                error=result.get('error', 'Unknown error')
            )

    def nosql_update(
        self,
        db_name: str,
        document: Dict[str, Any],
        doc_id: Any = None,
        where: Dict[str, Any] = None,
        collection: str = 'default'
    ) -> QueryResult:
        """
        Update documents in a NoSQL collection.

        Args:
            db_name: Name of the database
            document: Document fields to update
            doc_id: Document ID (if updating a specific document)
            where: Dictionary of field-value pairs to filter by
            collection: Name of the collection

        Returns:
            Query result
        """
        query_data = {
            'operation': 'update',
            'collection': collection,
            'document': document,
            'id': doc_id,
            'where': where or {}
        }

        result = self.query_router.route_query(db_name, query_data)

        # Add debug logging
        logger.info(f"NoSQL update result from coordinator: {result}")

        if result.get('success', False):
            data = result.get('data')
            logger.info(f"NoSQL update data: {data}")
            return QueryResult(
                success=True,
                data=data,
                affected_rows=result.get('affected_rows', 0)
            )
        else:
            return QueryResult(
                success=False,
                error=result.get('error', 'Unknown error')
            )

    def nosql_delete(
        self,
        db_name: str,
        doc_id: Any = None,
        where: Dict[str, Any] = None,
        collection: str = 'default'
    ) -> QueryResult:
        """
        Delete documents from a NoSQL collection.

        Args:
            db_name: Name of the database
            doc_id: Document ID (if deleting a specific document)
            where: Dictionary of field-value pairs to filter by
            collection: Name of the collection

        Returns:
            Query result
        """
        query_data = {
            'operation': 'delete',
            'collection': collection,
            'id': doc_id,
            'where': where or {}
        }

        result = self.query_router.route_query(db_name, query_data)

        if result.get('success', False):
            return QueryResult(
                success=True,
                affected_rows=result.get('affected_rows', 0)
            )
        else:
            return QueryResult(
                success=False,
                error=result.get('error', 'Unknown error')
            )

    # Utility methods

    def get_storage_nodes(self) -> List[Dict[str, Any]]:
        """
        Get a list of all storage nodes.

        Returns:
            List of storage node information
        """
        return self.query_router.get_storage_nodes()

    def get_database_partitions(self, db_name: str) -> List[Dict[str, Any]]:
        """
        Get a list of partitions for a database.

        Args:
            db_name: Name of the database

        Returns:
            List of partition information
        """
        return self.query_router.get_database_partitions(db_name)
