"""
NoSQL storage implementation for storage nodes.
"""
import os
from typing import Dict, List, Any, Optional, Tuple
from tinydb import TinyDB, Query, where
from tinydb.operations import set as tinydb_set
from distributed_db.common.utils import get_logger, ensure_directory_exists
from distributed_db.common.models import QueryResult
from distributed_db.common.config import NOSQL_DB_EXTENSION

logger = get_logger(__name__)


class NoSQLStorage:
    """
    Implements storage for NoSQL databases using TinyDB.
    """

    def __init__(self, data_dir: str):
        """
        Initialize the NoSQL storage.

        Args:
            data_dir: Directory to store database files
        """
        self.data_dir = data_dir
        ensure_directory_exists(data_dir)
        logger.info(f"NoSQL storage initialized with data directory {data_dir}")

    def create_database(self, db_name: str) -> Tuple[bool, Optional[str]]:
        """
        Create a new database.

        Args:
            db_name: Name of the database

        Returns:
            Tuple of (success, error_message)
        """
        db_path = os.path.join(self.data_dir, f"{db_name}{NOSQL_DB_EXTENSION}")

        # Check if database already exists
        if os.path.exists(db_path):
            return False, f"Database '{db_name}' already exists"

        try:
            # Create database file
            db = TinyDB(db_path)
            db.close()

            logger.info(f"Created NoSQL database '{db_name}'")
            return True, None

        except Exception as e:
            logger.error(f"Failed to create NoSQL database '{db_name}': {e}")

            # Clean up if database file was created
            if os.path.exists(db_path):
                try:
                    os.remove(db_path)
                except:
                    pass

            return False, str(e)

    def delete_database(self, db_name: str) -> Tuple[bool, Optional[str]]:
        """
        Delete a database.

        Args:
            db_name: Name of the database

        Returns:
            Tuple of (success, error_message)
        """
        db_path = os.path.join(self.data_dir, f"{db_name}{NOSQL_DB_EXTENSION}")

        # Check if database exists
        if not os.path.exists(db_path):
            return False, f"Database '{db_name}' does not exist"

        try:
            # Delete database file
            os.remove(db_path)

            logger.info(f"Deleted NoSQL database '{db_name}'")
            return True, None

        except Exception as e:
            logger.error(f"Failed to delete NoSQL database '{db_name}': {e}")
            return False, str(e)

    def execute_query(self, db_name: str, query_data: Dict[str, Any]) -> QueryResult:
        """
        Execute a query on a database.

        Args:
            db_name: Name of the database
            query_data: Query data

        Returns:
            Query result
        """
        db_path = os.path.join(self.data_dir, f"{db_name}{NOSQL_DB_EXTENSION}")

        # Check if database exists
        if not os.path.exists(db_path):
            return QueryResult(success=False, error=f"Database '{db_name}' does not exist")

        try:
            # Connect to database
            db = TinyDB(db_path)

            operation = query_data.get('operation', '').lower()
            collection = query_data.get('collection', 'default')
            table = db.table(collection)

            # Execute query based on operation
            if operation == 'get' or operation == 'query':
                # GET query
                doc_id = query_data.get('id')

                if doc_id is not None:
                    # Get by ID
                    result = table.get(doc_id=doc_id)
                    # Return the document directly, not in a list
                    return QueryResult(success=True, data=result)

                # Query by conditions
                conditions = query_data.get('where', {})

                if conditions:
                    # Build query
                    q = Query()
                    query_obj = None

                    for key, value in conditions.items():
                        if query_obj is None:
                            query_obj = (q[key] == value)
                        else:
                            query_obj &= (q[key] == value)

                    result = table.search(query_obj)
                else:
                    # Get all documents
                    result = table.all()

                return QueryResult(success=True, data=result)

            elif operation == 'insert':
                # INSERT query
                document = query_data.get('document', {})

                if not document:
                    return QueryResult(success=False, error="Document is required for INSERT operation")

                # Insert document
                doc_id = table.insert(document)

                return QueryResult(success=True, data={"id": doc_id}, affected_rows=1)

            elif operation == 'update':
                # UPDATE query
                doc_id = query_data.get('id')
                document = query_data.get('document', {})
                conditions = query_data.get('where', {})

                if not document:
                    return QueryResult(success=False, error="Document is required for UPDATE operation")

                if doc_id is not None:
                    # Update by ID
                    table.update(document, doc_ids=[doc_id])
                    # Get the updated document to return
                    updated_doc = table.get(doc_id=doc_id)
                    return QueryResult(success=True, data=updated_doc, affected_rows=1)

                # Update by conditions
                if conditions:
                    # Build query
                    q = Query()
                    query_obj = None

                    for key, value in conditions.items():
                        if query_obj is None:
                            query_obj = (q[key] == value)
                        else:
                            query_obj &= (q[key] == value)

                    # Update fields
                    updates = {}
                    for key, value in document.items():
                        updates[key] = value

                    count = len(table.search(query_obj))
                    table.update(updates, query_obj)

                    return QueryResult(success=True, affected_rows=count)
                else:
                    return QueryResult(success=False, error="Either document ID or conditions are required for UPDATE operation")

            elif operation == 'delete':
                # DELETE query
                doc_id = query_data.get('id')
                conditions = query_data.get('where', {})

                if doc_id is not None:
                    # Delete by ID
                    table.remove(doc_ids=[doc_id])
                    return QueryResult(success=True, affected_rows=1)

                # Delete by conditions
                if conditions:
                    # Build query
                    q = Query()
                    query_obj = None

                    for key, value in conditions.items():
                        if query_obj is None:
                            query_obj = (q[key] == value)
                        else:
                            query_obj &= (q[key] == value)

                    count = len(table.search(query_obj))
                    table.remove(query_obj)

                    return QueryResult(success=True, affected_rows=count)
                else:
                    return QueryResult(success=False, error="Either document ID or conditions are required for DELETE operation")

            else:
                return QueryResult(success=False, error=f"Unsupported operation: {operation}")

        except Exception as e:
            logger.error(f"Failed to execute query on NoSQL database '{db_name}': {e}")
            return QueryResult(success=False, error=str(e))

        finally:
            if 'db' in locals():
                db.close()

    def get_collections(self, db_name: str) -> List[str]:
        """
        Get a list of collections in a database.

        Args:
            db_name: Name of the database

        Returns:
            List of collection names
        """
        db_path = os.path.join(self.data_dir, f"{db_name}{NOSQL_DB_EXTENSION}")

        # Check if database exists
        if not os.path.exists(db_path):
            return []

        try:
            # Connect to database
            db = TinyDB(db_path)

            # Get collections (tables in TinyDB)
            collections = db.tables()

            db.close()

            return collections

        except Exception as e:
            logger.error(f"Failed to get collections for NoSQL database '{db_name}': {e}")
            return []

    def get_collection_schema(self, db_name: str, collection_name: str) -> Dict[str, Any]:
        """
        Get the schema for a collection.
        This is an approximation since NoSQL databases don't have fixed schemas.

        Args:
            db_name: Name of the database
            collection_name: Name of the collection

        Returns:
            Collection schema approximation
        """
        db_path = os.path.join(self.data_dir, f"{db_name}{NOSQL_DB_EXTENSION}")

        # Check if database exists
        if not os.path.exists(db_path):
            return {}

        try:
            # Connect to database
            db = TinyDB(db_path)
            table = db.table(collection_name)

            # Get all documents
            documents = table.all()

            # Extract schema from documents
            schema = {}

            for doc in documents:
                for key, value in doc.items():
                    if key not in schema:
                        schema[key] = type(value).__name__

            db.close()

            return {
                'name': collection_name,
                'fields': schema
            }

        except Exception as e:
            logger.error(f"Failed to get schema for collection '{collection_name}' in NoSQL database '{db_name}': {e}")
            return {}
