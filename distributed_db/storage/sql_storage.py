"""
SQL storage implementation for storage nodes.
"""
import os
import sqlite3
from typing import Dict, List, Any, Optional, Tuple
from distributed_db.common.utils import get_logger, ensure_directory_exists
from distributed_db.common.models import QueryResult
from distributed_db.common.config import SQL_DB_EXTENSION

logger = get_logger(__name__)


class SQLStorage:
    """
    Implements storage for SQL databases using SQLite.
    """

    def __init__(self, data_dir: str):
        """
        Initialize the SQL storage.

        Args:
            data_dir: Directory to store database files
        """
        self.data_dir = data_dir
        ensure_directory_exists(data_dir)
        logger.info(f"SQL storage initialized with data directory {data_dir}")

    def create_database(self, db_name: str, schema: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Create a new database with the given schema.

        Args:
            db_name: Name of the database
            schema: Database schema

        Returns:
            Tuple of (success, error_message)
        """
        db_path = os.path.join(self.data_dir, f"{db_name}{SQL_DB_EXTENSION}")

        # Check if database already exists
        if os.path.exists(db_path):
            return False, f"Database '{db_name}' already exists"

        try:
            # Create database file
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Create tables
            for table in schema.get('tables', []):
                table_name = table['name']
                columns = []

                for column in table.get('columns', []):
                    column_def = f"{column['name']} {column['data_type']}"

                    if column.get('primary_key', False):
                        column_def += " PRIMARY KEY"

                    if not column.get('nullable', True):
                        column_def += " NOT NULL"

                    if 'default' in column and column['default'] is not None:
                        column_def += f" DEFAULT {column['default']}"

                    columns.append(column_def)

                create_table_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
                cursor.execute(create_table_sql)

                # Create indexes
                for index in table.get('indexes', []):
                    create_index_sql = f"CREATE INDEX idx_{table_name}_{index} ON {table_name} ({index})"
                    cursor.execute(create_index_sql)

            conn.commit()
            conn.close()

            logger.info(f"Created SQL database '{db_name}'")
            return True, None

        except Exception as e:
            logger.error(f"Failed to create SQL database '{db_name}': {e}")

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
        db_path = os.path.join(self.data_dir, f"{db_name}{SQL_DB_EXTENSION}")

        # Check if database exists
        if not os.path.exists(db_path):
            return False, f"Database '{db_name}' does not exist"

        try:
            # Delete database file
            os.remove(db_path)

            logger.info(f"Deleted SQL database '{db_name}'")
            return True, None

        except Exception as e:
            logger.error(f"Failed to delete SQL database '{db_name}': {e}")
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
        db_path = os.path.join(self.data_dir, f"{db_name}{SQL_DB_EXTENSION}")

        # Check if database exists
        if not os.path.exists(db_path):
            return QueryResult(success=False, error=f"Database '{db_name}' does not exist")

        try:
            # Connect to database
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row  # Return rows as dictionaries
            cursor = conn.cursor()

            operation = query_data.get('operation', '').lower()
            table_name = query_data.get('table')

            if not table_name:
                return QueryResult(success=False, error="Table name is required")

            # Check if table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
            if cursor.fetchone() is None:
                # Table doesn't exist, create it if this is an insert operation
                if operation == 'insert':
                    # Get the column names and types from the values
                    values = query_data.get('values', {})
                    if not values:
                        return QueryResult(success=False, error="Values are required for INSERT operation")

                    # Create a simple table with the columns from the values
                    columns = []
                    for key in values.keys():
                        if key == 'id':
                            columns.append(f"{key} INTEGER PRIMARY KEY")
                        else:
                            columns.append(f"{key} TEXT")

                    create_table_sql = f"CREATE TABLE {table_name} ({', '.join(columns)})"
                    logger.info(f"Creating table: {create_table_sql}")
                    cursor.execute(create_table_sql)
                    conn.commit()

                    # Verify table was created
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                    if cursor.fetchone() is None:
                        logger.error(f"Failed to create table '{table_name}' in database '{db_name}'")
                        return QueryResult(success=False, error=f"Failed to create table '{table_name}'")

                    logger.info(f"Created table '{table_name}' in database '{db_name}'")
                else:
                    return QueryResult(success=False, error=f"Table '{table_name}' does not exist")

            # Execute query based on operation
            if operation == 'get' or operation == 'query':
                # SELECT query
                columns = query_data.get('columns', ['*'])
                where_clause = query_data.get('where', {})
                order_by = query_data.get('order_by')
                limit = query_data.get('limit')

                # Build SQL query
                sql = f"SELECT {', '.join(columns)} FROM {table_name}"

                # Add WHERE clause
                params = []
                if where_clause:
                    conditions = []
                    for key, value in where_clause.items():
                        if isinstance(value, dict):
                            # Handle operators like $lt, $gt, etc.
                            for op, op_value in value.items():
                                if op == '$lt':
                                    conditions.append(f"{key} < ?")
                                    params.append(op_value)
                                elif op == '$lte':
                                    conditions.append(f"{key} <= ?")
                                    params.append(op_value)
                                elif op == '$gt':
                                    conditions.append(f"{key} > ?")
                                    params.append(op_value)
                                elif op == '$gte':
                                    conditions.append(f"{key} >= ?")
                                    params.append(op_value)
                                elif op == '$ne':
                                    conditions.append(f"{key} != ?")
                                    params.append(op_value)
                                else:
                                    conditions.append(f"{key} = ?")
                                    params.append(op_value)
                        else:
                            conditions.append(f"{key} = ?")
                            params.append(value)

                    sql += f" WHERE {' AND '.join(conditions)}"

                # Add ORDER BY clause
                if order_by:
                    sql += f" ORDER BY {order_by}"

                # Add LIMIT clause
                if limit:
                    sql += f" LIMIT {limit}"

                # Execute query
                logger.info(f"Executing SQL query: {sql} with params {params}")
                cursor.execute(sql, params)
                rows = cursor.fetchall()

                # Convert rows to dictionaries
                result = []
                for row in rows:
                    result.append(dict(row))

                logger.info(f"Query result: {result}")
                return QueryResult(success=True, data=result)

            elif operation == 'insert':
                # INSERT query
                values = query_data.get('values', {})

                if not values:
                    return QueryResult(success=False, error="Values are required for INSERT operation")

                # Build SQL query
                columns = list(values.keys())
                placeholders = ['?'] * len(columns)

                sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"

                # Execute query
                logger.info(f"Executing SQL insert: {sql} with values {list(values.values())}")
                cursor.execute(sql, list(values.values()))
                conn.commit()

                # Return the inserted row
                select_sql = f"SELECT * FROM {table_name} WHERE id = ?"
                if 'id' in values:
                    logger.info(f"Selecting inserted row: {select_sql} with id {values['id']}")
                    cursor.execute(select_sql, (values['id'],))
                else:
                    logger.info(f"Selecting inserted row: {select_sql} with rowid {cursor.lastrowid}")
                    cursor.execute(f"SELECT * FROM {table_name} WHERE rowid = ?", (cursor.lastrowid,))

                row = cursor.fetchone()
                if row:
                    row_dict = dict(row)
                    # Log the inserted row
                    logger.info(f"Inserted row: {row_dict}")
                    return QueryResult(success=True, data=[row_dict], affected_rows=1)
                else:
                    logger.warning(f"No row found after insert")
                    return QueryResult(success=True, affected_rows=1)

            elif operation == 'update':
                # UPDATE query
                values = query_data.get('values', {})
                where_clause = query_data.get('where', {})

                if not values:
                    return QueryResult(success=False, error="Values are required for UPDATE operation")

                # Build SQL query
                set_clause = []
                params = []

                for key, value in values.items():
                    set_clause.append(f"{key} = ?")
                    params.append(value)

                sql = f"UPDATE {table_name} SET {', '.join(set_clause)}"

                # Add WHERE clause
                if where_clause:
                    conditions = []
                    for key, value in where_clause.items():
                        if isinstance(value, dict):
                            # Handle operators like $lt, $gt, etc.
                            for op, op_value in value.items():
                                if op == '$lt':
                                    conditions.append(f"{key} < ?")
                                    params.append(op_value)
                                elif op == '$lte':
                                    conditions.append(f"{key} <= ?")
                                    params.append(op_value)
                                elif op == '$gt':
                                    conditions.append(f"{key} > ?")
                                    params.append(op_value)
                                elif op == '$gte':
                                    conditions.append(f"{key} >= ?")
                                    params.append(op_value)
                                elif op == '$ne':
                                    conditions.append(f"{key} != ?")
                                    params.append(op_value)
                                else:
                                    conditions.append(f"{key} = ?")
                                    params.append(op_value)
                        else:
                            conditions.append(f"{key} = ?")
                            params.append(value)

                    sql += f" WHERE {' AND '.join(conditions)}"

                # Execute query
                cursor.execute(sql, params)
                conn.commit()

                # Get the updated rows
                if where_clause:
                    # Build a new query to get the updated rows
                    select_sql = f"SELECT * FROM {table_name}"
                    select_params = []

                    conditions = []
                    for key, value in where_clause.items():
                        if isinstance(value, dict):
                            # Handle operators like $lt, $gt, etc.
                            for op, op_value in value.items():
                                if op == '$lt':
                                    conditions.append(f"{key} < ?")
                                    select_params.append(op_value)
                                elif op == '$lte':
                                    conditions.append(f"{key} <= ?")
                                    select_params.append(op_value)
                                elif op == '$gt':
                                    conditions.append(f"{key} > ?")
                                    select_params.append(op_value)
                                elif op == '$gte':
                                    conditions.append(f"{key} >= ?")
                                    select_params.append(op_value)
                                elif op == '$ne':
                                    conditions.append(f"{key} != ?")
                                    select_params.append(op_value)
                                else:
                                    conditions.append(f"{key} = ?")
                                    select_params.append(op_value)
                        else:
                            conditions.append(f"{key} = ?")
                            select_params.append(value)

                    select_sql += f" WHERE {' AND '.join(conditions)}"
                    cursor.execute(select_sql, select_params)
                    rows = cursor.fetchall()

                    # Convert rows to dictionaries
                    result = []
                    for row in rows:
                        result.append(dict(row))

                    return QueryResult(success=True, data=result, affected_rows=cursor.rowcount)

                return QueryResult(success=True, affected_rows=cursor.rowcount)

            elif operation == 'delete':
                # DELETE query
                where_clause = query_data.get('where', {})

                # Build SQL query
                sql = f"DELETE FROM {table_name}"

                # Add WHERE clause
                params = []
                if where_clause:
                    conditions = []
                    for key, value in where_clause.items():
                        if isinstance(value, dict):
                            # Handle operators like $lt, $gt, etc.
                            for op, op_value in value.items():
                                if op == '$lt':
                                    conditions.append(f"{key} < ?")
                                    params.append(op_value)
                                elif op == '$lte':
                                    conditions.append(f"{key} <= ?")
                                    params.append(op_value)
                                elif op == '$gt':
                                    conditions.append(f"{key} > ?")
                                    params.append(op_value)
                                elif op == '$gte':
                                    conditions.append(f"{key} >= ?")
                                    params.append(op_value)
                                elif op == '$ne':
                                    conditions.append(f"{key} != ?")
                                    params.append(op_value)
                                else:
                                    conditions.append(f"{key} = ?")
                                    params.append(op_value)
                        else:
                            conditions.append(f"{key} = ?")
                            params.append(value)

                    sql += f" WHERE {' AND '.join(conditions)}"

                # Execute query
                cursor.execute(sql, params)
                conn.commit()

                return QueryResult(success=True, affected_rows=cursor.rowcount)

            else:
                return QueryResult(success=False, error=f"Unsupported operation: {operation}")

        except Exception as e:
            logger.error(f"Failed to execute query on SQL database '{db_name}': {e}")
            return QueryResult(success=False, error=str(e))

        finally:
            if 'conn' in locals():
                conn.close()

    def get_tables(self, db_name: str) -> List[str]:
        """
        Get a list of tables in a database.

        Args:
            db_name: Name of the database

        Returns:
            List of table names
        """
        db_path = os.path.join(self.data_dir, f"{db_name}{SQL_DB_EXTENSION}")

        # Check if database exists
        if not os.path.exists(db_path):
            return []

        try:
            # Connect to database
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Get tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]

            conn.close()

            return tables

        except Exception as e:
            logger.error(f"Failed to get tables for SQL database '{db_name}': {e}")
            return []

    def get_table_schema(self, db_name: str, table_name: str) -> Dict[str, Any]:
        """
        Get the schema for a table.

        Args:
            db_name: Name of the database
            table_name: Name of the table

        Returns:
            Table schema
        """
        db_path = os.path.join(self.data_dir, f"{db_name}{SQL_DB_EXTENSION}")

        # Check if database exists
        if not os.path.exists(db_path):
            return {}

        try:
            # Connect to database
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            # Get table schema
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = []

            for row in cursor.fetchall():
                column = {
                    'name': row[1],
                    'data_type': row[2],
                    'nullable': not row[3],
                    'primary_key': bool(row[5])
                }
                columns.append(column)

            # Get indexes
            cursor.execute(f"PRAGMA index_list({table_name})")
            indexes = []

            for row in cursor.fetchall():
                index_name = row[1]
                cursor.execute(f"PRAGMA index_info({index_name})")
                for idx_row in cursor.fetchall():
                    column_name = idx_row[2]
                    if column_name not in indexes:
                        indexes.append(column_name)

            conn.close()

            return {
                'name': table_name,
                'columns': columns,
                'indexes': indexes
            }

        except Exception as e:
            logger.error(f"Failed to get schema for table '{table_name}' in SQL database '{db_name}': {e}")
            return {}
