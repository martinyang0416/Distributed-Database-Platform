"""
Schema manager for the coordinator.
"""
import json
import os
from typing import Dict, List, Optional, Any, Tuple
from distributed_db.common.utils import get_logger, generate_id, ensure_directory_exists
from distributed_db.common.models import (
    DatabaseType, PartitionType, DatabaseSchema, TableSchema, ColumnDefinition
)

logger = get_logger(__name__)


class SchemaManager:
    """
    Manages database schemas for the distributed database system.
    """
    
    def __init__(self, consensus_manager, schema_dir: str = "schemas"):
        """
        Initialize the schema manager.
        
        Args:
            consensus_manager: Consensus manager instance
            schema_dir: Directory to store schema files
        """
        self.consensus_manager = consensus_manager
        self.schema_dir = schema_dir
        ensure_directory_exists(schema_dir)
        logger.info(f"Schema manager initialized with schema directory {schema_dir}")
    
    def create_database(self, db_schema: DatabaseSchema) -> Tuple[bool, Optional[str]]:
        """
        Create a new database with the given schema.
        
        Args:
            db_schema: Database schema
            
        Returns:
            Tuple of (success, error_message)
        """
        # Check if database already exists
        if self.consensus_manager.get_database(db_schema.name) is not None:
            return False, f"Database '{db_schema.name}' already exists"
        
        # Convert schema to dictionary
        schema_dict = {
            'name': db_schema.name,
            'db_type': db_schema.db_type.value,
            'partition_type': db_schema.partition_type.value,
            'partition_key': db_schema.partition_key,
            'tables': []
        }
        
        # Add tables for SQL databases
        if db_schema.db_type == DatabaseType.SQL and db_schema.tables:
            for table in db_schema.tables:
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
                
                schema_dict['tables'].append(table_dict)
        
        # Save schema to consensus store
        success = self.consensus_manager.add_database(db_schema.name, schema_dict)
        
        if success:
            # Save schema to file as backup
            schema_file = os.path.join(self.schema_dir, f"{db_schema.name}.json")
            try:
                with open(schema_file, 'w') as f:
                    json.dump(schema_dict, f, indent=2)
                logger.info(f"Created database schema for '{db_schema.name}'")
                return True, None
            except Exception as e:
                logger.error(f"Failed to save schema file for '{db_schema.name}': {e}")
                # Try to rollback
                self.consensus_manager.remove_database(db_schema.name)
                return False, f"Failed to save schema file: {str(e)}"
        else:
            return False, "Failed to add database to consensus store"
    
    def get_database_schema(self, db_name: str) -> Optional[DatabaseSchema]:
        """
        Get the schema for a database.
        
        Args:
            db_name: Name of the database
            
        Returns:
            Database schema if found, None otherwise
        """
        # Try to get from consensus store
        db_info = self.consensus_manager.get_database(db_name)
        
        if db_info is None:
            # Try to load from file as fallback
            schema_file = os.path.join(self.schema_dir, f"{db_name}.json")
            try:
                if os.path.exists(schema_file):
                    with open(schema_file, 'r') as f:
                        db_info = json.load(f)
                else:
                    return None
            except Exception as e:
                logger.error(f"Failed to load schema file for '{db_name}': {e}")
                return None
        
        # Convert dictionary to DatabaseSchema
        db_type = DatabaseType(db_info['db_type'])
        partition_type = PartitionType(db_info['partition_type'])
        
        tables = []
        if 'tables' in db_info and db_info['tables']:
            for table_dict in db_info['tables']:
                columns = []
                for column_dict in table_dict.get('columns', []):
                    column = ColumnDefinition(
                        name=column_dict['name'],
                        data_type=column_dict['data_type'],
                        primary_key=column_dict.get('primary_key', False),
                        nullable=column_dict.get('nullable', True),
                        default=column_dict.get('default')
                    )
                    columns.append(column)
                
                table = TableSchema(
                    name=table_dict['name'],
                    columns=columns,
                    indexes=table_dict.get('indexes', [])
                )
                tables.append(table)
        
        return DatabaseSchema(
            name=db_info['name'],
            db_type=db_type,
            tables=tables if tables else None,
            partition_type=partition_type,
            partition_key=db_info.get('partition_key')
        )
    
    def list_databases(self) -> List[str]:
        """
        List all databases.
        
        Returns:
            List of database names
        """
        databases = self.consensus_manager.get_all_databases()
        return list(databases.keys())
    
    def delete_database(self, db_name: str) -> Tuple[bool, Optional[str]]:
        """
        Delete a database.
        
        Args:
            db_name: Name of the database
            
        Returns:
            Tuple of (success, error_message)
        """
        # Check if database exists
        if self.consensus_manager.get_database(db_name) is None:
            return False, f"Database '{db_name}' does not exist"
        
        # Remove from consensus store
        success = self.consensus_manager.remove_database(db_name)
        
        if success:
            # Remove schema file
            schema_file = os.path.join(self.schema_dir, f"{db_name}.json")
            try:
                if os.path.exists(schema_file):
                    os.remove(schema_file)
                logger.info(f"Deleted database schema for '{db_name}'")
                return True, None
            except Exception as e:
                logger.error(f"Failed to delete schema file for '{db_name}': {e}")
                return True, f"Database removed from consensus store but failed to delete schema file: {str(e)}"
        else:
            return False, "Failed to remove database from consensus store"
    
    def update_database_schema(self, db_schema: DatabaseSchema) -> Tuple[bool, Optional[str]]:
        """
        Update a database schema.
        
        Args:
            db_schema: Updated database schema
            
        Returns:
            Tuple of (success, error_message)
        """
        # Check if database exists
        if self.consensus_manager.get_database(db_schema.name) is None:
            return False, f"Database '{db_schema.name}' does not exist"
        
        # Convert schema to dictionary
        schema_dict = {
            'name': db_schema.name,
            'db_type': db_schema.db_type.value,
            'partition_type': db_schema.partition_type.value,
            'partition_key': db_schema.partition_key,
            'tables': []
        }
        
        # Add tables for SQL databases
        if db_schema.db_type == DatabaseType.SQL and db_schema.tables:
            for table in db_schema.tables:
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
                
                schema_dict['tables'].append(table_dict)
        
        # Update in consensus store
        success = self.consensus_manager.update_database(db_schema.name, schema_dict)
        
        if success:
            # Save schema to file as backup
            schema_file = os.path.join(self.schema_dir, f"{db_schema.name}.json")
            try:
                with open(schema_file, 'w') as f:
                    json.dump(schema_dict, f, indent=2)
                logger.info(f"Updated database schema for '{db_schema.name}'")
                return True, None
            except Exception as e:
                logger.error(f"Failed to save updated schema file for '{db_schema.name}': {e}")
                return True, f"Database updated in consensus store but failed to save schema file: {str(e)}"
        else:
            return False, "Failed to update database in consensus store"
