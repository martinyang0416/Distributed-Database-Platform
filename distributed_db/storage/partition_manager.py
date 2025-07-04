"""
Partition manager for storage nodes.
"""
import os
import json
import shutil
from typing import Dict, List, Any, Optional, Tuple
from distributed_db.common.utils import get_logger, ensure_directory_exists
from distributed_db.common.models import PartitionType
from distributed_db.common.config import SQL_DB_EXTENSION, NOSQL_DB_EXTENSION

logger = get_logger(__name__)


class PartitionManager:
    """
    Manages data partitioning for storage nodes.
    """
    
    def __init__(self, data_dir: str, partition_dir: str = "partitions"):
        """
        Initialize the partition manager.
        
        Args:
            data_dir: Directory containing database files
            partition_dir: Directory to store partition metadata
        """
        self.data_dir = data_dir
        self.partition_dir = os.path.join(data_dir, partition_dir)
        ensure_directory_exists(self.partition_dir)
        logger.info(f"Partition manager initialized with partition directory {self.partition_dir}")
    
    def create_partition(
        self,
        partition_id: str,
        db_name: str,
        partition_type: PartitionType,
        partition_info: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """
        Create a new partition.
        
        Args:
            partition_id: ID of the partition
            db_name: Name of the database
            partition_type: Type of partitioning
            partition_info: Partition information
            
        Returns:
            Tuple of (success, error_message)
        """
        # Check if partition already exists
        partition_file = os.path.join(self.partition_dir, f"{partition_id}.json")
        if os.path.exists(partition_file):
            return False, f"Partition '{partition_id}' already exists"
        
        # Create partition metadata
        metadata = {
            'id': partition_id,
            'database_name': db_name,
            'partition_type': partition_type.value,
            **partition_info
        }
        
        try:
            # Save partition metadata
            with open(partition_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            logger.info(f"Created partition '{partition_id}' for database '{db_name}'")
            return True, None
        
        except Exception as e:
            logger.error(f"Failed to create partition '{partition_id}': {e}")
            return False, str(e)
    
    def delete_partition(self, partition_id: str) -> Tuple[bool, Optional[str]]:
        """
        Delete a partition.
        
        Args:
            partition_id: ID of the partition
            
        Returns:
            Tuple of (success, error_message)
        """
        # Check if partition exists
        partition_file = os.path.join(self.partition_dir, f"{partition_id}.json")
        if not os.path.exists(partition_file):
            return False, f"Partition '{partition_id}' does not exist"
        
        try:
            # Delete partition metadata
            os.remove(partition_file)
            
            logger.info(f"Deleted partition '{partition_id}'")
            return True, None
        
        except Exception as e:
            logger.error(f"Failed to delete partition '{partition_id}': {e}")
            return False, str(e)
    
    def get_partition(self, partition_id: str) -> Optional[Dict[str, Any]]:
        """
        Get partition information.
        
        Args:
            partition_id: ID of the partition
            
        Returns:
            Partition information if found, None otherwise
        """
        partition_file = os.path.join(self.partition_dir, f"{partition_id}.json")
        if not os.path.exists(partition_file):
            return None
        
        try:
            with open(partition_file, 'r') as f:
                return json.load(f)
        
        except Exception as e:
            logger.error(f"Failed to get partition '{partition_id}': {e}")
            return None
    
    def list_partitions(self) -> List[Dict[str, Any]]:
        """
        List all partitions.
        
        Returns:
            List of partition information
        """
        partitions = []
        
        for filename in os.listdir(self.partition_dir):
            if filename.endswith('.json'):
                partition_id = filename[:-5]  # Remove .json extension
                partition_info = self.get_partition(partition_id)
                if partition_info:
                    partitions.append(partition_info)
        
        return partitions
    
    def get_database_partitions(self, db_name: str) -> List[Dict[str, Any]]:
        """
        Get all partitions for a specific database.
        
        Args:
            db_name: Name of the database
            
        Returns:
            List of partition information
        """
        return [
            partition for partition in self.list_partitions()
            if partition.get('database_name') == db_name
        ]
    
    def split_horizontal_partition(
        self,
        partition_id: str,
        split_value: Any,
        new_partition_id: str
    ) -> Tuple[bool, Optional[str]]:
        """
        Split a horizontal partition into two partitions.
        
        Args:
            partition_id: ID of the partition to split
            split_value: Value to split on
            new_partition_id: ID for the new partition
            
        Returns:
            Tuple of (success, error_message)
        """
        # Get partition information
        partition_info = self.get_partition(partition_id)
        if not partition_info:
            return False, f"Partition '{partition_id}' does not exist"
        
        # Check if this is a horizontal partition
        if partition_info.get('partition_type') != PartitionType.HORIZONTAL.value:
            return False, f"Partition '{partition_id}' is not a horizontal partition"
        
        # Get partition key
        partition_key = partition_info.get('partition_key')
        if not partition_key:
            return False, f"Partition '{partition_id}' does not have a partition key"
        
        # Create new partition
        new_partition_info = partition_info.copy()
        new_partition_info['id'] = new_partition_id
        new_partition_info['partition_value'] = split_value
        
        success, error = self.create_partition(
            new_partition_id,
            partition_info['database_name'],
            PartitionType.HORIZONTAL,
            new_partition_info
        )
        
        if not success:
            return False, error
        
        # Update original partition
        partition_info['partition_value'] = {'$lt': split_value}
        
        # Save updated partition
        partition_file = os.path.join(self.partition_dir, f"{partition_id}.json")
        try:
            with open(partition_file, 'w') as f:
                json.dump(partition_info, f, indent=2)
            
            logger.info(f"Split horizontal partition '{partition_id}' at value '{split_value}'")
            return True, None
        
        except Exception as e:
            logger.error(f"Failed to update partition '{partition_id}' after split: {e}")
            
            # Try to rollback
            self.delete_partition(new_partition_id)
            
            return False, str(e)
    
    def split_vertical_partition(
        self,
        partition_id: str,
        columns: List[str],
        new_partition_id: str
    ) -> Tuple[bool, Optional[str]]:
        """
        Split a vertical partition into two partitions.
        
        Args:
            partition_id: ID of the partition to split
            columns: Columns to move to the new partition
            new_partition_id: ID for the new partition
            
        Returns:
            Tuple of (success, error_message)
        """
        # Get partition information
        partition_info = self.get_partition(partition_id)
        if not partition_info:
            return False, f"Partition '{partition_id}' does not exist"
        
        # Check if this is a vertical partition
        if partition_info.get('partition_type') != PartitionType.VERTICAL.value:
            return False, f"Partition '{partition_id}' is not a vertical partition"
        
        # Create new partition
        new_partition_info = partition_info.copy()
        new_partition_info['id'] = new_partition_id
        new_partition_info['columns'] = columns
        
        success, error = self.create_partition(
            new_partition_id,
            partition_info['database_name'],
            PartitionType.VERTICAL,
            new_partition_info
        )
        
        if not success:
            return False, error
        
        # Update original partition
        if 'columns' not in partition_info or not partition_info['columns']:
            # If original partition had all columns, now it has all except the ones moved
            all_columns = self._get_all_columns(partition_info['database_name'], partition_info.get('table_name'))
            partition_info['columns'] = [col for col in all_columns if col not in columns]
        else:
            # Remove moved columns from original partition
            partition_info['columns'] = [col for col in partition_info['columns'] if col not in columns]
        
        # Save updated partition
        partition_file = os.path.join(self.partition_dir, f"{partition_id}.json")
        try:
            with open(partition_file, 'w') as f:
                json.dump(partition_info, f, indent=2)
            
            logger.info(f"Split vertical partition '{partition_id}' by columns {columns}")
            return True, None
        
        except Exception as e:
            logger.error(f"Failed to update partition '{partition_id}' after split: {e}")
            
            # Try to rollback
            self.delete_partition(new_partition_id)
            
            return False, str(e)
    
    def merge_horizontal_partitions(
        self,
        partition_ids: List[str],
        new_partition_id: str
    ) -> Tuple[bool, Optional[str]]:
        """
        Merge multiple horizontal partitions into a single partition.
        
        Args:
            partition_ids: IDs of the partitions to merge
            new_partition_id: ID for the new partition
            
        Returns:
            Tuple of (success, error_message)
        """
        if len(partition_ids) < 2:
            return False, "At least two partitions are required for merging"
        
        # Get partition information
        partitions = []
        db_name = None
        partition_key = None
        
        for partition_id in partition_ids:
            partition_info = self.get_partition(partition_id)
            if not partition_info:
                return False, f"Partition '{partition_id}' does not exist"
            
            # Check if this is a horizontal partition
            if partition_info.get('partition_type') != PartitionType.HORIZONTAL.value:
                return False, f"Partition '{partition_id}' is not a horizontal partition"
            
            # Check if all partitions are for the same database
            if db_name is None:
                db_name = partition_info['database_name']
            elif db_name != partition_info['database_name']:
                return False, f"Partition '{partition_id}' is for a different database"
            
            # Check if all partitions have the same partition key
            if partition_key is None:
                partition_key = partition_info.get('partition_key')
            elif partition_key != partition_info.get('partition_key'):
                return False, f"Partition '{partition_id}' has a different partition key"
            
            partitions.append(partition_info)
        
        # Create new partition
        new_partition_info = partitions[0].copy()
        new_partition_info['id'] = new_partition_id
        new_partition_info['partition_value'] = None  # Full range
        
        success, error = self.create_partition(
            new_partition_id,
            db_name,
            PartitionType.HORIZONTAL,
            new_partition_info
        )
        
        if not success:
            return False, error
        
        # Delete old partitions
        for partition_id in partition_ids:
            success, error = self.delete_partition(partition_id)
            if not success:
                logger.error(f"Failed to delete partition '{partition_id}' after merge: {error}")
        
        logger.info(f"Merged horizontal partitions {partition_ids} into '{new_partition_id}'")
        return True, None
    
    def merge_vertical_partitions(
        self,
        partition_ids: List[str],
        new_partition_id: str
    ) -> Tuple[bool, Optional[str]]:
        """
        Merge multiple vertical partitions into a single partition.
        
        Args:
            partition_ids: IDs of the partitions to merge
            new_partition_id: ID for the new partition
            
        Returns:
            Tuple of (success, error_message)
        """
        if len(partition_ids) < 2:
            return False, "At least two partitions are required for merging"
        
        # Get partition information
        partitions = []
        db_name = None
        table_name = None
        
        for partition_id in partition_ids:
            partition_info = self.get_partition(partition_id)
            if not partition_info:
                return False, f"Partition '{partition_id}' does not exist"
            
            # Check if this is a vertical partition
            if partition_info.get('partition_type') != PartitionType.VERTICAL.value:
                return False, f"Partition '{partition_id}' is not a vertical partition"
            
            # Check if all partitions are for the same database
            if db_name is None:
                db_name = partition_info['database_name']
            elif db_name != partition_info['database_name']:
                return False, f"Partition '{partition_id}' is for a different database"
            
            # Check if all partitions are for the same table
            if table_name is None:
                table_name = partition_info.get('table_name')
            elif table_name != partition_info.get('table_name'):
                return False, f"Partition '{partition_id}' is for a different table"
            
            partitions.append(partition_info)
        
        # Create new partition
        new_partition_info = partitions[0].copy()
        new_partition_info['id'] = new_partition_id
        
        # Combine columns from all partitions
        all_columns = set()
        for partition in partitions:
            if 'columns' in partition and partition['columns']:
                all_columns.update(partition['columns'])
        
        new_partition_info['columns'] = list(all_columns) if all_columns else None
        
        success, error = self.create_partition(
            new_partition_id,
            db_name,
            PartitionType.VERTICAL,
            new_partition_info
        )
        
        if not success:
            return False, error
        
        # Delete old partitions
        for partition_id in partition_ids:
            success, error = self.delete_partition(partition_id)
            if not success:
                logger.error(f"Failed to delete partition '{partition_id}' after merge: {error}")
        
        logger.info(f"Merged vertical partitions {partition_ids} into '{new_partition_id}'")
        return True, None
    
    def _get_all_columns(self, db_name: str, table_name: Optional[str] = None) -> List[str]:
        """
        Get all columns for a database table.
        
        Args:
            db_name: Name of the database
            table_name: Name of the table (for SQL databases)
            
        Returns:
            List of column names
        """
        # Check if SQL database
        db_path = os.path.join(self.data_dir, f"{db_name}{SQL_DB_EXTENSION}")
        if os.path.exists(db_path) and table_name:
            try:
                import sqlite3
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [row[1] for row in cursor.fetchall()]
                
                conn.close()
                
                return columns
            
            except Exception as e:
                logger.error(f"Failed to get columns for table '{table_name}' in SQL database '{db_name}': {e}")
                return []
        
        # Check if NoSQL database
        db_path = os.path.join(self.data_dir, f"{db_name}{NOSQL_DB_EXTENSION}")
        if os.path.exists(db_path):
            try:
                from tinydb import TinyDB
                db = TinyDB(db_path)
                
                collection = table_name or 'default'
                table = db.table(collection)
                
                # Get all documents
                documents = table.all()
                
                # Extract all field names
                fields = set()
                for doc in documents:
                    fields.update(doc.keys())
                
                db.close()
                
                return list(fields)
            
            except Exception as e:
                logger.error(f"Failed to get fields for collection '{collection}' in NoSQL database '{db_name}': {e}")
                return []
        
        return []
