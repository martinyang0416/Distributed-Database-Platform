"""
Replication manager for storage nodes.
"""
import os
import time
import threading
import shutil
from typing import Dict, List, Any, Optional, Tuple, Callable
import requests
from distributed_db.common.utils import get_logger, ensure_directory_exists
from distributed_db.common.config import (
    SQL_DB_EXTENSION, NOSQL_DB_EXTENSION, CLIENT_TIMEOUT
)

logger = get_logger(__name__)


class ReplicationManager:
    """
    Manages data replication across storage nodes.
    """
    
    def __init__(self, data_dir: str, node_id: str, coordinator_url: str):
        """
        Initialize the replication manager.
        
        Args:
            data_dir: Directory containing database files
            node_id: ID of this storage node
            coordinator_url: URL of the coordinator
        """
        self.data_dir = data_dir
        self.node_id = node_id
        self.coordinator_url = coordinator_url
        self.replication_tasks = {}  # {task_id: task_info}
        self.replication_lock = threading.RLock()
        
        # Start background thread for replication
        self.replication_thread = threading.Thread(target=self._replication_worker, daemon=True)
        self.replication_thread.start()
        
        logger.info(f"Replication manager initialized for node {node_id}")
    
    def replicate_database(
        self,
        db_name: str,
        source_node: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """
        Replicate a database from another node.
        
        Args:
            db_name: Name of the database
            source_node: Information about the source node
            
        Returns:
            Tuple of (success, error_message)
        """
        task_id = f"replicate_{db_name}_{int(time.time())}"
        
        with self.replication_lock:
            # Check if already replicating
            for task_info in self.replication_tasks.values():
                if task_info['db_name'] == db_name and task_info['status'] == 'running':
                    return False, f"Already replicating database '{db_name}'"
            
            # Create replication task
            task_info = {
                'id': task_id,
                'db_name': db_name,
                'source_node': source_node,
                'status': 'pending',
                'progress': 0,
                'error': None,
                'start_time': time.time(),
                'end_time': None
            }
            
            self.replication_tasks[task_id] = task_info
        
        logger.info(f"Scheduled replication of database '{db_name}' from node {source_node['id']}")
        return True, None
    
    def get_replication_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the status of a replication task.
        
        Args:
            task_id: ID of the replication task
            
        Returns:
            Task information if found, None otherwise
        """
        with self.replication_lock:
            return self.replication_tasks.get(task_id)
    
    def list_replication_tasks(self) -> List[Dict[str, Any]]:
        """
        List all replication tasks.
        
        Returns:
            List of replication task information
        """
        with self.replication_lock:
            return list(self.replication_tasks.values())
    
    def cancel_replication(self, task_id: str) -> Tuple[bool, Optional[str]]:
        """
        Cancel a replication task.
        
        Args:
            task_id: ID of the replication task
            
        Returns:
            Tuple of (success, error_message)
        """
        with self.replication_lock:
            if task_id not in self.replication_tasks:
                return False, f"Replication task '{task_id}' not found"
            
            task_info = self.replication_tasks[task_id]
            
            if task_info['status'] == 'completed' or task_info['status'] == 'failed':
                return False, f"Replication task '{task_id}' is already {task_info['status']}"
            
            task_info['status'] = 'cancelled'
            task_info['end_time'] = time.time()
            
            logger.info(f"Cancelled replication task '{task_id}'")
            return True, None
    
    def _replication_worker(self) -> None:
        """
        Background thread for processing replication tasks.
        """
        while True:
            time.sleep(1)  # Check for tasks every second
            
            task_to_process = None
            
            # Find a pending task
            with self.replication_lock:
                for task_id, task_info in self.replication_tasks.items():
                    if task_info['status'] == 'pending':
                        task_to_process = task_info.copy()
                        task_info['status'] = 'running'
                        break
            
            if task_to_process:
                # Process the task
                db_name = task_to_process['db_name']
                source_node = task_to_process['source_node']
                task_id = task_to_process['id']
                
                logger.info(f"Starting replication of database '{db_name}' from node {source_node['id']}")
                
                try:
                    # Determine database type
                    is_sql = self._is_sql_database(db_name, source_node)
                    
                    if is_sql:
                        success, error = self._replicate_sql_database(db_name, source_node)
                    else:
                        success, error = self._replicate_nosql_database(db_name, source_node)
                    
                    with self.replication_lock:
                        if task_id in self.replication_tasks:
                            if self.replication_tasks[task_id]['status'] == 'cancelled':
                                logger.info(f"Replication task '{task_id}' was cancelled")
                            else:
                                self.replication_tasks[task_id]['status'] = 'completed' if success else 'failed'
                                self.replication_tasks[task_id]['progress'] = 100 if success else self.replication_tasks[task_id]['progress']
                                self.replication_tasks[task_id]['error'] = error
                                self.replication_tasks[task_id]['end_time'] = time.time()
                                
                                if success:
                                    logger.info(f"Completed replication of database '{db_name}' from node {source_node['id']}")
                                else:
                                    logger.error(f"Failed to replicate database '{db_name}' from node {source_node['id']}: {error}")
                
                except Exception as e:
                    logger.error(f"Error during replication of database '{db_name}': {e}")
                    
                    with self.replication_lock:
                        if task_id in self.replication_tasks:
                            self.replication_tasks[task_id]['status'] = 'failed'
                            self.replication_tasks[task_id]['error'] = str(e)
                            self.replication_tasks[task_id]['end_time'] = time.time()
    
    def _is_sql_database(self, db_name: str, source_node: Dict[str, Any]) -> bool:
        """
        Determine if a database is SQL or NoSQL.
        
        Args:
            db_name: Name of the database
            source_node: Information about the source node
            
        Returns:
            True if SQL, False if NoSQL
        """
        try:
            # Query source node for database type
            url = f"http://{source_node['host']}:{source_node['port']}/database/{db_name}/type"
            response = requests.get(url, timeout=CLIENT_TIMEOUT)
            response.raise_for_status()
            
            data = response.json()
            return data.get('type') == 'sql'
        
        except Exception as e:
            logger.error(f"Failed to determine database type for '{db_name}': {e}")
            
            # Try to guess based on file extension
            sql_path = os.path.join(self.data_dir, f"{db_name}{SQL_DB_EXTENSION}")
            nosql_path = os.path.join(self.data_dir, f"{db_name}{NOSQL_DB_EXTENSION}")
            
            if os.path.exists(sql_path):
                return True
            elif os.path.exists(nosql_path):
                return False
            
            # Default to SQL
            return True
    
    def _replicate_sql_database(
        self,
        db_name: str,
        source_node: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """
        Replicate a SQL database from another node.
        
        Args:
            db_name: Name of the database
            source_node: Information about the source node
            
        Returns:
            Tuple of (success, error_message)
        """
        source_url = f"http://{source_node['host']}:{source_node['port']}/database/{db_name}/file"
        local_path = os.path.join(self.data_dir, f"{db_name}{SQL_DB_EXTENSION}")
        
        # Create temporary file
        temp_path = f"{local_path}.tmp"
        
        try:
            # Download database file
            with requests.get(source_url, stream=True, timeout=CLIENT_TIMEOUT) as response:
                response.raise_for_status()
                
                with open(temp_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            # Replace existing file
            if os.path.exists(local_path):
                os.remove(local_path)
            
            os.rename(temp_path, local_path)
            
            return True, None
        
        except Exception as e:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
            
            return False, str(e)
    
    def _replicate_nosql_database(
        self,
        db_name: str,
        source_node: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """
        Replicate a NoSQL database from another node.
        
        Args:
            db_name: Name of the database
            source_node: Information about the source node
            
        Returns:
            Tuple of (success, error_message)
        """
        source_url = f"http://{source_node['host']}:{source_node['port']}/database/{db_name}/file"
        local_path = os.path.join(self.data_dir, f"{db_name}{NOSQL_DB_EXTENSION}")
        
        # Create temporary file
        temp_path = f"{local_path}.tmp"
        
        try:
            # Download database file
            with requests.get(source_url, stream=True, timeout=CLIENT_TIMEOUT) as response:
                response.raise_for_status()
                
                with open(temp_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            # Replace existing file
            if os.path.exists(local_path):
                os.remove(local_path)
            
            os.rename(temp_path, local_path)
            
            return True, None
        
        except Exception as e:
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass
            
            return False, str(e)
