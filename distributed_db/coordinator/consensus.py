"""
Consensus implementation using the Raft algorithm.
"""
import time
import threading
from typing import Dict, List, Any, Optional, Callable
from pysyncobj import SyncObj, replicated
from distributed_db.common.utils import get_logger
from distributed_db.common.config import (
    RAFT_HEARTBEAT_INTERVAL,
    RAFT_ELECTION_TIMEOUT_MIN,
    RAFT_ELECTION_TIMEOUT_MAX
)

logger = get_logger(__name__)


class ConsensusManager(SyncObj):
    """
    Implements a distributed consensus mechanism using the Raft algorithm.
    Uses pysyncobj library which provides a Python implementation of Raft.
    """

    def __init__(self, self_address: str, partner_addresses: List[str]):
        """
        Initialize the consensus manager.

        Args:
            self_address: Address of this node in the format 'host:port'
            partner_addresses: List of addresses of other nodes in the format 'host:port'
        """
        # Configure Raft parameters
        from pysyncobj.syncobj import SyncObjConf
        config = SyncObjConf(
            autoTick=True,
            appendEntriesUseBatch=True,
            dynamicMembershipChange=True,
            heartbeatInterval=RAFT_HEARTBEAT_INTERVAL,
            minElectionTimeout=RAFT_ELECTION_TIMEOUT_MIN,
            maxElectionTimeout=RAFT_ELECTION_TIMEOUT_MAX
        )

        super().__init__(self_address, partner_addresses, conf=config)

        # Initialize replicated state
        self.__databases = {}  # Database metadata
        self.__storage_nodes = {}  # Storage node information
        self.__partitions = {}  # Partition information

        logger.info(f"Consensus manager initialized at {self_address} with partners {partner_addresses}")

    @replicated
    def add_database(self, db_name: str, db_info: Dict[str, Any]) -> bool:
        """
        Add a new database to the metadata store.

        Args:
            db_name: Name of the database
            db_info: Database information

        Returns:
            True if successful, False otherwise
        """
        if db_name in self.__databases:
            return False

        self.__databases[db_name] = db_info
        logger.info(f"Added database {db_name} to consensus store")
        return True

    @replicated
    def remove_database(self, db_name: str) -> bool:
        """
        Remove a database from the metadata store.

        Args:
            db_name: Name of the database

        Returns:
            True if successful, False otherwise
        """
        if db_name not in self.__databases:
            return False

        del self.__databases[db_name]

        # Also remove associated partitions
        partitions_to_remove = []
        for partition_id, partition_info in self.__partitions.items():
            if partition_info.get('database_name') == db_name:
                partitions_to_remove.append(partition_id)

        for partition_id in partitions_to_remove:
            del self.__partitions[partition_id]

        logger.info(f"Removed database {db_name} from consensus store")
        return True

    @replicated
    def update_database(self, db_name: str, db_info: Dict[str, Any]) -> bool:
        """
        Update database information.

        Args:
            db_name: Name of the database
            db_info: Updated database information

        Returns:
            True if successful, False otherwise
        """
        if db_name not in self.__databases:
            return False

        self.__databases[db_name] = db_info
        logger.info(f"Updated database {db_name} in consensus store")
        return True

    @replicated
    def add_storage_node(self, node_id: str, node_info: Dict[str, Any]) -> bool:
        """
        Add a new storage node to the metadata store.

        Args:
            node_id: ID of the storage node
            node_info: Storage node information

        Returns:
            True if successful, False otherwise
        """
        if node_id in self.__storage_nodes:
            return False

        self.__storage_nodes[node_id] = node_info
        logger.info(f"Added storage node {node_id} to consensus store")
        return True

    @replicated
    def remove_storage_node(self, node_id: str) -> bool:
        """
        Remove a storage node from the metadata store.

        Args:
            node_id: ID of the storage node

        Returns:
            True if successful, False otherwise
        """
        if node_id not in self.__storage_nodes:
            return False

        del self.__storage_nodes[node_id]

        # Update partitions that reference this node
        for partition_id, partition_info in self.__partitions.items():
            if 'storage_nodes' in partition_info and node_id in partition_info['storage_nodes']:
                partition_info['storage_nodes'].remove(node_id)

        logger.info(f"Removed storage node {node_id} from consensus store")
        return True

    @replicated
    def update_storage_node(self, node_id: str, node_info: Dict[str, Any]) -> bool:
        """
        Update storage node information.

        Args:
            node_id: ID of the storage node
            node_info: Updated storage node information

        Returns:
            True if successful, False otherwise
        """
        if node_id not in self.__storage_nodes:
            return False

        self.__storage_nodes[node_id] = node_info
        logger.info(f"Updated storage node {node_id} in consensus store")
        return True

    @replicated
    def add_partition(self, partition_id: str, partition_info: Dict[str, Any]) -> bool:
        """
        Add a new partition to the metadata store.

        Args:
            partition_id: ID of the partition
            partition_info: Partition information

        Returns:
            True if successful, False otherwise
        """
        if partition_id in self.__partitions:
            return False

        self.__partitions[partition_id] = partition_info
        logger.info(f"Added partition {partition_id} to consensus store")
        return True

    @replicated
    def remove_partition(self, partition_id: str) -> bool:
        """
        Remove a partition from the metadata store.

        Args:
            partition_id: ID of the partition

        Returns:
            True if successful, False otherwise
        """
        if partition_id not in self.__partitions:
            return False

        del self.__partitions[partition_id]
        logger.info(f"Removed partition {partition_id} from consensus store")
        return True

    @replicated
    def update_partition(self, partition_id: str, partition_info: Dict[str, Any]) -> bool:
        """
        Update partition information.

        Args:
            partition_id: ID of the partition
            partition_info: Updated partition information

        Returns:
            True if successful, False otherwise
        """
        if partition_id not in self.__partitions:
            return False

        self.__partitions[partition_id] = partition_info
        logger.info(f"Updated partition {partition_id} in consensus store")
        return True

    def get_database(self, db_name: str) -> Optional[Dict[str, Any]]:
        """
        Get database information.

        Args:
            db_name: Name of the database

        Returns:
            Database information if found, None otherwise
        """
        return self.__databases.get(db_name)

    def get_all_databases(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all database information.

        Returns:
            Dictionary of all databases
        """
        # Ensure we return a dictionary even if __databases is not initialized yet
        return self.__databases.copy() if hasattr(self, '_ConsensusManager__databases') else {}

    def get_storage_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """
        Get storage node information.

        Args:
            node_id: ID of the storage node

        Returns:
            Storage node information if found, None otherwise
        """
        return self.__storage_nodes.get(node_id)

    def get_all_storage_nodes(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all storage node information.

        Returns:
            Dictionary of all storage nodes
        """
        # Ensure we return a dictionary even if __storage_nodes is not initialized yet
        return self.__storage_nodes.copy() if hasattr(self, '_ConsensusManager__storage_nodes') else {}

    def get_partition(self, partition_id: str) -> Optional[Dict[str, Any]]:
        """
        Get partition information.

        Args:
            partition_id: ID of the partition

        Returns:
            Partition information if found, None otherwise
        """
        return self.__partitions.get(partition_id)

    def get_all_partitions(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all partition information.

        Returns:
            Dictionary of all partitions
        """
        # Ensure we return a dictionary even if __partitions is not initialized yet
        return self.__partitions.copy() if hasattr(self, '_ConsensusManager__partitions') else {}

    def get_database_partitions(self, db_name: str) -> Dict[str, Dict[str, Any]]:
        """
        Get all partitions for a specific database.

        Args:
            db_name: Name of the database

        Returns:
            Dictionary of partitions for the database
        """
        # Ensure we handle the case when __partitions is not initialized yet
        if not hasattr(self, '_ConsensusManager__partitions'):
            return {}

        return {
            partition_id: partition_info
            for partition_id, partition_info in self.__partitions.items()
            if partition_info.get('database_name') == db_name
        }

    def is_leader(self) -> bool:
        """
        Check if this node is the leader.

        Returns:
            True if this node is the leader, False otherwise
        """
        return self._isLeader()

    def wait_for_sync(self, timeout: float = 5.0) -> bool:
        """
        Wait for this node to sync with the cluster.

        Args:
            timeout: Maximum time to wait in seconds

        Returns:
            True if synced, False if timed out
        """
        start_time = time.time()
        while time.time() - start_time < timeout:
            if self._getLeader() is not None:
                return True
            time.sleep(0.1)
        return False
