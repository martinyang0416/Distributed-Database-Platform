"""
Query router for the client library.
"""
import json
import time
from typing import Dict, List, Any, Optional, Tuple
import requests
from distributed_db.common.utils import get_logger
from distributed_db.common.config import CLIENT_TIMEOUT, CLIENT_RETRY_COUNT

logger = get_logger(__name__)


class QueryRouter:
    """
    Routes queries to the appropriate coordinator or storage nodes.
    """

    def __init__(self, coordinator_url: str):
        """
        Initialize the query router.

        Args:
            coordinator_url: URL of the coordinator
        """
        self.coordinator_url = coordinator_url
        logger.info(f"Query router initialized with coordinator URL {coordinator_url}")

    def route_query(
        self,
        db_name: str,
        query_data: Dict[str, Any],
        direct_node: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Route a query to the appropriate node.

        Args:
            db_name: Name of the database
            query_data: Query data
            direct_node: Optional storage node to query directly

        Returns:
            Query result
        """
        if direct_node:
            # Direct query to a specific storage node
            return self._query_storage_node(direct_node, db_name, query_data)
        else:
            # Route through coordinator
            return self._query_coordinator(db_name, query_data)

    def _query_coordinator(self, db_name: str, query_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send a query to the coordinator.

        Args:
            db_name: Name of the database
            query_data: Query data

        Returns:
            Query result
        """
        url = f"{self.coordinator_url}/query/{db_name}"

        for attempt in range(CLIENT_RETRY_COUNT):
            try:
                response = requests.post(url, json=query_data, timeout=CLIENT_TIMEOUT)
                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                logger.error(f"Error querying coordinator (attempt {attempt + 1}/{CLIENT_RETRY_COUNT}): {e}")

                if attempt < CLIENT_RETRY_COUNT - 1:
                    # Exponential backoff
                    time.sleep(2 ** attempt)
                else:
                    return {
                        "success": False,
                        "error": f"Failed to query coordinator after {CLIENT_RETRY_COUNT} attempts: {str(e)}"
                    }

    def _query_storage_node(
        self,
        node: Dict[str, Any],
        db_name: str,
        query_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Send a query directly to a storage node.

        Args:
            node: Storage node information
            db_name: Name of the database
            query_data: Query data

        Returns:
            Query result
        """
        url = f"http://{node['host']}:{node['port']}/query/{db_name}"

        for attempt in range(CLIENT_RETRY_COUNT):
            try:
                response = requests.post(url, json=query_data, timeout=CLIENT_TIMEOUT)
                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                logger.error(f"Error querying storage node (attempt {attempt + 1}/{CLIENT_RETRY_COUNT}): {e}")

                # If this is a connection error or timeout, try to notify the coordinator
                if isinstance(e, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)):
                    try:
                        # Notify coordinator that this node is down
                        notify_url = f"{self.coordinator_url}/nodes/{node.get('id', '')}/status"
                        requests.post(notify_url, json={"status": "inactive"}, timeout=CLIENT_TIMEOUT)
                        logger.info(f"Notified coordinator that node {node.get('id', '')} is inactive")
                    except Exception as notify_error:
                        logger.error(f"Failed to notify coordinator about node failure: {notify_error}")

                if attempt < CLIENT_RETRY_COUNT - 1:
                    # Exponential backoff
                    time.sleep(2 ** attempt)
                else:
                    # If all retries failed, try to route through coordinator instead
                    try:
                        logger.info(f"Retrying query through coordinator after direct node failure")
                        return self._query_coordinator(db_name, query_data)
                    except Exception as coord_error:
                        logger.error(f"Failed to query through coordinator as fallback: {coord_error}")
                        return {
                            "success": False,
                            "error": f"Failed to query storage node after {CLIENT_RETRY_COUNT} attempts: {str(e)}"
                        }

    def get_storage_nodes(self) -> List[Dict[str, Any]]:
        """
        Get a list of all storage nodes.

        Returns:
            List of storage node information
        """
        url = f"{self.coordinator_url}/nodes"

        try:
            response = requests.get(url, timeout=CLIENT_TIMEOUT)
            response.raise_for_status()
            data = response.json()

            if data.get('success', False):
                return list(data.get('nodes', {}).values())
            else:
                logger.error(f"Error getting storage nodes: {data.get('error')}")
                return []

        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting storage nodes: {e}")
            return []

    def get_database_partitions(self, db_name: str) -> List[Dict[str, Any]]:
        """
        Get a list of partitions for a database.

        Args:
            db_name: Name of the database

        Returns:
            List of partition information
        """
        url = f"{self.coordinator_url}/partitions/{db_name}"

        try:
            response = requests.get(url, timeout=CLIENT_TIMEOUT)
            response.raise_for_status()
            data = response.json()

            if data.get('success', False):
                return list(data.get('partitions', {}).values())
            else:
                logger.error(f"Error getting partitions for database '{db_name}': {data.get('error')}")
                return []

        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting partitions for database '{db_name}': {e}")
            return []
