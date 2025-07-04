"""
Utility functions for the distributed database system.
"""
import os
import json
import uuid
import logging
from typing import Dict, List, Any, Optional
import requests
from distributed_db.common.config import CLIENT_TIMEOUT

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def generate_id() -> str:
    """Generate a unique ID."""
    return str(uuid.uuid4())


def ensure_directory_exists(directory_path: str) -> None:
    """Ensure that a directory exists, creating it if necessary."""
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)


def make_http_request(url: str, method: str = "GET", data: Optional[Dict] = None) -> Dict:
    """Make an HTTP request to a given URL."""
    try:
        if method.upper() == "GET":
            response = requests.get(url, timeout=CLIENT_TIMEOUT)
        elif method.upper() == "POST":
            response = requests.post(url, json=data, timeout=CLIENT_TIMEOUT)
        elif method.upper() == "PUT":
            response = requests.put(url, json=data, timeout=CLIENT_TIMEOUT)
        elif method.upper() == "DELETE":
            response = requests.delete(url, json=data, timeout=CLIENT_TIMEOUT)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
        
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP request failed: {e}")
        return {"success": False, "error": str(e)}


def load_json_file(file_path: str) -> Dict:
    """Load a JSON file."""
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Failed to load JSON file {file_path}: {e}")
        return {}


def save_json_file(file_path: str, data: Dict) -> bool:
    """Save data to a JSON file."""
    try:
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
        return True
    except Exception as e:
        logging.error(f"Failed to save JSON file {file_path}: {e}")
        return False


def parse_connection_string(connection_string: str) -> Dict[str, str]:
    """Parse a connection string into its components."""
    result = {}
    parts = connection_string.split(';')
    for part in parts:
        if '=' in part:
            key, value = part.split('=', 1)
            result[key.strip()] = value.strip()
    return result


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the given name."""
    return logging.getLogger(name)
