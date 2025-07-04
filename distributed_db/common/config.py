"""
Configuration settings for the distributed database system.
"""

# Coordinator settings
COORDINATOR_HOST = "localhost"
COORDINATOR_PORT = 5000
COORDINATOR_CACHE_SIZE = 1000  # Number of items to cache

# Storage node settings
DEFAULT_STORAGE_PORT_START = 5100  # Storage nodes will use ports starting from this number
DEFAULT_REPLICATION_FACTOR = 3  # Number of replicas for each partition

# Consensus settings
RAFT_HEARTBEAT_INTERVAL = 0.5  # seconds
RAFT_ELECTION_TIMEOUT_MIN = 1.5  # seconds
RAFT_ELECTION_TIMEOUT_MAX = 3.0  # seconds

# Database settings
SQL_DB_EXTENSION = ".db"
NOSQL_DB_EXTENSION = ".json"
DB_STORAGE_PATH = "data"  # Relative path to store database files

# Client settings
CLIENT_TIMEOUT = 5.0  # seconds
CLIENT_RETRY_COUNT = 3  # Number of retries for failed operations
