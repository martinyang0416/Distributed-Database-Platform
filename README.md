# Distributed Database Platform

A robust, fault-tolerant distributed database platform that supports both SQL and NoSQL databases with advanced features like partitioning, replication, consensus, and in-memory caching.

This project implements a complete distributed database system with a focus on reliability, scalability, and ease of use. It uses a coordinator-based architecture with multiple storage nodes and implements a consensus algorithm for fault tolerance.

## Features

- Support for both SQL and NoSQL databases
- Horizontal and vertical partitioning and mixed partitioning strategies
- Data replication across multiple storage nodes
- In-memory caching for popular content
- Consensus mechanism for fault tolerance
- Simple client library and CLI for easy integration

## Tech Stack

### Core Technologies
- **Python 3.8+**: Primary programming language
- **Flask**: Web framework for RESTful API endpoints
- **SQLite3**: Embedded SQL database engine
- **TinyDB**: Lightweight document-oriented database for NoSQL support
- **Requests**: HTTP library for service communication

### Database Technologies
- **SQL**: Relational data storage with SQLite
- **NoSQL**: Document-oriented storage with TinyDB
- **In-memory caching**: Custom implementation without external dependencies

### System Components
- **Coordinator**: Central management service
- **Storage Nodes**: Distributed data storage services
- **Client Library**: Python API for application integration
- **Command-Line Interface (CLI)**: Tool for interacting with the system

## Architecture

The system implements a distributed database architecture with a coordinator-based design that supports both SQL and NoSQL data models, horizontal and vertical partitioning, and built-in fault tolerance.

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Client App │     │     CLI     │     │ Client App  │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       │                   │                   │
       │                   ▼                   │
       │           ┌───────────────┐           │
       └──────────►│  Coordinator  │◄──────────┘
                   │   (Master)    │
                   └───────┬───────┘
                           │
                           │
         ┌─────────────────┼─────────────────┐
         │                 │                 │
         ▼                 ▼                 ▼
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│  Storage Node 1 │ │  Storage Node 2 │ │  Storage Node 3 │
│                 │ │                 │ │                 │
│ ┌─────┐ ┌─────┐ │ │ ┌─────┐ ┌─────┐ │ │ ┌─────┐ ┌─────┐ │
│ │ SQL │ │NoSQL│ │ │ │ SQL │ │NoSQL│ │ │ │ SQL │ │NoSQL│ │
│ └─────┘ └─────┘ │ │ └─────┘ └─────┘ │ │ └─────┘ └─────┘ │
└─────────────────┘ └─────────────────┘ └─────────────────┘
```

### System Components

1. **Coordinator**: Central management server that routes requests, manages schemas, and coordinates storage nodes. Features include in-memory caching, consensus management, and result aggregation.

2. **Storage Nodes**: Distributed servers that store and process data using SQLite (SQL) and TinyDB (NoSQL). They handle partitioning, replication, and query execution.

3. **Client Library**: API that simplifies database interactions, handling communication with the coordinator and abstracting away distributed system complexity.

4. **Command-Line Interface**: User-friendly tool for database operations, system management, and monitoring.

### Data Flow and Processing

1. **Request Flow**: Client → Coordinator → Storage Nodes → Coordinator → Client

2. **Partitioning**:
   - **Horizontal**: Distributes rows across nodes by partition key (e.g., ID ranges)
   - **Vertical**: Splits columns/attributes across nodes (for NoSQL)
   - **Mixed**: Combines both strategies for complex data models

3. **Consistency**: Consensus protocol ensures metadata consistency across nodes and handles failure recovery

4. **Performance**: In-memory caching, intelligent query routing, and connection pooling optimize performance


## Project Structure

```
distributed_db/
├── client/                 # Client library for database access
│   ├── client.py           # Main client interface
│   └── query_router.py     # Routes queries to appropriate nodes
├── common/                 # Shared utilities and models
│   ├── models.py           # Data models and schemas
│   └── utils.py            # Utility functions
├── coordinator/            # Coordinator service
│   ├── cache_manager.py    # In-memory caching
│   ├── consensus.py        # Raft consensus implementation
│   ├── coordinator.py      # Main coordinator service
│   └── schema_manager.py   # Database schema management
├── storage/                # Storage node implementation
│   ├── nosql_storage.py    # NoSQL database operations
│   ├── partition_manager.py # Data partitioning
│   ├── replication_manager.py # Data replication
│   ├── sql_storage.py      # SQL database operations
│   └── storage_node.py     # Storage node service
└── tests/                  # Test suite
    ├── test_consensus.py   # Tests for consensus mechanism
    ├── test_crud.py        # Tests for basic operations
    ├── test_partitioning.py # Tests for data partitioning
    └── test_concurrent_clients.py # Tests for concurrent access
    └── more tests...
```

## Requirements

- Python 3.8+
- Flask
- TinyDB
- PySQLite3
- PySyncObj
- Requests

## Installation

1. Clone the repository:

```bash
git clone https://github.com/yourusername/distributed-db.git
cd distributed-db
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

## Usage

### Command-Line Interface (CLI)

The system provides a CLI for interacting with the distributed database:

```bash
# Core Commands
python cli.py --help                                                # Get help
python cli.py coordinator --host localhost --port 5000              # Start coordinator
python cli.py storage-node --port 5100 --coordinator http://localhost:5000  # Start storage node

# Database Operations
python cli.py list-databases --coordinator http://localhost:5000    # List databases
python cli.py create-database --coordinator http://localhost:5000 --name mydb \
    --type sql --schema-file schema.json --partition-type horizontal --partition-key id  # Create SQL DB
python cli.py create-database --coordinator http://localhost:5000 --name mydocs \
    --type nosql --partition-type vertical  # Create NoSQL DB

# Query Operations
python cli.py query --coordinator http://localhost:5000 --db-name mydb \
    --operation sql --query-file query.json  # Execute SQL query
python cli.py query --coordinator http://localhost:5000 --db-name mydocs \
    --operation nosql --query-file query.json  # Execute NoSQL query
```

#### Demo Scripts

The project includes demo scripts that showcase the system's capabilities:

```bash
./demo_crud.sh

# Clean up demo files
./cleanup.sh

Below are some outputs from demo scripts
==== Creating database ====

$ python cli.py create-database --coordinator http://localhost:7000 --name demo_db --type sql --schema-file demo_schema.json --partition-type horizontal --partition-key id
2025-05-02 16:28:50,064 - distributed_db.client.query_router - INFO - Query router initialized with coordinator URL http://localhost:7000
2025-05-02 16:28:50,064 - distributed_db.client.client - INFO - Database client initialized with coordinator URL http://localhost:7000
2025-05-02 16:28:50,075 - distributed_db.coordinator.coordinator - INFO - Added database demo_db to consensus store
2025-05-02 16:28:50,076 - distributed_db.coordinator.coordinator - INFO - Created horizontal partition for database 'demo_db'
2025-05-02 16:28:50,076 - werkzeug - INFO - 127.0.0.1 - - [02/May/2025 16:28:50] "POST /databases HTTP/1.1" 200 -
2025-05-02 16:28:50,076 - distributed_db.client.client - INFO - Created database 'demo_db'
2025-05-02 16:28:50,081 - werkzeug - INFO - 127.0.0.1 - - [02/May/2025 16:28:50] "GET /partitions/demo_db HTTP/1.1" 200 -
2025-05-02 16:28:50,081 - distributed_db.client.client - INFO - Waiting for database 'demo_db' to be fully ready (attempt 1/3)
2025-05-02 16:28:50,173 - distributed_db.coordinator.consensus - INFO - Added partition b9530a7a-e98b-46fb-81f8-5829a149c743 to consensus store
2025-05-02 16:28:51,091 - werkzeug - INFO - 127.0.0.1 - - [02/May/2025 16:28:51] "GET /partitions/demo_db HTTP/1.1" 200 -
2025-05-02 16:28:51,091 - distributed_db.client.client - INFO - Database 'demo_db' is ready with 1 partitions
Database 'demo_db' created successfully.

...
...
...
==== Creating insert query files ====

Created insert_user1.json
Created insert_user2.json
Created insert_order1.json
Created insert_order2.json
Created insert_order3.json

==== Creating query files ====

Created query_users.json
Created query_orders.json
Created query_user_orders.json
Created update_user.json
Created delete_order.json

==== CREATE: Inserting data ====

$ python cli.py query --coordinator http://localhost:7000 --db-name demo_db --operation sql --query-file insert_user1.json
2025-05-02 16:28:55,744 - distributed_db.client.query_router - INFO - Query router initialized with coordinator URL http://localhost:7000
2025-05-02 16:28:55,744 - distributed_db.client.client - INFO - Database client initialized with coordinator URL http://localhost:7000
2025-05-02 16:28:55,763 - distributed_db.storage.storage_node - INFO - Received query for database 'demo_db': {'operation': 'insert', 'table': 'users', 'values': {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'age': 30}}
2025-05-02 16:28:55,764 - distributed_db.storage.storage_node - INFO - Partitions for database 'demo_db': []
2025-05-02 16:28:55,764 - distributed_db.storage.storage_node - INFO - Database 'demo_db' doesn't exist, creating it for insert operation
2025-05-02 16:28:55,766 - distributed_db.storage.storage_node - INFO - Created SQL database 'demo_db'
2025-05-02 16:28:55,768 - distributed_db.storage.sql_storage - INFO - Creating table: CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age TEXT)
2025-05-02 16:28:55,768 - distributed_db.storage.sql_storage - INFO - Created table 'users' in database 'demo_db'
2025-05-02 16:28:55,768 - distributed_db.storage.sql_storage - INFO - Executing SQL insert: INSERT INTO users (id, name, email, age) VALUES (?, ?, ?, ?) with values [1, 'John Doe', 'john@example.com', 30]
2025-05-02 16:28:55,769 - distributed_db.storage.sql_storage - INFO - Selecting inserted row: SELECT * FROM users WHERE id = ? with id 1
2025-05-02 16:28:55,769 - distributed_db.storage.sql_storage - INFO - Inserted row: {'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'age': '30'}
2025-05-02 16:28:55,769 - distributed_db.storage.storage_node - INFO - SQL query result after creating database: [{'id': 1, 'name': 'John Doe', 'email': 'john@example.com', 'age': '30'}]
2025-05-02 16:28:55,769 - werkzeug - INFO - 127.0.0.1 - - [02/May/2025 16:28:55] "POST /query/demo_db HTTP/1.1" 200 -
2025-05-02 16:28:55,770 - werkzeug - INFO - 127.0.0.1 - - [02/May/2025 16:28:55] "POST /query/demo_db HTTP/1.1" 200 -
Query results:
{
  "age": "30",
  "email": "john@example.com",
  "id": 1,
  "name": "John Doe"
}
Affected rows: 1
...
...
...
```
##### You can simpliy check logs/demo_crud_log.txt for whole log.

## Testing
**We used LLM (ChatGPT) on generating some testcases and README (more for formatting and garmmar purpose)**

The project includes a comprehensive test suite that verifies all aspects of the distributed database system. The tests are designed to validate functionality, performance, and fault tolerance.

### Test Categories

1. **Schema Definition and Basic CRUD Operations (20 points)**
   - Tests database creation with various schemas (SQL and NoSQL)
   - Validates basic Create, Read, Update, Delete operations
   - Tests SQL operations (tables, rows, columns)
   - Tests NoSQL operations (collections, documents)
   - 11 test cases

2. **Horizontal Partitioning and Routing (30 points)**
   - Tests data distribution across nodes based on partition key
   - Validates routing of queries to appropriate nodes
   - Tests partition management and metadata
   - Verifies data integrity across partitions
   - 20 test cases

3. **Vertical Partitioning (30 points)**
   - Tests column-based partitioning for NoSQL databases
   - Validates attribute distribution across nodes
   - Tests query aggregation from multiple nodes
   - Verifies complete document reconstruction
   - 18 test cases

4. **Consensus and Fault Tolerance (30 points)**
   - Tests consensus protocol for system-wide state management
   - Validates node failure detection and recovery
   - Tests metadata consistency across nodes
   - Verifies system behavior during node failures
   - 10 test cases

5. **Coordinator Caching (40 points)**
   - Tests in-memory caching for frequently accessed data
   - Validates cache invalidation and coherence
   - Tests performance improvements with caching
   - Verifies correct behavior with cached vs. uncached data
   - 20 test cases

6. **Concurrent Clients and Consistency Under Load (50 points)**
   - Tests multiple clients accessing the database simultaneously
   - Validates transaction isolation and consistency
   - Tests system performance under high concurrency
   - Verifies data integrity during concurrent operations
   - 19 test cases

### Running Tests

There are two ways to run the tests:

#### 1. Using the Shell Script (Recommended)

The easiest way to run all tests is using the provided shell script:

```bash
# Make the script executable (if needed)
chmod +x run_all_tests.sh

# Run all tests
./run_all_tests.sh
```

This script:
- Runs each test module individually to ensure proper test discovery
- Provides clear section headers for each test category
- Ensures tests run in the correct order
- Saves detailed logs to `run_tests_log.txt`

#### 2. Using the Python Test Runner

You can also run tests directly using the Python test runner:

```bash
# Run all tests
python run_tests.py

# Run a specific test module
python run_tests.py tests.test_consensus

# Run tests with make
make final
```


### Test Output

The test runner provides a clean, readable output showing:
- Descriptive test names extracted from docstrings
- Pass/Fail status for each test
- Summary of passed/failed tests by category
- Total points earned

Example output:
```
Vertical Partitioning:
  Test creating a table schema suitable for vertical partitioning. Pass
  Test creating a database with vertical partitioning. Pass
  Test that the vertically partitioned database exists. Pass
  Test that the database has vertical partition type. Pass
  ...
  Summary: 18/18 tests passed

============================================================
TEST SUMMARY
============================================================
Vertical Partitioning (30 points):
  18/18 tests passed
  Points earned: 15/15
```

