#!/bin/bash
# Demo script for CRUD operations using the distributed database CLI

# Set up colors for better readability
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Use CLI ports to avoid conflicts with tests
CLI_COORDINATOR_PORT=7000
CLI_STORAGE_PORT_1=7100
CLI_STORAGE_PORT_2=7101
CLI_STORAGE_PORT_3=7102

# Function to print section headers
print_header() {
    echo -e "\n${BLUE}==== $1 ====${NC}\n"
}

# Function to run a command with a description
run_command() {
    echo -e "${YELLOW}$ $1${NC}"
    eval "$1"
    echo ""
}

# Function to clean up processes
cleanup() {
    print_header "Cleaning up"

    # Kill background processes
    if [ -n "$COORDINATOR_PID" ]; then
        echo "Stopping coordinator (PID: $COORDINATOR_PID)"
        kill $COORDINATOR_PID 2>/dev/null || true
    fi

    if [ -n "$STORAGE_PID_1" ]; then
        echo "Stopping storage node 1 (PID: $STORAGE_PID_1)"
        kill $STORAGE_PID_1 2>/dev/null || true
    fi

    if [ -n "$STORAGE_PID_2" ]; then
        echo "Stopping storage node 2 (PID: $STORAGE_PID_2)"
        kill $STORAGE_PID_2 2>/dev/null || true
    fi

    if [ -n "$STORAGE_PID_3" ]; then
        echo "Stopping storage node 3 (PID: $STORAGE_PID_3)"
        kill $STORAGE_PID_3 2>/dev/null || true
    fi

    echo "All processes stopped"
}

# Set up trap to clean up on exit
trap cleanup EXIT

# Create schema file for our demo database
print_header "Creating schema file"
cat > demo_schema.json << EOF
{
  "tables": [
    {
      "name": "users",
      "columns": [
        {
          "name": "id",
          "data_type": "INTEGER",
          "primary_key": true
        },
        {
          "name": "name",
          "data_type": "TEXT",
          "nullable": false
        },
        {
          "name": "email",
          "data_type": "TEXT",
          "nullable": false
        },
        {
          "name": "age",
          "data_type": "INTEGER",
          "nullable": true
        }
      ],
      "indexes": ["name", "email"]
    },
    {
      "name": "orders",
      "columns": [
        {
          "name": "id",
          "data_type": "INTEGER",
          "primary_key": true
        },
        {
          "name": "user_id",
          "data_type": "INTEGER",
          "nullable": false
        },
        {
          "name": "product",
          "data_type": "TEXT",
          "nullable": false
        },
        {
          "name": "quantity",
          "data_type": "INTEGER",
          "nullable": false
        },
        {
          "name": "price",
          "data_type": "REAL",
          "nullable": false
        }
      ],
      "indexes": ["user_id"]
    }
  ]
}
EOF
echo -e "${GREEN}Created schema file: demo_schema.json${NC}"

# Start the coordinator
print_header "Starting coordinator"
run_command "python cli.py coordinator --host localhost --port $CLI_COORDINATOR_PORT &"
COORDINATOR_PID=$!
echo -e "${GREEN}Coordinator started with PID: $COORDINATOR_PID${NC}"

# Wait for coordinator to start
echo "Waiting for coordinator to start..."
sleep 3

# Start storage nodes
print_header "Starting storage nodes"
run_command "python cli.py storage-node --host localhost --port $CLI_STORAGE_PORT_1 --coordinator http://localhost:$CLI_COORDINATOR_PORT &"
STORAGE_PID_1=$!
echo -e "${GREEN}Storage node 1 started with PID: $STORAGE_PID_1${NC}"

run_command "python cli.py storage-node --host localhost --port $CLI_STORAGE_PORT_2 --coordinator http://localhost:$CLI_COORDINATOR_PORT &"
STORAGE_PID_2=$!
echo -e "${GREEN}Storage node 2 started with PID: $STORAGE_PID_2${NC}"

run_command "python cli.py storage-node --host localhost --port $CLI_STORAGE_PORT_3 --coordinator http://localhost:$CLI_COORDINATOR_PORT &"
STORAGE_PID_3=$!
echo -e "${GREEN}Storage node 3 started with PID: $STORAGE_PID_3${NC}"

# Wait for storage nodes to register
echo "Waiting for storage nodes to register..."
sleep 3

# Create a database
print_header "Creating database"
run_command "python cli.py create-database --coordinator http://localhost:$CLI_COORDINATOR_PORT --name demo_db --type sql --schema-file demo_schema.json --partition-type horizontal --partition-key id"

# Wait for database to be fully created
echo "Waiting for database to be fully created..."
sleep 2

# List databases
print_header "Listing databases"
run_command "python cli.py list-databases --coordinator http://localhost:$CLI_COORDINATOR_PORT"

# Wait a moment to ensure database is fully registered
sleep 2

# Create insert query files
print_header "Creating insert query files"

# Insert users
cat > insert_user1.json << EOF
{
  "operation": "insert",
  "table": "users",
  "values": {
    "id": 1,
    "name": "John Doe",
    "email": "john@example.com",
    "age": 30
  }
}
EOF
echo -e "${GREEN}Created insert_user1.json${NC}"

cat > insert_user2.json << EOF
{
  "operation": "insert",
  "table": "users",
  "values": {
    "id": 2,
    "name": "Jane Smith",
    "email": "jane@example.com",
    "age": 25
  }
}
EOF
echo -e "${GREEN}Created insert_user2.json${NC}"

# Insert orders
cat > insert_order1.json << EOF
{
  "operation": "insert",
  "table": "orders",
  "values": {
    "id": 1,
    "user_id": 1,
    "product": "Laptop",
    "quantity": 1,
    "price": 999.99
  }
}
EOF
echo -e "${GREEN}Created insert_order1.json${NC}"

cat > insert_order2.json << EOF
{
  "operation": "insert",
  "table": "orders",
  "values": {
    "id": 2,
    "user_id": 1,
    "product": "Mouse",
    "quantity": 2,
    "price": 24.99
  }
}
EOF
echo -e "${GREEN}Created insert_order2.json${NC}"

cat > insert_order3.json << EOF
{
  "operation": "insert",
  "table": "orders",
  "values": {
    "id": 3,
    "user_id": 2,
    "product": "Keyboard",
    "quantity": 1,
    "price": 49.99
  }
}
EOF
echo -e "${GREEN}Created insert_order3.json${NC}"

# Create query files
print_header "Creating query files"

# Query to get all users
cat > query_users.json << EOF
{
  "operation": "query",
  "table": "users",
  "columns": ["*"]
}
EOF
echo -e "${GREEN}Created query_users.json${NC}"

# Query to get all orders
cat > query_orders.json << EOF
{
  "operation": "query",
  "table": "orders",
  "columns": ["*"]
}
EOF
echo -e "${GREEN}Created query_orders.json${NC}"

# Query to get orders for a specific user
cat > query_user_orders.json << EOF
{
  "operation": "query",
  "table": "orders",
  "columns": ["*"],
  "where": {
    "user_id": 1
  }
}
EOF
echo -e "${GREEN}Created query_user_orders.json${NC}"

# Create update query file
cat > update_user.json << EOF
{
  "operation": "update",
  "table": "users",
  "values": {
    "age": 31
  },
  "where": {
    "id": 1
  }
}
EOF
echo -e "${GREEN}Created update_user.json${NC}"

# Create delete query file
cat > delete_order.json << EOF
{
  "operation": "delete",
  "table": "orders",
  "where": {
    "id": 2
  }
}
EOF
echo -e "${GREEN}Created delete_order.json${NC}"

# Perform CRUD operations
print_header "CREATE: Inserting data"
run_command "python cli.py query --coordinator http://localhost:$CLI_COORDINATOR_PORT --db-name demo_db --operation sql --query-file insert_user1.json"
run_command "python cli.py query --coordinator http://localhost:$CLI_COORDINATOR_PORT --db-name demo_db --operation sql --query-file insert_user2.json"
run_command "python cli.py query --coordinator http://localhost:$CLI_COORDINATOR_PORT --db-name demo_db --operation sql --query-file insert_order1.json"
run_command "python cli.py query --coordinator http://localhost:$CLI_COORDINATOR_PORT --db-name demo_db --operation sql --query-file insert_order2.json"
run_command "python cli.py query --coordinator http://localhost:$CLI_COORDINATOR_PORT --db-name demo_db --operation sql --query-file insert_order3.json"

print_header "READ: Querying data"
run_command "python cli.py query --coordinator http://localhost:$CLI_COORDINATOR_PORT --db-name demo_db --operation sql --query-file query_users.json"
run_command "python cli.py query --coordinator http://localhost:$CLI_COORDINATOR_PORT --db-name demo_db --operation sql --query-file query_orders.json"
run_command "python cli.py query --coordinator http://localhost:$CLI_COORDINATOR_PORT --db-name demo_db --operation sql --query-file query_user_orders.json"

print_header "UPDATE: Updating data"
run_command "python cli.py query --coordinator http://localhost:$CLI_COORDINATOR_PORT --db-name demo_db --operation sql --query-file update_user.json"
run_command "python cli.py query --coordinator http://localhost:$CLI_COORDINATOR_PORT --db-name demo_db --operation sql --query-file query_users.json"

print_header "DELETE: Deleting data"
run_command "python cli.py query --coordinator http://localhost:$CLI_COORDINATOR_PORT --db-name demo_db --operation sql --query-file delete_order.json"
run_command "python cli.py query --coordinator http://localhost:$CLI_COORDINATOR_PORT --db-name demo_db --operation sql --query-file query_orders.json"

print_header "Demo completed successfully!"
echo -e "${GREEN}All CRUD operations were demonstrated successfully.${NC}"
echo "Press Enter to clean up and exit..."
read

# Cleanup is handled by the trap
