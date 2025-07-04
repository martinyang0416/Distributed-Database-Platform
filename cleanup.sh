#!/bin/bash
# Cleanup script to remove temporary files created by the demo

# Set up colors for better readability
GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}Cleaning up temporary files...${NC}"

# Remove JSON query files
echo "Removing JSON query files..."
rm -f insert_user1.json
rm -f insert_user2.json
rm -f insert_order1.json
rm -f insert_order2.json
rm -f insert_order3.json
rm -f query_users.json
rm -f query_orders.json
rm -f query_user_orders.json
rm -f update_user.json
rm -f delete_order.json
rm -f demo_schema.json

# Remove NoSQL JSON query files (if they exist)
rm -f insert_user1_nosql.json
rm -f insert_user2_nosql.json
rm -f insert_product1_nosql.json
rm -f insert_product2_nosql.json
rm -f query_users_nosql.json
rm -f query_products_nosql.json
rm -f query_user1_nosql.json
rm -f update_user_nosql.json
rm -f delete_product_nosql.json

# Clean up database files
echo "Cleaning up database files..."
rm -rf data/sql/demo_db.db
rm -rf data/nosql/demo_db.json
rm -rf data/nosql/nosql_demo.json

# Remove any PID files
echo "Removing PID files..."
rm -f coordinator.pid
rm -f storage_node_*.pid

echo -e "${GREEN}Cleanup complete!${NC}"
