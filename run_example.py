#!/usr/bin/env python3
"""
Example script demonstrating the usage of the distributed database system.
"""
import time
import json
from distributed_db.client.client import DatabaseClient
from distributed_db.common.models import (
    DatabaseType, PartitionType, TableSchema, ColumnDefinition
)


def main():
    """Run an example demonstrating the distributed database functionality."""
    # Create client
    client = DatabaseClient('http://localhost:5000')
    
    print("Connected to coordinator. Creating example databases...")
    
    # Create SQL database
    columns = [
        ColumnDefinition(name='id', data_type='INTEGER', primary_key=True),
        ColumnDefinition(name='name', data_type='TEXT', nullable=False),
        ColumnDefinition(name='email', data_type='TEXT')
    ]
    table = TableSchema(name='users', columns=columns, indexes=['name'])
    success, error = client.create_database(
        name='example_sql',
        db_type=DatabaseType.SQL,
        tables=[table],
        partition_type=PartitionType.HORIZONTAL,
        partition_key='id'
    )
    
    if success:
        print("Created SQL database 'example_sql'")
    else:
        print(f"Failed to create SQL database: {error}")
    
    # Create NoSQL database
    success, error = client.create_database(
        name='example_nosql',
        db_type=DatabaseType.NOSQL,
        partition_type=PartitionType.HORIZONTAL,
        partition_key='_id'
    )
    
    if success:
        print("Created NoSQL database 'example_nosql'")
    else:
        print(f"Failed to create NoSQL database: {error}")
    
    # Insert data into SQL database
    print("\nInserting data into SQL database...")
    for i in range(1, 6):
        result = client.sql_insert(
            'example_sql',
            'users',
            {'id': i, 'name': f'User {i}', 'email': f'user{i}@example.com'}
        )
        if result.success:
            print(f"Inserted user {i}")
        else:
            print(f"Failed to insert user {i}: {result.error}")
    
    # Query SQL database
    print("\nQuerying SQL database...")
    result = client.sql_query('example_sql', 'users')
    if result.success:
        print("All users:")
        for user in result.data:
            print(f"  {user['id']}: {user['name']} ({user['email']})")
    else:
        print(f"Query failed: {result.error}")
    
    # Insert data into NoSQL database
    print("\nInserting data into NoSQL database...")
    for i in range(1, 6):
        result = client.nosql_insert(
            'example_nosql',
            {'name': f'Document {i}', 'value': i, 'tags': [f'tag{j}' for j in range(1, i+1)]}
        )
        if result.success:
            print(f"Inserted document {i}")
        else:
            print(f"Failed to insert document {i}: {result.error}")
    
    # Query NoSQL database
    print("\nQuerying NoSQL database...")
    result = client.nosql_get('example_nosql')
    if result.success:
        print("All documents:")
        for doc in result.data:
            print(f"  {doc.get('_id')}: {doc.get('name')} (tags: {doc.get('tags')})")
    else:
        print(f"Query failed: {result.error}")
    
    print("\nExample completed successfully!")


if __name__ == "__main__":
    main()
