"""
Data models for the distributed database system.
"""
from enum import Enum
from typing import Dict, List, Optional, Union, Any
from dataclasses import dataclass


class DatabaseType(Enum):
    """Type of database: SQL or NoSQL."""
    SQL = "SQL"
    NOSQL = "NOSQL"


class PartitionType(Enum):
    """Type of partitioning strategy."""
    HORIZONTAL = "HORIZONTAL"  # Row-based partitioning
    VERTICAL = "VERTICAL"      # Column-based partitioning
    MIXED = "MIXED"            # Combination of horizontal and vertical


@dataclass
class ColumnDefinition:
    """Definition of a column in a SQL database."""
    name: str
    data_type: str
    primary_key: bool = False
    nullable: bool = True
    default: Optional[Any] = None


@dataclass
class TableSchema:
    """Schema definition for a SQL table."""
    name: str
    columns: List[ColumnDefinition]
    indexes: List[str] = None


@dataclass
class DatabaseSchema:
    """Schema definition for a database."""
    name: str
    db_type: DatabaseType
    tables: List[TableSchema] = None  # For SQL databases
    partition_type: PartitionType = PartitionType.HORIZONTAL
    partition_key: Optional[str] = None  # Column/field to partition by


@dataclass
class StorageNode:
    """Information about a storage node."""
    id: str
    host: str
    port: int
    status: str = "active"  # active, inactive, syncing
    databases: List[str] = None


@dataclass
class Partition:
    """Information about a database partition."""
    id: str
    database_name: str
    partition_type: PartitionType
    partition_key: str
    partition_value: Optional[Any] = None  # For horizontal partitioning
    columns: List[str] = None  # For vertical partitioning
    storage_nodes: List[str] = None  # IDs of storage nodes containing this partition


@dataclass
class QueryResult:
    """Result of a database query."""
    success: bool
    data: Optional[Union[List[Dict], Dict]] = None
    error: Optional[str] = None
    affected_rows: int = 0
