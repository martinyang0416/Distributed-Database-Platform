#!/usr/bin/env python3
"""
Script to start a storage node.
"""
import os
import argparse
from distributed_db.storage.storage_node import StorageNode
from distributed_db.common.config import COORDINATOR_HOST, COORDINATOR_PORT, DEFAULT_STORAGE_PORT_START, DB_STORAGE_PATH
from distributed_db.common.utils import ensure_directory_exists


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Start a storage node')
    parser.add_argument('--host', default='localhost', help='Host to bind to')
    parser.add_argument('--port', type=int, default=0, help='Port to bind to (0 for auto)')
    parser.add_argument('--data-dir', default=DB_STORAGE_PATH, help='Directory to store data')
    parser.add_argument('--coordinator', default=f'http://{COORDINATOR_HOST}:{COORDINATOR_PORT}', help='Coordinator URL')
    parser.add_argument('--node-id', help='Node ID (generated if not provided)')

    args = parser.parse_args()

    # Auto-assign port if not specified
    if args.port == 0:
        import socket

        # Find an available port
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('', 0))
            args.port = s.getsockname()[1]

    # Ensure data directory exists
    ensure_directory_exists(args.data_dir)

    # Start storage node
    print(f"Starting storage node at {args.host}:{args.port}...")
    node = StorageNode(
        host=args.host,
        port=args.port,
        data_dir=args.data_dir,
        coordinator_url=args.coordinator,
        node_id=args.node_id
    )

    node.start()


if __name__ == "__main__":
    main()
