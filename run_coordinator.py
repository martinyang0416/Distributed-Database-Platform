#!/usr/bin/env python3
"""
Script to start the coordinator service.
"""
import argparse
from distributed_db.coordinator.coordinator import Coordinator
from distributed_db.common.config import COORDINATOR_HOST, COORDINATOR_PORT, DB_STORAGE_PATH
from distributed_db.common.utils import ensure_directory_exists


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description='Start the coordinator service')
    parser.add_argument('--host', default=COORDINATOR_HOST, help='Host to bind to')
    parser.add_argument('--port', type=int, default=COORDINATOR_PORT, help='Port to bind to')
    parser.add_argument('--data-dir', default=DB_STORAGE_PATH, help='Directory to store data')
    parser.add_argument('--replication-factor', type=int, default=3, help='Replication factor')

    args = parser.parse_args()

    # Ensure data directory exists
    ensure_directory_exists(args.data_dir)

    # Start coordinator
    print(f"Starting coordinator at {args.host}:{args.port}...")
    coordinator = Coordinator(
        host=args.host,
        port=args.port,
        data_dir=args.data_dir,
        replication_factor=args.replication_factor
    )

    coordinator.start()


if __name__ == "__main__":
    main()
