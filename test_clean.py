#!/usr/bin/env python3
"""
Simple database connection test script
"""

import sys
print("Script starting...")

try:
    import sqlalchemy
    print("Successfully imported SQLAlchemy")
except ImportError as e:
    print(f"Error importing SQLAlchemy: {e}")
    print("Please install it with: pip install sqlalchemy")
    sys.exit(1)

try:
    import pandas as pd
    print("Successfully imported pandas")
except ImportError as e:
    print(f"Error importing pandas: {e}")
    print("Please install it with: pip install pandas")
    sys.exit(1)

try:
    from sqlalchemy import create_engine, inspect, text
    print("Successfully imported SQLAlchemy components")
except ImportError as e:
    print(f"Error importing SQLAlchemy components: {e}")
    sys.exit(1)

print("Attempting to connect to database...")

try:
    # Get connection parameters from command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Database connection test')
    parser.add_argument('--host', required=True, help='Database host')
    parser.add_argument('--port', type=int, default=5432, help='Database port')
    parser.add_argument('--database', required=True, help='Database name')
    parser.add_argument('--user', required=True, help='Database user')
    parser.add_argument('--password', required=True, help='Database password')
    
    args = parser.parse_args()
    print(f"Connecting to database {args.database} on {args.host}:{args.port}")
    
    # Create SQLAlchemy engine
    conn_str = f"postgresql://{args.user}:{args.password}@{args.host}:{args.port}/{args.database}"
    engine = create_engine(conn_str)
    print("Successfully created engine")
    
    # Test connection
    connection = engine.connect()
    print("Successfully connected to the database")
    
    # Get tables
    inspector = inspect(engine)
    tables = inspector.get_table_names()
    print(f"Found {len(tables)} tables: {', '.join(tables)}")
    
    






    connection.close()
    print("Connection closed")
    
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("Script completed successfully")