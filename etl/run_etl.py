#!/usr/bin/env python3
"""
ETL Runner for pricing_refresh pipeline
Executes pricing.sp_refresh_pricing_mart stored procedure
"""

import pyodbc
import yaml
import sys
import os
from pathlib import Path

def load_config(config_path='config.yaml'):
    """Load configuration from YAML file"""
    config_file = Path(__file__).parent / config_path
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

def get_connection_string(config):
    """Build SQL Server connection string"""
    db = config['database']
    driver = db['driver']
    server = db['server']
    port = db['port']
    database = db['database']
    username = db['username']
    password = db['password']
    
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server},{port};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"TrustServerCertificate=yes;"
    )

def run_etl():
    """Execute ETL pipeline"""
    try:
        # Load configuration
        config = load_config()
        
        # Get connection string
        conn_str = get_connection_string(config)
        
        # Connect to database
        print("Connecting to database...")
        conn = pyodbc.connect(conn_str, autocommit=False)
        cursor = conn.cursor()
        
        try:
            # Set SESSION_CONTEXT for ETL
            print("Setting ETL session context...")
            cursor.execute("EXEC sp_set_session_context @key = N'is_etl', @value = 1")
            
            # Execute stored procedure
            print("Executing pricing.sp_refresh_pricing_mart...")
            cursor.execute("EXEC pricing.sp_refresh_pricing_mart")
            
            # Fetch results
            results = cursor.fetchall()
            columns = [column[0] for column in cursor.description]
            
            # Commit transaction
            conn.commit()
            
            # Print results
            print("\n=== ETL Run Results ===")
            for row in results:
                result_dict = dict(zip(columns, row))
                print(f"Run ID: {result_dict.get('run_id')}")
                print(f"Status: {result_dict.get('status')}")
                print(f"Rows Loaded: {result_dict.get('rows_loaded')}")
                print(f"Rows Rejected: {result_dict.get('rows_rejected')}")
            
            print("\nETL run completed successfully.")
            return 0
            
        except Exception as e:
            # Rollback on error
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()
            
    except pyodbc.Error as e:
        print(f"Database error: {e}", file=sys.stderr)
        return 1
    except FileNotFoundError as e:
        print(f"Configuration file not found: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

if __name__ == '__main__':
    sys.exit(run_etl())
