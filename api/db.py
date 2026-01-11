"""
Database connection and query helpers
"""

import pyodbc
import yaml
from pathlib import Path
from typing import List, Tuple, Optional, Any

def load_config(config_path='../etl/config.yaml'):
    """Load database configuration from ETL config file"""
    config_file = Path(__file__).parent.parent / 'etl' / 'config.yaml'
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

def get_conn():
    """Create and return a new database connection"""
    config = load_config()
    conn_str = get_connection_string(config)
    return pyodbc.connect(conn_str, autocommit=True)

def fetch_all(query: str, params: Optional[Tuple] = None) -> List[dict]:
    """
    Execute query and return all rows as list of dictionaries
    Uses parameterized queries for safety
    Supports multi-statement queries (e.g., DECLARE variable; SELECT ...)
    """
    conn = None
    cursor = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        # Skip DECLARE results if present (no result set)
        while cursor.description is None:
            if not cursor.nextset():
                break
        
        # Get column names from first result set with data
        columns = [column[0] for column in cursor.description] if cursor.description else []
        
        # Fetch all rows and convert to dict
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def fetch_one(query: str, params: Optional[Tuple] = None) -> Optional[dict]:
    """
    Execute query and return first row as dictionary, or None if no rows
    Uses parameterized queries for safety
    """
    conn = None
    cursor = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        # Get column names
        columns = [column[0] for column in cursor.description] if cursor.description else []
        
        # Fetch one row and convert to dict
        row = cursor.fetchone()
        if row:
            return dict(zip(columns, row))
        return None
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
