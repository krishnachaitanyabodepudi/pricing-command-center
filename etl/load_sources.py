#!/usr/bin/env python3
"""
Load source data into staging tables
Supports seed mode and late-arriving data injection
"""

import pyodbc
import yaml
import sys
import argparse
import subprocess
from datetime import date, datetime, timedelta
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

def run_seed_sqlcmd(config):
    """Run seed script using sqlcmd (requires SQLCMD mode)"""
    db = config['database']
    script_path = Path(__file__).parent.parent / 'sql' / 'seeds' / 'seed_all_no_r.sql'
    
    if not script_path.exists():
        print(f"Error: Seed script not found at {script_path}", file=sys.stderr)
        return 1
    
    server = f"{db['server']},{db['port']}"
    database = db['database']
    username = db['username']
    password = db['password']
    
    # Use sqlcmd to execute script
    cmd = [
        'sqlcmd',
        '-S', server,
        '-d', database,
        '-U', username,
        '-P', password,
        '-i', str(script_path),
        '-C'  # Trust server certificate
    ]
    
    try:
        print(f"Running seed script via sqlcmd: {script_path}")
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        print(result.stdout)
        print("Seed data loaded successfully.")
        return 0
    except subprocess.CalledProcessError as e:
        print(f"Error running sqlcmd: {e}", file=sys.stderr)
        print(e.stderr, file=sys.stderr)
        return 1
    except FileNotFoundError:
        print("Error: sqlcmd not found. Please install SQL Server Command Line Utilities.", file=sys.stderr)
        return 1

def run_seed_pyodbc(config):
    """Run seed script using pyodbc (reads and executes SQL file)"""
    conn_str = get_connection_string(config)
    script_path = Path(__file__).parent.parent / 'sql' / 'seeds' / 'seed_all_no_r.sql'
    
    if not script_path.exists():
        print(f"Error: Seed script not found at {script_path}", file=sys.stderr)
        return 1
    
    try:
        print(f"Reading seed script: {script_path}")
        with open(script_path, 'r', encoding='utf-8') as f:
            sql_script = f.read()
        
        print("Connecting to database...")
        conn = pyodbc.connect(conn_str, autocommit=False)
        cursor = conn.cursor()
        
        try:
            # Execute script (split by GO statements)
            batches = [b.strip() for b in sql_script.split('GO') if b.strip()]
            
            for i, batch in enumerate(batches, 1):
                print(f"Executing batch {i}/{len(batches)}...")
                cursor.execute(batch)
            
            conn.commit()
            print("Seed data loaded successfully.")
            return 0
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"Error loading seed data: {e}", file=sys.stderr)
        return 1

def inject_late_arriving_data(config):
    """Inject late-arriving data into staging tables"""
    conn_str = get_connection_string(config)
    
    try:
        print("Connecting to database...")
        conn = pyodbc.connect(conn_str, autocommit=False)
        cursor = conn.cursor()
        
        try:
            # Get some existing SKUs, regions, channels
            cursor.execute("""
                SELECT TOP 5 sku, region_code, channel_code 
                FROM pricing.dim_product p
                CROSS JOIN pricing.dim_region r
                CROSS JOIN pricing.dim_channel c
                WHERE p.is_active = 1
                ORDER BY NEWID()
            """)
            keys = cursor.fetchall()
            
            if not keys:
                print("Error: No product/region/channel combinations found", file=sys.stderr)
                return 1
            
            # Late-arriving price history (loaded today, effective_start 30+ days ago)
            print("Inserting late-arriving price history records...")
            late_date = date.today() - timedelta(days=35)
            
            for sku, region_code, channel_code in keys:
                cursor.execute("""
                    INSERT INTO pricing.stg_price_history
                    (sku, region_code, channel_code, price, currency, effective_start, effective_end, source_system, loaded_at)
                    VALUES (?, ?, ?, ?, 'USD', ?, NULL, 'LATE_LOAD_SYS', GETDATE())
                """, sku, region_code, channel_code, 99.99, late_date)
            
            # Late-arriving sales (loaded today, sale_date 30+ days ago)
            print("Inserting late-arriving sales records...")
            late_sale_date = date.today() - timedelta(days=32)
            
            for sku, region_code, channel_code in keys:
                cursor.execute("""
                    INSERT INTO pricing.stg_sales
                    (sale_date, sku, region_code, channel_code, qty, net_sales, source_file, loaded_at)
                    VALUES (?, ?, ?, ?, ?, ?, 'late_sales_2025_02_15.csv', GETDATE())
                """, late_sale_date, sku, region_code, channel_code, 10, 999.90)
            
            conn.commit()
            print(f"Injected {len(keys)} late-arriving price history records")
            print(f"Injected {len(keys)} late-arriving sales records")
            print("Late-arriving data injection completed successfully.")
            return 0
            
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        print(f"Error injecting late-arriving data: {e}", file=sys.stderr)
        return 1

def main():
    parser = argparse.ArgumentParser(description='Load source data into staging tables')
    parser.add_argument('--mode', choices=['seed', 'inject-late'], required=True,
                       help='Operation mode: seed (load seed data) or inject-late (inject late-arriving data)')
    parser.add_argument('--method', choices=['sqlcmd', 'pyodbc'], default='pyodbc',
                       help='Method for seed mode: sqlcmd (requires SQLCMD) or pyodbc (default)')
    
    args = parser.parse_args()
    
    try:
        config = load_config()
    except Exception as e:
        print(f"Error loading configuration: {e}", file=sys.stderr)
        return 1
    
    if args.mode == 'seed':
        if args.method == 'sqlcmd':
            return run_seed_sqlcmd(config)
        else:
            return run_seed_pyodbc(config)
    elif args.mode == 'inject-late':
        return inject_late_arriving_data(config)

if __name__ == '__main__':
    sys.exit(main())
