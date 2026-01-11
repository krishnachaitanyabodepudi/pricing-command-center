#!/usr/bin/env python3
"""
Data Quality checks for pricing data warehouse
Validates fact table data quality and reports issues
"""

import pyodbc
import yaml
import sys
import json
from datetime import datetime, timezone
from pathlib import Path

def load_config(config_path='../etl/config.yaml'):
    """Load database configuration from ETL config file"""
    config_file = Path(__file__).parent.parent / 'etl' / 'config.yaml'
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

def get_connection_string(config, driver_override=None):
    """Build SQL Server connection string with driver fallback"""
    db = config['database']
    driver = driver_override or db['driver']
    
    # Adjust connection string for older 'SQL Server' driver if needed
    if driver == 'SQL Server':
        return (
            f"DRIVER={{{driver}}};"
            f"SERVER={db['server']},{db['port']};"
            f"DATABASE={db['database']};"
            f"UID={db['username']};"
            f"PWD={db['password']};"
        )
    else:
        return (
            f"DRIVER={{{driver}}};"
            f"SERVER={db['server']},{db['port']};"
            f"DATABASE={db['database']};"
            f"UID={db['username']};"
            f"PWD={db['password']};"
            f"TrustServerCertificate=yes;"
        )

def connect_to_db(config):
    """Connect to database, trying common drivers as fallback"""
    db = config['database']
    specified_driver = db['driver']
    available_drivers = pyodbc.drivers()
    
    # Try the specified driver first
    drivers_to_try = [specified_driver]
    
    # Add common fallback drivers if available and not already in list
    common_fallbacks = [
        'ODBC Driver 17 for SQL Server',
        'ODBC Driver 18 for SQL Server',
        'ODBC Driver 13 for SQL Server',
        'SQL Server',
        'SQL Server Native Client 11.0'
    ]
    for fallback in common_fallbacks:
        if fallback in available_drivers and fallback not in drivers_to_try:
            drivers_to_try.append(fallback)
    
    last_error = None
    for attempt_driver in drivers_to_try:
        try:
            conn_str = get_connection_string(config, attempt_driver)
            conn = pyodbc.connect(conn_str, timeout=10)
            return conn
        except pyodbc.Error as e:
            last_error = e
            continue
    
    # If all attempts failed, raise the last error
    raise last_error

def check_negative_prices(cursor):
    """Check 1: Negative prices in fact_price_history"""
    query = """
        SELECT COUNT(*) AS issue_count
        FROM pricing.fact_price_history
        WHERE price < 0
    """
    cursor.execute(query)
    count = cursor.fetchone()[0]
    
    status = "PASS" if count == 0 else "FAIL"
    sample_query = "SELECT TOP 5 * FROM pricing.fact_price_history WHERE price < 0"
    
    return {
        "name": "Negative Prices",
        "status": status,
        "count": count,
        "sample_query": sample_query,
        "critical": True
    }

def check_overlapping_ranges(cursor):
    """Check 2: Overlapping effective ranges in fact_price_history"""
    query = """
        WITH PriceRanges AS (
            SELECT 
                sku,
                region_code,
                channel_code,
                effective_start,
                ISNULL(effective_end, '9999-12-31') AS effective_end,
                price_hist_id
            FROM pricing.fact_price_history
        ),
        Overlaps AS (
            SELECT COUNT(*) AS issue_count
            FROM PriceRanges pr1
            INNER JOIN PriceRanges pr2
                ON pr1.sku = pr2.sku
                AND pr1.region_code = pr2.region_code
                AND pr1.channel_code = pr2.channel_code
                AND pr1.price_hist_id < pr2.price_hist_id
            WHERE pr1.effective_start < pr2.effective_end
                AND pr2.effective_start < pr1.effective_end
        )
        SELECT ISNULL(issue_count, 0) FROM Overlaps
    """
    cursor.execute(query)
    count = cursor.fetchone()[0]
    
    status = "PASS" if count == 0 else "FAIL"
    sample_query = """
        WITH PriceRanges AS (
            SELECT sku, region_code, channel_code, effective_start,
                   ISNULL(effective_end, '9999-12-31') AS effective_end, price_hist_id
            FROM pricing.fact_price_history
        )
        SELECT TOP 5 pr1.*, pr2.effective_start AS overlap_start, pr2.effective_end AS overlap_end
        FROM PriceRanges pr1
        INNER JOIN PriceRanges pr2
            ON pr1.sku = pr2.sku AND pr1.region_code = pr2.region_code
            AND pr1.channel_code = pr2.channel_code AND pr1.price_hist_id < pr2.price_hist_id
        WHERE pr1.effective_start < pr2.effective_end AND pr2.effective_start < pr1.effective_end
    """
    
    return {
        "name": "Overlapping Effective Ranges",
        "status": status,
        "count": count,
        "sample_query": sample_query,
        "critical": True
    }

def check_orphan_facts(cursor):
    """Check 3: Orphan facts (missing dimension references)"""
    # Check fact_sales orphans
    query_sales = """
        SELECT COUNT(*) AS issue_count
        FROM pricing.fact_sales fs
        WHERE NOT EXISTS (SELECT 1 FROM pricing.dim_product dp WHERE dp.sku = fs.sku)
            OR NOT EXISTS (SELECT 1 FROM pricing.dim_region dr WHERE dr.region_code = fs.region_code)
            OR NOT EXISTS (SELECT 1 FROM pricing.dim_channel dc WHERE dc.channel_code = fs.channel_code)
    """
    cursor.execute(query_sales)
    sales_count = cursor.fetchone()[0]
    
    # Check fact_price_history orphans
    query_price = """
        SELECT COUNT(*) AS issue_count
        FROM pricing.fact_price_history ph
        WHERE NOT EXISTS (SELECT 1 FROM pricing.dim_product dp WHERE dp.sku = ph.sku)
            OR NOT EXISTS (SELECT 1 FROM pricing.dim_region dr WHERE dr.region_code = ph.region_code)
            OR NOT EXISTS (SELECT 1 FROM pricing.dim_channel dc WHERE dc.channel_code = ph.channel_code)
    """
    cursor.execute(query_price)
    price_count = cursor.fetchone()[0]
    
    total_count = sales_count + price_count
    status = "PASS" if total_count == 0 else "FAIL"
    
    sample_query = """
        SELECT 'fact_sales' AS table_name, sale_id, sku, region_code, channel_code
        FROM pricing.fact_sales fs
        WHERE NOT EXISTS (SELECT 1 FROM pricing.dim_product dp WHERE dp.sku = fs.sku)
            OR NOT EXISTS (SELECT 1 FROM pricing.dim_region dr WHERE dr.region_code = fs.region_code)
            OR NOT EXISTS (SELECT 1 FROM pricing.dim_channel dc WHERE dc.channel_code = fs.channel_code)
        UNION ALL
        SELECT 'fact_price_history', price_hist_id, sku, region_code, channel_code
        FROM pricing.fact_price_history ph
        WHERE NOT EXISTS (SELECT 1 FROM pricing.dim_product dp WHERE dp.sku = ph.sku)
            OR NOT EXISTS (SELECT 1 FROM pricing.dim_region dr WHERE dr.region_code = ph.region_code)
            OR NOT EXISTS (SELECT 1 FROM pricing.dim_channel dc WHERE dc.channel_code = ph.channel_code)
    """
    
    return {
        "name": "Orphan Facts",
        "status": status,
        "count": total_count,
        "sample_query": sample_query,
        "critical": True
    }

def check_overlapping_discounts(cursor):
    """Check 4: Overlapping active discounts (WARNING, not critical)"""
    query = """
        SELECT COUNT(*) AS issue_count
        FROM pricing.fact_discount_events de1
        INNER JOIN pricing.fact_discount_events de2
            ON de1.sku = de2.sku
            AND de1.region_code = de2.region_code
            AND de1.channel_code = de2.channel_code
            AND de1.discount_event_id < de2.discount_event_id
        WHERE de1.start_date <= de2.end_date
            AND de2.start_date <= de1.end_date
    """
    cursor.execute(query)
    count = cursor.fetchone()[0]
    
    status = "WARNING" if count > 0 else "PASS"
    sample_query = """
        SELECT TOP 5 de1.sku, de1.region_code, de1.channel_code,
               de1.start_date AS disc1_start, de1.end_date AS disc1_end,
               de2.start_date AS disc2_start, de2.end_date AS disc2_end
        FROM pricing.fact_discount_events de1
        INNER JOIN pricing.fact_discount_events de2
            ON de1.sku = de2.sku AND de1.region_code = de2.region_code
            AND de1.channel_code = de2.channel_code AND de1.discount_event_id < de2.discount_event_id
        WHERE de1.start_date <= de2.end_date AND de2.start_date <= de1.end_date
    """
    
    return {
        "name": "Overlapping Active Discounts",
        "status": status,
        "count": count,
        "sample_query": sample_query,
        "critical": False
    }

def check_missing_price_coverage(cursor):
    """Check 5: Missing current price coverage for recent sales"""
    query = """
        SELECT COUNT(DISTINCT CONCAT(fs.sku, '|', fs.region_code, '|', fs.channel_code)) AS issue_count
        FROM pricing.fact_sales fs
        WHERE fs.sale_date >= DATEADD(DAY, -30, CAST(GETDATE() AS DATE))
            AND NOT EXISTS (
                SELECT 1 FROM pricing.fact_price_history ph
                WHERE ph.sku = fs.sku
                    AND ph.region_code = fs.region_code
                    AND ph.channel_code = fs.channel_code
                    AND ph.effective_end IS NULL
            )
    """
    cursor.execute(query)
    count = cursor.fetchone()[0]
    
    status = "PASS" if count == 0 else "FAIL"
    sample_query = """
        SELECT DISTINCT TOP 5 fs.sku, fs.region_code, fs.channel_code
        FROM pricing.fact_sales fs
        WHERE fs.sale_date >= DATEADD(DAY, -30, CAST(GETDATE() AS DATE))
            AND NOT EXISTS (
                SELECT 1 FROM pricing.fact_price_history ph
                WHERE ph.sku = fs.sku AND ph.region_code = fs.region_code
                    AND ph.channel_code = fs.channel_code AND ph.effective_end IS NULL
            )
    """
    
    return {
        "name": "Missing Current Price Coverage",
        "status": status,
        "count": count,
        "sample_query": sample_query,
        "critical": True
    }

def run_checks():
    """Execute all data quality checks"""
    try:
        # Load configuration
        config = load_config()
        
        # Connect to database
        print("Connecting to database...")
        conn = connect_to_db(config)
        conn.autocommit = True
        cursor = conn.cursor()
        
        try:
            print("Running data quality checks...\n")
            
            # Run all checks
            checks = [
                check_negative_prices(cursor),
                check_overlapping_ranges(cursor),
                check_orphan_facts(cursor),
                check_overlapping_discounts(cursor),
                check_missing_price_coverage(cursor)
            ]
            
            # Determine overall status
            critical_failed = any(c['status'] == 'FAIL' and c['critical'] for c in checks)
            overall_status = "FAIL" if critical_failed else "PASS"
            
            # Generate report
            report = {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "overall_status": overall_status,
                "checks": [
                    {
                        "name": c["name"],
                        "status": c["status"],
                        "count": c["count"],
                        "sample_query": c["sample_query"]
                    }
                    for c in checks
                ]
            }
            
            # Write JSON report
            report_file = Path(__file__).parent / 'dq_report.json'
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=2)
            
            # Print console summary
            print("=" * 60)
            print("DATA QUALITY REPORT")
            print("=" * 60)
            print(f"Generated: {report['generated_at']}")
            print(f"Overall Status: {overall_status}")
            print("\nCheck Results:")
            print("-" * 60)
            
            for check in checks:
                status_symbol = "[PASS]" if check['status'] == "PASS" else ("[WARN]" if check['status'] == "WARNING" else "[FAIL]")
                critical_marker = " [CRITICAL]" if check['critical'] else ""
                print(f"{status_symbol} {check['name']}: {check['status']} (Count: {check['count']}){critical_marker}")
            
            print("-" * 60)
            print(f"\nFull report saved to: {report_file}")
            
            # Determine exit code
            if critical_failed:
                print("\nCRITICAL CHECKS FAILED - Exiting with code 2")
                return 2
            else:
                print("\nAll critical checks passed")
                return 0
                
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
    sys.exit(run_checks())
