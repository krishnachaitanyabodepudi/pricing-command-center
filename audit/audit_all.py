#!/usr/bin/env python3
"""
End-to-end audit script for pricing-command-center
Verifies all components and generates a comprehensive report
"""

import sys
import os
import json
import subprocess
import pyodbc
import yaml
import requests
from pathlib import Path
from datetime import datetime

class AuditResult:
    def __init__(self):
        self.sections = []
        self.overall_pass = True
    
    def add(self, status, message, details=None):
        """Add a result: PASS, FAIL, or WARN"""
        self.sections.append({
            'status': status,
            'message': message,
            'details': details
        })
        if status == 'FAIL':
            self.overall_pass = False
    
    def print_report(self):
        """Print formatted report"""
        for section in self.sections:
            status = section['status']
            msg = section['message']
            details = section.get('details')
            
            print(f"[{status}] {msg}")
            if details:
                if isinstance(details, str):
                    print(f"  {details}")
                elif isinstance(details, list):
                    for d in details:
                        print(f"  {d}")
                elif isinstance(details, dict):
                    for k, v in details.items():
                        print(f"  {k}: {v}")

def load_config():
    """Load database configuration"""
    config_file = Path(__file__).parent.parent / 'etl' / 'config.yaml'
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

def get_connection_string(config, driver_override=None):
    """Build SQL Server connection string with driver fallback"""
    db = config['database']
    driver = driver_override or db['driver']
    
    # Build base connection string
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={db['server']},{db['port']};"
        f"DATABASE={db['database']};"
        f"UID={db['username']};"
        f"PWD={db['password']};"
    )
    
    # TrustServerCertificate only for newer ODBC drivers
    if 'ODBC Driver' in driver:
        conn_str += "TrustServerCertificate=yes;"
    
    return conn_str

def connect_to_db(config):
    """Connect to database, trying common drivers as fallback"""
    db = config['database']
    driver = db['driver']
    available_drivers = pyodbc.drivers()
    
    # Try the specified driver first
    drivers_to_try = [driver]
    
    # Add common fallback drivers if available
    common_drivers = [
        'ODBC Driver 17 for SQL Server',
        'ODBC Driver 18 for SQL Server',
        'ODBC Driver 13 for SQL Server',
        'SQL Server',
        'SQL Server Native Client 11.0'
    ]
    for fallback in common_drivers:
        if fallback in available_drivers and fallback not in drivers_to_try:
            drivers_to_try.append(fallback)
    
    last_error = None
    for attempt_driver in drivers_to_try:
        try:
            conn_str = get_connection_string(config, attempt_driver)
            conn = pyodbc.connect(conn_str, timeout=10)
            return conn, attempt_driver
        except pyodbc.Error as e:
            last_error = e
            continue
    
    # If all attempts failed, raise the last error
    raise last_error or pyodbc.Error("Could not find a working ODBC driver")

def execute_query(cursor, query, params=None):
    """Execute query and return results"""
    if params:
        cursor.execute(query, params)
    else:
        cursor.execute(query)
    
    columns = [col[0] for col in cursor.description] if cursor.description else []
    rows = cursor.fetchall()
    return [dict(zip(columns, row)) for row in rows] if columns else []

def check_objects_exist(cursor, result):
    """Check required objects exist"""
    required_tables = [
        'dim_product', 'dim_region', 'dim_channel', 'dim_pricing_rule',
        'fact_sales', 'fact_price_history', 'fact_discount_events', 'fact_margin_impact',
        'etl_run_history', 'price_override_audit',
        'stg_sales', 'stg_price_history', 'stg_discount_events'
    ]
    required_sprocs = ['sp_refresh_pricing_mart', 'sp_get_current_price', 'sp_get_price_history']
    required_views = ['vw_sales_daily', 'vw_discount_active', 'vw_etl_latest_run', 'vw_pricing_bi_dataset']
    required_triggers = ['trg_log_price_override']
    
    # Get existing tables
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'pricing'")
    existing_tables = {row[0] for row in cursor.fetchall()}
    
    # Get existing procedures
    cursor.execute("SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = 'pricing' AND ROUTINE_TYPE = 'PROCEDURE'")
    existing_sprocs = {row[0] for row in cursor.fetchall()}
    
    # Get existing views
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = 'pricing'")
    existing_views = {row[0] for row in cursor.fetchall()}
    
    # Get existing triggers
    cursor.execute("SELECT name FROM sys.triggers WHERE OBJECT_SCHEMA_NAME(parent_id) = 'pricing'")
    existing_triggers = {row[0] for row in cursor.fetchall()}
    
    missing_tables = [t for t in required_tables if t not in existing_tables]
    missing_sprocs = [s for s in required_sprocs if s not in existing_sprocs]
    missing_views = [v for v in required_views if v not in existing_views]
    missing_triggers = [t for t in required_triggers if t not in existing_triggers]
    
    all_missing = missing_tables + missing_sprocs + missing_views + missing_triggers
    
    if all_missing:
        details = []
        if missing_tables:
            details.append(f"Missing tables: {', '.join(missing_tables)}")
        if missing_sprocs:
            details.append(f"Missing stored procedures: {', '.join(missing_sprocs)}")
        if missing_views:
            details.append(f"Missing views: {', '.join(missing_views)}")
        if missing_triggers:
            details.append(f"Missing triggers: {', '.join(missing_triggers)}")
        result.add('FAIL', 'Object existence check', details)
    else:
        result.add('PASS', 'Object existence check')

def check_staging_data(cursor, result):
    """Check staging table row counts and bad data"""
    # Row counts
    cursor.execute("""
        SELECT 'stg_sales' AS table_name, COUNT(*) AS row_count FROM pricing.stg_sales
        UNION ALL SELECT 'stg_price_history', COUNT(*) FROM pricing.stg_price_history
        UNION ALL SELECT 'stg_discount_events', COUNT(*) FROM pricing.stg_discount_events
    """)
    counts = {row[0]: row[1] for row in cursor.fetchall()}
    
    details = [f"{tbl}: {counts.get(tbl, 0)} rows" for tbl in ['stg_sales', 'stg_price_history', 'stg_discount_events']]
    
    # Thresholds
    if counts.get('stg_sales', 0) < 5000:
        result.add('FAIL', 'Staging data checks', details + [f"stg_sales threshold failed: {counts.get('stg_sales', 0)} < 5000"])
        return
    if counts.get('stg_price_history', 0) < 500:
        result.add('FAIL', 'Staging data checks', details + [f"stg_price_history threshold failed: {counts.get('stg_price_history', 0)} < 500"])
        return
    if counts.get('stg_discount_events', 0) < 50:
        result.add('FAIL', 'Staging data checks', details + [f"stg_discount_events threshold failed: {counts.get('stg_discount_events', 0)} < 50"])
        return
    
    # Bad data checks
    cursor.execute("SELECT COUNT(*) FROM pricing.stg_price_history WHERE price < 0")
    neg_price_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM pricing.stg_price_history WHERE sku IS NULL OR LTRIM(RTRIM(ISNULL(sku, ''))) = ''")
    missing_sku_count = cursor.fetchone()[0]
    
    cursor.execute("""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT de1.sku, de1.region_code, de1.channel_code, de1.start_date, de1.end_date
            FROM pricing.stg_discount_events de1
            INNER JOIN pricing.stg_discount_events de2
                ON de1.sku = de2.sku AND de1.region_code = de2.region_code
                AND de1.channel_code = de2.channel_code
                AND (de1.start_date != de2.start_date OR de1.end_date != de2.end_date 
                     OR ISNULL(de1.discount_type, '') != ISNULL(de2.discount_type, '')
                     OR de1.discount_value != de2.discount_value)
            WHERE de1.start_date <= de2.end_date AND de2.start_date <= de1.end_date
        ) overlaps
    """)
    overlap_count = cursor.fetchone()[0]
    
    details.extend([
        f"Negative prices: {neg_price_count} (expected >= 10)",
        f"Missing SKUs: {missing_sku_count} (expected >= 10)",
        f"Overlapping discounts: {overlap_count} (expected > 0)"
    ])
    
    if neg_price_count < 10 or missing_sku_count < 10 or overlap_count == 0:
        result.add('FAIL', 'Staging data checks', details)
    else:
        result.add('PASS', 'Staging data checks', details)

def run_etl_check(cursor, result):
    """Run ETL and check status"""
    try:
        cursor.execute("EXEC sp_set_session_context @key = N'is_etl', @value = 1")
        cursor.execute("EXEC pricing.sp_refresh_pricing_mart")
        cursor.fetchall()  # Clear results
        
        # Get latest run
        cursor.execute("""
            SELECT TOP 1 run_id, status, rows_loaded, rows_rejected, failure_reason
            FROM pricing.etl_run_history
            WHERE pipeline_name = 'pricing_refresh'
            ORDER BY run_id DESC
        """)
        run = cursor.fetchone()
        
        if run:
            run_id, status, rows_loaded, rows_rejected, failure_reason = run
            details = [
                f"Run ID: {run_id}",
                f"Status: {status}",
                f"Rows Loaded: {rows_loaded}",
                f"Rows Rejected: {rows_rejected}"
            ]
            if failure_reason:
                details.append(f"Failure Reason: {failure_reason}")
            
            if status == 'SUCCESS':
                result.add('PASS', 'ETL run', details)
            else:
                result.add('FAIL', 'ETL run', details)
        else:
            result.add('FAIL', 'ETL run', ['No ETL run found'])
    except Exception as e:
        result.add('FAIL', 'ETL run', [f"Error: {str(e)}"])

def check_facts(cursor, result):
    """Check fact table data quality"""
    # Row counts
    cursor.execute("""
        SELECT 'fact_sales' AS table_name, COUNT(*) AS row_count FROM pricing.fact_sales
        UNION ALL SELECT 'fact_price_history', COUNT(*) FROM pricing.fact_price_history
        UNION ALL SELECT 'fact_discount_events', COUNT(*) FROM pricing.fact_discount_events
    """)
    counts = {row[0]: row[1] for row in cursor.fetchall()}
    
    details = [f"{tbl}: {counts.get(tbl, 0)} rows" for tbl in ['fact_sales', 'fact_price_history', 'fact_discount_events']]
    
    # Negative prices check
    cursor.execute("SELECT COUNT(*) FROM pricing.fact_price_history WHERE price < 0")
    neg_count = cursor.fetchone()[0]
    
    if neg_count > 0:
        result.add('FAIL', 'Fact sanity checks', details + [f"Negative prices found: {neg_count} (expected 0)"])
        return
    
    # Overlap check
    cursor.execute("""
        WITH PriceRanges AS (
            SELECT sku, region_code, channel_code, effective_start,
                   ISNULL(effective_end, '9999-12-31') AS effective_end, price_hist_id
            FROM pricing.fact_price_history
        )
        SELECT COUNT(*) AS overlap_count
        FROM PriceRanges pr1
        INNER JOIN PriceRanges pr2
            ON pr1.sku = pr2.sku AND pr1.region_code = pr2.region_code
            AND pr1.channel_code = pr2.channel_code AND pr1.price_hist_id < pr2.price_hist_id
        WHERE pr1.effective_start < pr2.effective_end AND pr2.effective_start < pr1.effective_end
    """)
    overlap_count = cursor.fetchone()[0]
    
    details.append(f"Effective date overlaps: {overlap_count} (expected 0)")
    
    if overlap_count > 0:
        result.add('FAIL', 'Fact sanity checks', details)
    else:
        result.add('PASS', 'Fact sanity checks', details)

def run_dq_check(result):
    """Run DQ script and parse results"""
    dq_script = Path(__file__).parent.parent / 'dq' / 'checks.py'
    dq_report = Path(__file__).parent.parent / 'dq' / 'dq_report.json'
    
    try:
        # Run DQ script
        proc = subprocess.run(
            [sys.executable, str(dq_script)],
            capture_output=True,
            text=True,
            cwd=str(dq_script.parent)
        )
        
        if not dq_report.exists():
            result.add('FAIL', 'DQ script run', ['dq_report.json not found'])
            return
        
        # Parse JSON
        with open(dq_report, 'r') as f:
            dq_data = json.load(f)
        
        required_keys = ['generated_at', 'overall_status', 'checks']
        if not all(k in dq_data for k in required_keys):
            result.add('FAIL', 'DQ script run', ['Missing required keys in report'])
            return
        
        overall_status = dq_data['overall_status']
        checks = dq_data['checks']
        
        failed_checks = [c for c in checks if c['status'] in ['FAIL', 'WARNING']]
        details = [f"Overall Status: {overall_status}"]
        if failed_checks:
            for check in failed_checks:
                details.append(f"{check['name']}: {check['status']} (count: {check['count']})")
        
        if overall_status != 'PASS':
            result.add('FAIL', 'DQ script run', details)
        else:
            result.add('PASS', 'DQ script run', details)
            
    except Exception as e:
        result.add('FAIL', 'DQ script run', [f"Error: {str(e)}"])

def test_api(api_url, cursor, result):
    """Test API endpoints"""
    if not api_url:
        result.add('WARN', 'API tests', ['Skipped (API_URL not set)'])
        return
    
    endpoints = [
        ('/health', 'GET', None),
        ('/etl/runs', 'GET', None),
        ('/dq/latest', 'GET', None),
    ]
    
    # Get test data for pricing endpoints
    try:
        cursor.execute("SELECT TOP 1 sku, region_code, channel_code FROM pricing.fact_price_history WHERE effective_end IS NULL")
        test_row = cursor.fetchone()
        if test_row:
            sku, region, channel = test_row
            from datetime import date, timedelta
            today = date.today()
            thirty_days_ago = today - timedelta(days=30)
            endpoints.extend([
                (f'/pricing/current?sku={sku}&region_code={region}&channel_code={channel}', 'GET', None),
                (f'/pricing/history?sku={sku}&from_date={thirty_days_ago}&to_date={today}', 'GET', None),
                (f'/pricing/bi-snapshot?as_of_date={today}&region_code={region}&channel_code={channel}&limit=5', 'GET', None),
            ])
    except:
        pass
    
    details = []
    all_pass = True
    
    for endpoint, method, _ in endpoints:
        try:
            url = f"{api_url.rstrip('/')}{endpoint}"
            resp = requests.get(url, timeout=5)
            status_code = resp.status_code
            payload_size = len(resp.content)
            details.append(f"{endpoint}: {status_code} ({payload_size} bytes)")
            if status_code != 200:
                all_pass = False
        except Exception as e:
            details.append(f"{endpoint}: Error - {str(e)}")
            all_pass = False
    
    if all_pass:
        result.add('PASS', 'API tests', details)
    else:
        result.add('FAIL', 'API tests', details)

def check_performance_artifacts(result):
    """Check performance proof files exist"""
    perf_dir = Path(__file__).parent.parent / 'performance_proofs'
    index_file = Path(__file__).parent.parent / 'sql' / 'indexes' / 'index_changes.sql'
    
    required_files = [
        perf_dir / 'baseline_query.sql',
        perf_dir / 'optimized_query.sql',
        perf_dir / 'before_after_timings.md',
        index_file,
    ]
    optional_file = perf_dir / 'verify_results.sql'
    
    missing = []
    for f in required_files:
        if not f.exists():
            missing.append(str(f.relative_to(Path(__file__).parent.parent)))
    
    if optional_file.exists():
        details = [f"All required files present", f"verify_results.sql: present"]
    else:
        details = [f"All required files present", f"verify_results.sql: missing (WARN - suggested to generate)"]
    
    if missing:
        result.add('FAIL', 'Performance artifacts', missing)
    else:
        result.add('PASS', 'Performance artifacts', details)

def main():
    result = AuditResult()
    
    # Environment summary
    print("=" * 60)
    print("PRICING COMMAND CENTER - END-TO-END AUDIT")
    print("=" * 60)
    print(f"Python Version: {sys.version}")
    print(f"OS: {os.name}")
    
    try:
        config = load_config()
        db_config = config['database']
        print(f"SQL Server: {db_config['server']}:{db_config['port']}")
        print(f"Database: {db_config['database']}")
    except Exception as e:
        print(f"Config Error: {e}")
        result.add('FAIL', 'Config loading', [str(e)])
        result.print_report()
        print("\nOVERALL: FAIL")
        sys.exit(2)
    
    api_url = os.environ.get('API_URL')
    if api_url:
        print(f"API URL: {api_url}")
    else:
        print("API URL: Not set (API tests will be skipped)")
    print("=" * 60)
    print()
    
    # DB connectivity
    try:
        conn, used_driver = connect_to_db(config)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("SELECT @@SERVERNAME, DB_NAME()")
        server, db = cursor.fetchone()
        result.add('PASS', 'DB connectivity', [f"Connected to {server}/{db} (driver: {used_driver})"])
    except Exception as e:
        result.add('FAIL', 'DB connectivity', [str(e)])
        result.print_report()
        print("\nOVERALL: FAIL")
        sys.exit(2)
    
    try:
        # Object existence
        check_objects_exist(cursor, result)
        
        # Staging checks
        check_staging_data(cursor, result)
        
        # Run ETL
        run_etl_check(cursor, result)
        
        # Fact checks
        check_facts(cursor, result)
        
        # DQ check
        run_dq_check(result)
        
        # API tests
        test_api(api_url, cursor, result)
        
        # Performance artifacts
        check_performance_artifacts(result)
        
    finally:
        cursor.close()
        conn.close()
    
    # Print report
    print()
    result.print_report()
    print()
    print("=" * 60)
    if result.overall_pass:
        print("OVERALL: PASS")
        sys.exit(0)
    else:
        print("OVERALL: FAIL")
        sys.exit(2)

if __name__ == '__main__':
    main()

