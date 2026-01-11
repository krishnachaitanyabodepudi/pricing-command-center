"""
FastAPI service for pricing data warehouse
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional, List
from datetime import datetime
import json
from pathlib import Path

from db import fetch_one, fetch_all

app = FastAPI(title="Pricing Command Center API", version="1.0.0")

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "ok"}

@app.get("/pricing/current")
async def get_current_price(
    sku: str = Query(..., description="Product SKU"),
    region_code: str = Query(..., description="Region code"),
    channel_code: str = Query(..., description="Channel code")
):
    """
    Get current price for a product in a region/channel
    Calls stored procedure: pricing.sp_get_current_price
    """
    try:
        query = "EXEC pricing.sp_get_current_price ?, ?, ?"
        result = fetch_one(query, (sku, region_code, channel_code))
        
        if not result:
            raise HTTPException(
                status_code=404,
                detail=f"Current price not found for SKU={sku}, region={region_code}, channel={channel_code}"
            )
        
        return result
    except Exception as e:
        # Handle stored procedure not found or other DB errors
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/pricing/history")
async def get_price_history(
    sku: str = Query(..., description="Product SKU"),
    from_date: str = Query(..., description="Start date (YYYY-MM-DD)"),
    to_date: str = Query(..., description="End date (YYYY-MM-DD)"),
    region_code: Optional[str] = Query(None, description="Region code (optional)"),
    channel_code: Optional[str] = Query(None, description="Channel code (optional)")
):
    """
    Get price history for a product over a date range
    Calls stored procedure: pricing.sp_get_price_history
    """
    # Validate date formats
    try:
        datetime.strptime(from_date, '%Y-%m-%d')
        datetime.strptime(to_date, '%Y-%m-%d')
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid date format. Use YYYY-MM-DD format for from_date and to_date"
        )
    
    try:
        # Call stored procedure with NULLs for optional params
        query = "EXEC pricing.sp_get_price_history ?, ?, ?, ?, ?"
        params = (sku, from_date, to_date, region_code, channel_code)
        results = fetch_all(query, params)
        
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/etl/runs")
async def get_etl_runs():
    """
    Get last 50 ETL runs for pricing_refresh pipeline
    """
    try:
        query = """
            SELECT TOP 50
                run_id,
                pipeline_name,
                started_at,
                finished_at,
                status,
                rows_loaded,
                rows_rejected,
                failure_reason
            FROM pricing.etl_run_history
            WHERE pipeline_name = 'pricing_refresh'
            ORDER BY run_id DESC
        """
        results = fetch_all(query)
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/pricing/bi-snapshot")
async def get_bi_snapshot(
    as_of_date: str = Query(..., description="Date (YYYY-MM-DD)"),
    region_code: str = Query(..., description="Region code"),
    channel_code: str = Query(..., description="Channel code"),
    limit: int = Query(100, ge=1, le=500, description="Maximum number of rows to return")
):
    """
    Get BI snapshot from pricing.vw_pricing_bi_dataset for a specific date/region/channel
    Returns top products by daily_net_sales
    """
    # Validate date format
    try:
        datetime.strptime(as_of_date, '%Y-%m-%d')
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail="Invalid date format. Use YYYY-MM-DD format for as_of_date"
        )
    
    try:
        # Use OFFSET/FETCH for limit (works with parameters)
        query = """
            SELECT
                as_of_date,
                sku,
                product_name,
                category,
                brand,
                region_code,
                region_name,
                channel_code,
                channel_name,
                current_price,
                currency,
                active_discount_type,
                active_discount_value,
                daily_sales_qty,
                daily_net_sales,
                dq_missing_price_flag
            FROM pricing.vw_pricing_bi_dataset
            WHERE as_of_date = ?
                AND region_code = ?
                AND channel_code = ?
            ORDER BY daily_net_sales DESC
            OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY
        """
        results = fetch_all(query, (as_of_date, region_code, channel_code, limit))
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/dq/latest")
async def get_dq_latest():
    """
    Get latest data quality report from JSON file
    """
    report_file = Path(__file__).parent.parent / 'dq' / 'dq_report.json'
    
    if not report_file.exists():
        raise HTTPException(
            status_code=404,
            detail="Data quality report not found. Run dq/checks.py to generate report."
        )
    
    try:
        with open(report_file, 'r') as f:
            report = json.load(f)
        return report
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=500, detail=f"Invalid JSON in report file: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading report: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
