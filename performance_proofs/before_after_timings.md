# Performance Improvement Documentation

## Environment

- **SQL Server Version**: [e.g., SQL Server 2022 Developer Edition]
- **Database**: PricingDWH
- **Test Date**: [Date]

## Measurement Methodology

**IMPORTANT**: Always run each query **TWICE** and use the **SECOND run** results. The first run warms the buffer cache, making the second run more representative of actual performance.

### Enabling Statistics and Execution Plans

1. **Enable Actual Execution Plan**:
   - In SSMS: Query menu → Include Actual Execution Plan (or Ctrl+M)
   - Save execution plan screenshots for baseline and optimized queries

2. **STATISTICS IO/TIME**:
   - Enabled in query scripts via `SET STATISTICS IO ON;` and `SET STATISTICS TIME ON;`
   - Results appear in the Messages tab

## Baseline Performance

**Query**: `baseline_query.sql`

**Parameters Used**:
- @from_date: [Date, e.g., 2025-01-15]
- @to_date: [Date, e.g., 2025-02-15]
- @region_code: [e.g., 'MW']
- @channel_code: [e.g., 'RETAIL']

**Second Run Results**:
- **Elapsed Time**: [e.g., 2450 ms]
- **CPU Time**: [e.g., 2150 ms]
- **Logical Reads**: [e.g., 15,234]

**Execution Plan Screenshot**: `baseline_execution_plan.png`

## Indexes Added

Created via `sql/indexes/index_changes.sql`:

1. **IX_fact_sales_perf_date_region_channel**
   - Columns: (sale_date, region_code, channel_code, sku)
   - Included: (qty, net_sales)

2. **IX_fact_discount_events_perf_region_channel_sku_dates**
   - Columns: (region_code, channel_code, sku, start_date, end_date)
   - Included: (discount_value, discount_type)

3. **IX_fact_price_history_perf_region_channel_sku_effective**
   - Columns: (region_code, channel_code, sku, effective_start, effective_end)
   - Included: (price, currency, created_at)

## Optimized Performance

**Query**: `optimized_query.sql`

**Parameters Used**: (Same as baseline)
- @from_date: [Date, e.g., 2025-01-15]
- @to_date: [Date, e.g., 2025-02-15]
- @region_code: [e.g., 'MW']
- @channel_code: [e.g., 'RETAIL']

**Second Run Results**:
- **Elapsed Time**: [e.g., 125 ms]
- **CPU Time**: [e.g., 98 ms]
- **Logical Reads**: [e.g., 1,456]

**Execution Plan Screenshot**: `optimized_execution_plan.png`

## Performance Summary

**Improvement**:
- **Elapsed Time**: [X]% faster ([baseline] ms → [optimized] ms)
- **CPU Time**: [X]% reduction ([baseline] ms → [optimized] ms)
- **Logical Reads**: [X]% reduction ([baseline] → [optimized])

### Why It Improved

The optimized query improved performance through several key changes: (1) Replaced non-sargable VARCHAR casts with direct DATE comparisons, enabling index seek operations instead of scans. (2) Pre-aggregated sales data in a CTE to reduce join complexity and leverage the covering index on fact_sales. (3) Used window functions (ROW_NUMBER) with proper ordering instead of multiple LEFT JOINs with complex WHERE conditions, allowing SQL Server to efficiently select the most recent price and highest discount. (4) The new indexes provide covering index support, eliminating key lookups by including frequently accessed columns (qty, net_sales, price, discount_value) in the index leaf pages. Together, these changes transformed table scans into index seeks and reduced I/O by [X]%, resulting in [X]x faster query execution.

## Verification

**Result Set Comparison**: Both queries return identical results (same columns, same grain: one row per SKU).

**Row Count**: [e.g., 45 rows]
