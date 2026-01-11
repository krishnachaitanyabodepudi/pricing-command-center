USE PricingDWH;
GO

-- Baseline query - intentionally non-optimized for performance comparison
-- Parameters
DECLARE @from_date DATE = DATEADD(DAY, -30, CAST(GETDATE() AS DATE));
DECLARE @to_date DATE = CAST(GETDATE() AS DATE);
DECLARE @region_code VARCHAR(10) = 'MW';
DECLARE @channel_code VARCHAR(10) = 'RETAIL';

SET STATISTICS IO ON;
SET STATISTICS TIME ON;

-- Baseline query: Uses broad joins (row explosion) and non-sargable filters
-- This query is intentionally designed to be slower for comparison
SELECT 
    p.sku,
    SUM(fs.qty) AS total_qty_30d,
    SUM(fs.net_sales) AS total_sales_30d,
    MAX(ph.price) AS current_price,
    MAX(de.discount_value) AS active_discount_value
FROM pricing.dim_product p
INNER JOIN pricing.fact_sales fs
    ON p.sku = fs.sku
LEFT JOIN pricing.fact_price_history ph
    ON p.sku = ph.sku
    AND fs.region_code = ph.region_code
    AND fs.channel_code = ph.channel_code
LEFT JOIN pricing.fact_discount_events de
    ON p.sku = de.sku
    AND fs.region_code = de.region_code
    AND fs.channel_code = de.channel_code
WHERE CAST(fs.sale_date AS VARCHAR(10)) BETWEEN CAST(@from_date AS VARCHAR(10)) AND CAST(@to_date AS VARCHAR(10))
    AND fs.region_code = @region_code
    AND fs.channel_code = @channel_code
    -- Date filtering in WHERE causes row explosion (should be in JOIN)
    AND (ph.effective_end IS NULL OR CAST(ph.effective_end AS VARCHAR(10)) >= CAST(GETDATE() AS VARCHAR(10)))
    AND CAST(ph.effective_start AS VARCHAR(10)) <= CAST(GETDATE() AS VARCHAR(10))
    AND CAST(de.start_date AS VARCHAR(10)) <= CAST(GETDATE() AS VARCHAR(10))
    AND CAST(de.end_date AS VARCHAR(10)) >= CAST(GETDATE() AS VARCHAR(10))
GROUP BY p.sku;

SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;

-- CAPTURE RESULTS:
-- 1. Check Messages tab for STATISTICS IO output (logical reads)
-- 2. Check Messages tab for STATISTICS TIME output (CPU time, elapsed time)
-- 3. Run query TWICE - use SECOND run results (first run warms cache)
GO
