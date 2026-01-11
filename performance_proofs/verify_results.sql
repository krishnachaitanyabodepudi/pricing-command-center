USE PricingDWH;
GO

-- Verification script to compare baseline and optimized query results
-- Parameters (must match baseline_query.sql and optimized_query.sql)
DECLARE @from_date DATE = DATEADD(DAY, -30, CAST(GETDATE() AS DATE));
DECLARE @to_date DATE = CAST(GETDATE() AS DATE);
DECLARE @region_code VARCHAR(10) = 'MW';
DECLARE @channel_code VARCHAR(10) = 'RETAIL';
DECLARE @as_of_date DATE = CAST(GETDATE() AS DATE);

-- Create temp tables
IF OBJECT_ID('tempdb..#baseline') IS NOT NULL DROP TABLE #baseline;
IF OBJECT_ID('tempdb..#optimized') IS NOT NULL DROP TABLE #optimized;

CREATE TABLE #baseline (
    sku VARCHAR(255),
    total_qty_30d INT,
    total_sales_30d DECIMAL(18,2),
    current_price DECIMAL(18,4),
    active_discount_value DECIMAL(18,4)
);

CREATE TABLE #optimized (
    sku VARCHAR(255),
    total_qty_30d INT,
    total_sales_30d DECIMAL(18,2),
    current_price DECIMAL(18,4),
    active_discount_value DECIMAL(18,4)
);

-- Run baseline query (matches baseline_query.sql exactly)
INSERT INTO #baseline (sku, total_qty_30d, total_sales_30d, current_price, active_discount_value)
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
    AND (ph.effective_end IS NULL OR CAST(ph.effective_end AS VARCHAR(10)) >= CAST(GETDATE() AS VARCHAR(10)))
    AND CAST(ph.effective_start AS VARCHAR(10)) <= CAST(GETDATE() AS VARCHAR(10))
    AND CAST(de.start_date AS VARCHAR(10)) <= CAST(GETDATE() AS VARCHAR(10))
    AND CAST(de.end_date AS VARCHAR(10)) >= CAST(GETDATE() AS VARCHAR(10))
GROUP BY p.sku;

-- Run optimized query
INSERT INTO #optimized (sku, total_qty_30d, total_sales_30d, current_price, active_discount_value)
WITH SalesAgg AS (
    SELECT 
        sku,
        SUM(qty) AS total_qty_30d,
        SUM(net_sales) AS total_sales_30d
    FROM pricing.fact_sales
    WHERE sale_date >= @from_date
        AND sale_date <= @to_date
        AND region_code = @region_code
        AND channel_code = @channel_code
    GROUP BY sku
),
CurrentPrice AS (
    SELECT DISTINCT
        ph1.sku,
        ph1.price AS current_price,
        ROW_NUMBER() OVER (
            PARTITION BY ph1.sku 
            ORDER BY ph1.effective_start DESC, ph1.created_at DESC, ph1.price_hist_id DESC
        ) AS rn
    FROM pricing.fact_price_history ph1
    WHERE ph1.region_code = @region_code
        AND ph1.channel_code = @channel_code
        AND ph1.effective_start <= @as_of_date
        AND (ph1.effective_end IS NULL OR ph1.effective_end >= @as_of_date)
),
CurrentPriceTop AS (
    SELECT sku, current_price
    FROM CurrentPrice
    WHERE rn = 1
),
ActiveDiscount AS (
    SELECT 
        de1.sku,
        de1.discount_value AS active_discount_value,
        ROW_NUMBER() OVER (
            PARTITION BY de1.sku 
            ORDER BY de1.discount_value DESC, de1.start_date ASC
        ) AS rn
    FROM pricing.fact_discount_events de1
    WHERE de1.region_code = @region_code
        AND de1.channel_code = @channel_code
        AND de1.start_date <= @as_of_date
        AND de1.end_date >= @as_of_date
),
ActiveDiscountTop AS (
    SELECT sku, active_discount_value
    FROM ActiveDiscount
    WHERE rn = 1
)
SELECT 
    sa.sku,
    sa.total_qty_30d,
    sa.total_sales_30d,
    cp.current_price,
    ad.active_discount_value
FROM SalesAgg sa
LEFT JOIN CurrentPriceTop cp ON sa.sku = cp.sku
LEFT JOIN ActiveDiscountTop ad ON sa.sku = ad.sku;

-- Compare results
DECLARE @baseline_count INT;
DECLARE @optimized_count INT;
DECLARE @diff_baseline INT;
DECLARE @diff_optimized INT;

SELECT @baseline_count = COUNT(*) FROM #baseline;
SELECT @optimized_count = COUNT(*) FROM #optimized;

-- Find rows in baseline but not in optimized
SELECT @diff_baseline = COUNT(*)
FROM (
    SELECT sku, total_qty_30d, total_sales_30d, current_price, active_discount_value
    FROM #baseline
    EXCEPT
    SELECT sku, total_qty_30d, total_sales_30d, current_price, active_discount_value
    FROM #optimized
) diff;

-- Find rows in optimized but not in baseline
SELECT @diff_optimized = COUNT(*)
FROM (
    SELECT sku, total_qty_30d, total_sales_30d, current_price, active_discount_value
    FROM #optimized
    EXCEPT
    SELECT sku, total_qty_30d, total_sales_30d, current_price, active_discount_value
    FROM #baseline
) diff;

-- Output results
PRINT '=== Verification Results ===';
PRINT '';
PRINT 'Baseline row count: ' + CAST(@baseline_count AS VARCHAR(10));
PRINT 'Optimized row count: ' + CAST(@optimized_count AS VARCHAR(10));
PRINT '';

IF @diff_baseline = 0 AND @diff_optimized = 0 AND @baseline_count = @optimized_count
BEGIN
    PRINT 'RESULT: PASS';
    PRINT 'Both queries return identical results.';
END
ELSE
BEGIN
    PRINT 'RESULT: FAIL';
    PRINT '';
    PRINT 'Differences found:';
    PRINT 'Rows in baseline but not in optimized: ' + CAST(@diff_baseline AS VARCHAR(10));
    PRINT 'Rows in optimized but not in baseline: ' + CAST(@diff_optimized AS VARCHAR(10));
    PRINT '';
    
    IF @diff_baseline > 0
    BEGIN
        PRINT 'Sample rows in baseline but not in optimized (TOP 5):';
        SELECT TOP 5 *
        FROM (
            SELECT sku, total_qty_30d, total_sales_30d, current_price, active_discount_value
            FROM #baseline
            EXCEPT
            SELECT sku, total_qty_30d, total_sales_30d, current_price, active_discount_value
            FROM #optimized
        ) diff;
    END
    
    IF @diff_optimized > 0
    BEGIN
        PRINT 'Sample rows in optimized but not in baseline (TOP 5):';
        SELECT TOP 5 *
        FROM (
            SELECT sku, total_qty_30d, total_sales_30d, current_price, active_discount_value
            FROM #optimized
            EXCEPT
            SELECT sku, total_qty_30d, total_sales_30d, current_price, active_discount_value
            FROM #baseline
        ) diff;
    END
END

-- Cleanup
DROP TABLE #baseline;
DROP TABLE #optimized;
GO

