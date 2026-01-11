USE PricingDWH;
GO

-- Optimized query - tuned for performance using sargable predicates and efficient patterns
-- Parameters
DECLARE @from_date DATE = DATEADD(DAY, -30, CAST(GETDATE() AS DATE));
DECLARE @to_date DATE = CAST(GETDATE() AS DATE);
DECLARE @region_code VARCHAR(10) = 'MW';
DECLARE @channel_code VARCHAR(10) = 'RETAIL';
DECLARE @as_of_date DATE = CAST(GETDATE() AS DATE);

SET STATISTICS IO ON;
SET STATISTICS TIME ON;

-- Optimized query: Uses sargable predicates, pre-aggregation, and efficient lookups
WITH SalesAgg AS (
    -- Pre-aggregate sales data with sargable date filter
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
    -- Get current price using sargable date predicates
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
    -- Get active discount using sargable date predicates
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

SET STATISTICS IO OFF;
SET STATISTICS TIME OFF;

-- CAPTURE RESULTS:
-- 1. Check Messages tab for STATISTICS IO output (logical reads)
-- 2. Check Messages tab for STATISTICS TIME output (CPU time, elapsed time)
-- 3. Run query TWICE - use SECOND run results (first run warms cache)
GO
