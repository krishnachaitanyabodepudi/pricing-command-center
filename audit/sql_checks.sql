-- SQL queries used by audit script

-- Object existence check for tables
SELECT TABLE_SCHEMA, TABLE_NAME 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'pricing';

-- Object existence check for stored procedures
SELECT ROUTINE_SCHEMA, ROUTINE_NAME 
FROM INFORMATION_SCHEMA.ROUTINES 
WHERE ROUTINE_SCHEMA = 'pricing' AND ROUTINE_TYPE = 'PROCEDURE';

-- Object existence check for views
SELECT TABLE_SCHEMA, TABLE_NAME 
FROM INFORMATION_SCHEMA.VIEWS 
WHERE TABLE_SCHEMA = 'pricing';

-- Object existence check for triggers
SELECT OBJECT_SCHEMA_NAME(parent_id) AS trigger_schema, name AS trigger_name
FROM sys.triggers
WHERE OBJECT_SCHEMA_NAME(parent_id) = 'pricing';

-- Row counts for staging tables
SELECT 'stg_sales' AS table_name, COUNT(*) AS row_count FROM pricing.stg_sales
UNION ALL SELECT 'stg_price_history', COUNT(*) FROM pricing.stg_price_history
UNION ALL SELECT 'stg_discount_events', COUNT(*) FROM pricing.stg_discount_events;

-- Intentional bad data checks
SELECT 'negative_price' AS check_type, COUNT(*) AS count FROM pricing.stg_price_history WHERE price < 0
UNION ALL SELECT 'missing_sku', COUNT(*) FROM pricing.stg_price_history WHERE sku IS NULL OR LTRIM(RTRIM(ISNULL(sku, ''))) = ''
UNION ALL SELECT 'overlapping_discounts', COUNT(*) FROM (
    SELECT DISTINCT de1.sku, de1.region_code, de1.channel_code, de1.start_date, de1.end_date
    FROM pricing.stg_discount_events de1
    INNER JOIN pricing.stg_discount_events de2
        ON de1.sku = de2.sku AND de1.region_code = de2.region_code
        AND de1.channel_code = de2.channel_code
        AND (de1.start_date != de2.start_date OR de1.end_date != de2.end_date 
             OR ISNULL(de1.discount_type, '') != ISNULL(de2.discount_type, '')
             OR de1.discount_value != de2.discount_value)
    WHERE de1.start_date <= de2.end_date AND de2.start_date <= de1.end_date
) overlaps;

-- Fact table row counts
SELECT 'fact_sales' AS table_name, COUNT(*) AS row_count FROM pricing.fact_sales
UNION ALL SELECT 'fact_price_history', COUNT(*) FROM pricing.fact_price_history
UNION ALL SELECT 'fact_discount_events', COUNT(*) FROM pricing.fact_discount_events;

-- Negative prices in facts (should be 0)
SELECT COUNT(*) AS negative_price_count FROM pricing.fact_price_history WHERE price < 0;

-- Effective date overlap detection
WITH PriceRanges AS (
    SELECT 
        sku, region_code, channel_code,
        effective_start,
        ISNULL(effective_end, '9999-12-31') AS effective_end,
        price_hist_id
    FROM pricing.fact_price_history
)
SELECT COUNT(*) AS overlap_count
FROM PriceRanges pr1
INNER JOIN PriceRanges pr2
    ON pr1.sku = pr2.sku
    AND pr1.region_code = pr2.region_code
    AND pr1.channel_code = pr2.channel_code
    AND pr1.price_hist_id < pr2.price_hist_id
WHERE pr1.effective_start < pr2.effective_end
    AND pr2.effective_start < pr1.effective_end;

-- Missing current price coverage (sales last 30 days with no current price)
SELECT COUNT(DISTINCT CONCAT(fs.sku, '|', fs.region_code, '|', fs.channel_code)) AS missing_price_count
FROM pricing.fact_sales fs
WHERE fs.sale_date >= DATEADD(DAY, -30, CAST(GETDATE() AS DATE))
    AND NOT EXISTS (
        SELECT 1 FROM pricing.fact_price_history ph
        WHERE ph.sku = fs.sku
            AND ph.region_code = fs.region_code
            AND ph.channel_code = fs.channel_code
            AND ph.effective_end IS NULL
    );
