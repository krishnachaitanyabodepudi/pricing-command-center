USE PricingDWH;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_WARNINGS ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET ARITHABORT ON;
GO

CREATE OR ALTER VIEW pricing.vw_pricing_bi_dataset
AS
WITH DateSeries AS (
    SELECT CAST(DATEADD(DAY, -n, CAST(GETDATE() AS DATE)) AS DATE) AS as_of_date
    FROM (
        SELECT TOP 60 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS n
        FROM sys.objects
    ) numbers
),
RecentActivityKeys AS (
    SELECT DISTINCT sku, region_code, channel_code
    FROM pricing.fact_sales
    WHERE sale_date >= DATEADD(DAY, -60, CAST(GETDATE() AS DATE))
    UNION
    SELECT DISTINCT sku, region_code, channel_code
    FROM pricing.fact_price_history
    WHERE effective_start <= CAST(GETDATE() AS DATE)
        AND (effective_end IS NULL OR effective_end >= DATEADD(DAY, -60, CAST(GETDATE() AS DATE)))
    UNION
    SELECT DISTINCT sku, region_code, channel_code
    FROM pricing.fact_discount_events
    WHERE start_date <= CAST(GETDATE() AS DATE)
        AND end_date >= DATEADD(DAY, -60, CAST(GETDATE() AS DATE))
),
ProductRegionChannel AS (
    SELECT DISTINCT
        rak.sku,
        dp.product_name,
        dp.category,
        dp.brand,
        rak.region_code,
        dr.region_name,
        rak.channel_code,
        dc.channel_name
    FROM RecentActivityKeys rak
    INNER JOIN pricing.dim_product dp ON rak.sku = dp.sku
    INNER JOIN pricing.dim_region dr ON rak.region_code = dr.region_code
    INNER JOIN pricing.dim_channel dc ON rak.channel_code = dc.channel_code
    WHERE dp.is_active = 1
),
SalesDaily AS (
    SELECT 
        sale_date AS as_of_date,
        sku,
        region_code,
        channel_code,
        daily_sales_qty,
        daily_net_sales
    FROM pricing.vw_sales_daily
),
PriceAsOf AS (
    SELECT 
        ds.as_of_date,
        ph.sku,
        ph.region_code,
        ph.channel_code,
        ph.price AS current_price,
        ph.currency,
        ROW_NUMBER() OVER (
            PARTITION BY ds.as_of_date, ph.sku, ph.region_code, ph.channel_code
            ORDER BY ph.effective_start DESC, ph.created_at DESC, ph.price_hist_id DESC
        ) AS rn
    FROM DateSeries ds
    INNER JOIN pricing.fact_price_history ph
        ON ph.effective_start <= ds.as_of_date
        AND (ph.effective_end IS NULL OR ph.effective_end >= ds.as_of_date)
),
PriceAsOfTop AS (
    SELECT 
        as_of_date,
        sku,
        region_code,
        channel_code,
        current_price,
        currency
    FROM PriceAsOf
    WHERE rn = 1
),
DiscountAsOf AS (
    SELECT 
        de.sku,
        de.region_code,
        de.channel_code,
        ds.as_of_date,
        de.discount_type AS active_discount_type,
        de.discount_value AS active_discount_value,
        ROW_NUMBER() OVER (
            PARTITION BY de.sku, de.region_code, de.channel_code, ds.as_of_date
            ORDER BY de.discount_value DESC, de.start_date ASC
        ) AS rn
    FROM DateSeries ds
    INNER JOIN pricing.fact_discount_events de
        ON de.start_date <= ds.as_of_date
        AND de.end_date >= ds.as_of_date
),
DiscountAsOfTop AS (
    SELECT 
        sku,
        region_code,
        channel_code,
        as_of_date,
        active_discount_type,
        active_discount_value
    FROM DiscountAsOf
    WHERE rn = 1
)
SELECT 
    ds.as_of_date,
    prc.sku,
    prc.product_name,
    prc.category,
    prc.brand,
    prc.region_code,
    prc.region_name,
    prc.channel_code,
    prc.channel_name,
    ISNULL(pa.current_price, 0) AS current_price,
    ISNULL(pa.currency, 'USD') AS currency,
    da.active_discount_type,
    da.active_discount_value,
    ISNULL(sd.daily_sales_qty, 0) AS daily_sales_qty,
    ISNULL(sd.daily_net_sales, 0) AS daily_net_sales,
    CASE WHEN pa.current_price IS NULL THEN 1 ELSE 0 END AS dq_missing_price_flag
FROM DateSeries ds
CROSS JOIN ProductRegionChannel prc
LEFT JOIN PriceAsOfTop pa
    ON pa.as_of_date = ds.as_of_date
    AND pa.sku = prc.sku
    AND pa.region_code = prc.region_code
    AND pa.channel_code = prc.channel_code
LEFT JOIN DiscountAsOfTop da
    ON da.as_of_date = ds.as_of_date
    AND da.sku = prc.sku
    AND da.region_code = prc.region_code
    AND da.channel_code = prc.channel_code
LEFT JOIN SalesDaily sd
    ON sd.as_of_date = ds.as_of_date
    AND sd.sku = prc.sku
    AND sd.region_code = prc.region_code
    AND sd.channel_code = prc.channel_code;
GO
