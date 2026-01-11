USE PricingDWH;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_WARNINGS ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET ARITHABORT ON;
GO

CREATE OR ALTER VIEW pricing.vw_sales_daily
AS
SELECT 
    sale_date,
    sku,
    region_code,
    channel_code,
    SUM(qty) AS daily_sales_qty,
    SUM(net_sales) AS daily_net_sales
FROM pricing.fact_sales
GROUP BY sale_date, sku, region_code, channel_code;
GO
