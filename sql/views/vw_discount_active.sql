USE PricingDWH;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_WARNINGS ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET ARITHABORT ON;
GO

CREATE OR ALTER VIEW pricing.vw_discount_active
AS
SELECT 
    sku,
    region_code,
    channel_code,
    discount_type,
    discount_value,
    start_date,
    end_date
FROM pricing.fact_discount_events
WHERE start_date <= CAST(GETDATE() AS DATE)
    AND end_date >= CAST(GETDATE() AS DATE);
GO
