USE PricingDWH;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_WARNINGS ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET ARITHABORT ON;
GO

CREATE OR ALTER PROCEDURE pricing.sp_get_current_price
    @sku VARCHAR(255),
    @region_code VARCHAR(10),
    @channel_code VARCHAR(10)
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT TOP (1)
        sku,
        region_code,
        channel_code,
        price AS current_price,
        currency,
        effective_start,
        effective_end
    FROM pricing.fact_price_history
    WHERE sku = @sku
        AND region_code = @region_code
        AND channel_code = @channel_code
        AND effective_end IS NULL
    ORDER BY effective_start DESC;
END;
GO

