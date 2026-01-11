USE PricingDWH;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_WARNINGS ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET ARITHABORT ON;
GO

CREATE OR ALTER PROCEDURE pricing.sp_get_price_history
    @sku VARCHAR(255),
    @from_date DATE,
    @to_date DATE,
    @region_code VARCHAR(10) = NULL,
    @channel_code VARCHAR(10) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT 
        sku,
        region_code,
        channel_code,
        price,
        currency,
        effective_start,
        effective_end
    FROM pricing.fact_price_history
    WHERE sku = @sku
        AND effective_start <= @to_date
        AND (effective_end IS NULL OR effective_end >= @from_date)
        AND (@region_code IS NULL OR region_code = @region_code)
        AND (@channel_code IS NULL OR channel_code = @channel_code)
    ORDER BY effective_start DESC, effective_end DESC;
END;
GO

