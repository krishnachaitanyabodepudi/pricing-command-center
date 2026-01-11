USE PricingDWH;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_WARNINGS ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET ARITHABORT ON;
GO

CREATE OR ALTER TRIGGER pricing.trg_log_price_override
ON pricing.fact_price_history
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Skip logging if ETL session context is set
    IF TRY_CAST(SESSION_CONTEXT(N'is_etl') AS INT) = 1
        RETURN;
    
    -- Log price changes for current records (effective_end IS NULL)
    INSERT INTO pricing.price_override_audit
        (sku, region_code, channel_code, old_price, new_price, changed_by, changed_at, reason)
    SELECT 
        i.sku,
        i.region_code,
        i.channel_code,
        d.price AS old_price,
        i.price AS new_price,
        ORIGINAL_LOGIN() AS changed_by,
        SYSUTCDATETIME() AS changed_at,
        NULL AS reason
    FROM inserted i
    INNER JOIN deleted d 
        ON i.price_hist_id = d.price_hist_id
    WHERE i.effective_end IS NULL
        AND d.price <> i.price;
END;
GO
