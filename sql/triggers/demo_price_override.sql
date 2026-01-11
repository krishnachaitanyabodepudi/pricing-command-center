USE PricingDWH;
GO

PRINT 'Demo: Price Override Audit Trigger';
PRINT '';

-- Step 1: Set ETL context and update a current price (should NOT log)
PRINT 'Step 1: Updating price with ETL context (should NOT log)...';
EXEC sp_set_session_context @key = N'is_etl', @value = 1;

UPDATE pricing.fact_price_history
SET price = price + 1.00
WHERE effective_end IS NULL
    AND price_hist_id = (SELECT TOP 1 price_hist_id FROM pricing.fact_price_history WHERE effective_end IS NULL ORDER BY price_hist_id);

PRINT 'Updated with ETL context set.';
GO

-- Step 2: Clear ETL context and update a current price (should log)
PRINT '';
PRINT 'Step 2: Updating price without ETL context (should log)...';
EXEC sp_set_session_context @key = N'is_etl', @value = NULL;

DECLARE @TestSku VARCHAR(255);
DECLARE @TestRegion VARCHAR(10);
DECLARE @TestChannel VARCHAR(10);
DECLARE @OldPrice DECIMAL(18,4);

-- Get a current price record for testing
SELECT TOP 1 
    @TestSku = sku,
    @TestRegion = region_code,
    @TestChannel = channel_code,
    @OldPrice = price
FROM pricing.fact_price_history
WHERE effective_end IS NULL
ORDER BY price_hist_id;

IF @TestSku IS NOT NULL
BEGIN
    UPDATE pricing.fact_price_history
    SET price = price + 5.50
    WHERE sku = @TestSku
        AND region_code = @TestRegion
        AND channel_code = @TestChannel
        AND effective_end IS NULL
        AND price_hist_id = (SELECT TOP 1 price_hist_id FROM pricing.fact_price_history 
                            WHERE sku = @TestSku AND region_code = @TestRegion 
                            AND channel_code = @TestChannel AND effective_end IS NULL);
    
    PRINT 'Updated price for: ' + @TestSku + ', ' + @TestRegion + ', ' + @TestChannel;
END
ELSE
BEGIN
    PRINT 'No current price records found for testing.';
END
GO

-- Step 3: Show audit log results
PRINT '';
PRINT 'Step 3: Last 5 audit log entries:';
SELECT TOP 5
    audit_id,
    sku,
    region_code,
    channel_code,
    old_price,
    new_price,
    changed_by,
    changed_at,
    reason
FROM pricing.price_override_audit
ORDER BY audit_id DESC;
GO

PRINT '';
PRINT 'Demo complete.';
GO


