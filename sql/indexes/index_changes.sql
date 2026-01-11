USE PricingDWH;
GO

-- Performance optimization indexes for pricing queries
-- These indexes support the optimized query patterns

-- Index for fact_sales: Supports date range and region/channel filters with included columns
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_fact_sales_perf_date_region_channel' AND object_id = OBJECT_ID('pricing.fact_sales'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_fact_sales_perf_date_region_channel
    ON pricing.fact_sales (sale_date, region_code, channel_code, sku)
    INCLUDE (qty, net_sales);
END
GO

-- Index for fact_discount_events: Supports active discount lookups with date range filtering
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_fact_discount_events_perf_region_channel_sku_dates' AND object_id = OBJECT_ID('pricing.fact_discount_events'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_fact_discount_events_perf_region_channel_sku_dates
    ON pricing.fact_discount_events (region_code, channel_code, sku, start_date, end_date)
    INCLUDE (discount_value, discount_type);
END
GO

-- Index for fact_price_history: Supports current price lookups with effective date range filtering
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_fact_price_history_perf_region_channel_sku_effective' AND object_id = OBJECT_ID('pricing.fact_price_history'))
BEGIN
    CREATE NONCLUSTERED INDEX IX_fact_price_history_perf_region_channel_sku_effective
    ON pricing.fact_price_history (region_code, channel_code, sku, effective_start, effective_end)
    INCLUDE (price, currency, created_at);
END
GO

PRINT 'Performance indexes created.';
GO


