USE PricingDWH;
GO

TRUNCATE TABLE pricing.stg_price_history;
GO

-- Generate price history with effective_start/end logic
DECLARE @BaseDate DATE = DATEADD(DAY, -90, CAST(GETDATE() AS DATE));
DECLARE @SkuCounter INT = 1;
DECLARE @RegionCodes TABLE (code VARCHAR(10));
INSERT INTO @RegionCodes VALUES ('NE'), ('SE'), ('MW'), ('SW'), ('W'), ('Central');
DECLARE @ChannelCodes TABLE (code VARCHAR(10));
INSERT INTO @ChannelCodes VALUES ('ONLINE'), ('RETAIL'), ('DIST'), ('DIRECT');

-- Create price history for each SKU/Region/Channel combination
WHILE @SkuCounter <= 55
BEGIN
    DECLARE @Sku VARCHAR(255) = 'SKU-' + FORMAT(10000 + @SkuCounter, 'D5');
    
    DECLARE @RegionCursor CURSOR;
    SET @RegionCursor = CURSOR FOR SELECT code FROM @RegionCodes;
    OPEN @RegionCursor;
    DECLARE @RegionCode VARCHAR(10);
    FETCH NEXT FROM @RegionCursor INTO @RegionCode;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        DECLARE @ChannelCursor CURSOR;
        SET @ChannelCursor = CURSOR FOR SELECT code FROM @ChannelCodes;
        OPEN @ChannelCursor;
        DECLARE @ChannelCode VARCHAR(10);
        FETCH NEXT FROM @ChannelCursor INTO @ChannelCode;
        
        WHILE @@FETCH_STATUS = 0
        BEGIN
            DECLARE @PriceChangeCount INT = (ABS(CHECKSUM(NEWID())) % 4) + 1;
            DECLARE @PriceChangeIndex INT = 0;
            DECLARE @PrevEndDate DATE = NULL;
            DECLARE @CurrentStart DATE = DATEADD(DAY, (ABS(CHECKSUM(NEWID())) % 30), @BaseDate);
            
            WHILE @PriceChangeIndex < @PriceChangeCount
            BEGIN
                DECLARE @Price DECIMAL(18,4) = CAST((ABS(CHECKSUM(NEWID())) % 50000 + 1000) AS DECIMAL(18,4)) / 100.0;
                DECLARE @Currency CHAR(3) = 'USD';
                DECLARE @EffectiveStart DATE = @CurrentStart;
                DECLARE @EffectiveEnd DATE = NULL;
                
                IF @PriceChangeIndex < @PriceChangeCount - 1
                BEGIN
                    SET @EffectiveEnd = DATEADD(DAY, (ABS(CHECKSUM(NEWID())) % 30) + 1, @EffectiveStart);
                    SET @CurrentStart = DATEADD(DAY, 1, @EffectiveEnd);
                END
                
                INSERT INTO pricing.stg_price_history 
                    (sku, region_code, channel_code, price, currency, effective_start, effective_end, source_system)
                VALUES (@Sku, @RegionCode, @ChannelCode, @Price, @Currency, @EffectiveStart, @EffectiveEnd, 'PRICING_SYS');
                
                SET @PriceChangeIndex = @PriceChangeIndex + 1;
            END;
            
            FETCH NEXT FROM @ChannelCursor INTO @ChannelCode;
        END;
        CLOSE @ChannelCursor;
        DEALLOCATE @ChannelCursor;
        
        FETCH NEXT FROM @RegionCursor INTO @RegionCode;
    END;
    CLOSE @RegionCursor;
    DEALLOCATE @RegionCursor;
    
    SET @SkuCounter = @SkuCounter + 1;
END;

-- Intentionally inject bad data: negative prices
INSERT INTO pricing.stg_price_history 
    (sku, region_code, channel_code, price, currency, effective_start, effective_end, source_system)
SELECT TOP 10
    sku, region_code, channel_code,
    -ABS(price) AS price,
    currency, effective_start, effective_end, source_system
FROM pricing.stg_price_history
ORDER BY NEWID();
GO

-- Intentionally inject bad data: missing/invalid SKUs (at least 10 with NULL or blank SKU)
DECLARE @BadDataBaseDate DATE = DATEADD(DAY, -90, CAST(GETDATE() AS DATE));
INSERT INTO pricing.stg_price_history 
    (sku, region_code, channel_code, price, currency, effective_start, effective_end, source_system)
VALUES
    -- 6 rows with NULL SKU
    (NULL, 'NE', 'ONLINE', 99.99, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS'),
    (NULL, 'SE', 'RETAIL', 149.50, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS'),
    (NULL, 'MW', 'DIST', 199.00, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS'),
    (NULL, 'SW', 'DIRECT', 249.75, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS'),
    (NULL, 'W', 'ONLINE', 299.99, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS'),
    (NULL, 'Central', 'RETAIL', 89.50, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS'),
    -- 6 rows with blank SKU
    ('', 'NE', 'RETAIL', 179.25, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS'),
    ('', 'SE', 'DIST', 219.00, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS'),
    ('', 'MW', 'DIRECT', 159.99, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS'),
    ('', 'SW', 'ONLINE', 269.50, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS'),
    ('', 'W', 'RETAIL', 189.75, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS'),
    ('', 'Central', 'DIST', 209.25, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS'),
    -- Additional invalid SKUs (not in dim_product, but have SKU value)
    ('INVALID-SKU-99999', 'NE', 'RETAIL', 149.50, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS'),
    ('SKU-XXXXX', 'SW', 'DIRECT', 249.75, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS'),
    ('SKU-99999', 'W', 'ONLINE', 299.99, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS'),
    ('BAD-SKU-001', 'NE', 'DIST', 179.25, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS'),
    ('SKU-NULL', 'MW', 'ONLINE', 159.99, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS'),
    ('INVALID', 'SW', 'RETAIL', 269.50, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS');
GO

-- Intentionally inject bad data: late-arriving price records (loaded today, effective_start 30+ days ago)
DECLARE @LateArrivingDate DATE = DATEADD(DAY, -45, CAST(GETDATE() AS DATE));
INSERT INTO pricing.stg_price_history 
    (sku, region_code, channel_code, price, currency, effective_start, effective_end, source_system, loaded_at)
SELECT TOP 5
    'SKU-' + FORMAT(10000 + (ABS(CHECKSUM(NEWID())) % 55 + 1), 'D5'),
    region_code, channel_code,
    CAST((ABS(CHECKSUM(NEWID())) % 50000 + 1000) AS DECIMAL(18,4)) / 100.0,
    'USD', @LateArrivingDate, NULL, 'LATE_LOAD_SYS', GETDATE()
FROM pricing.stg_price_history
ORDER BY NEWID();
GO

