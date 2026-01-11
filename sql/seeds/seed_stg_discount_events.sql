USE PricingDWH;
GO

TRUNCATE TABLE pricing.stg_discount_events;
GO

DECLARE @BaseDate DATE = DATEADD(DAY, -30, CAST(GETDATE() AS DATE));
DECLARE @SkuCounter INT = 1;
DECLARE @RegionCodes TABLE (code VARCHAR(10));
INSERT INTO @RegionCodes VALUES ('NE'), ('SE'), ('MW'), ('SW'), ('W'), ('Central');
DECLARE @ChannelCodes TABLE (code VARCHAR(10));
INSERT INTO @ChannelCodes VALUES ('ONLINE'), ('RETAIL'), ('DIST'), ('DIRECT');
DECLARE @DiscountTypes TABLE (type NVARCHAR(100));
INSERT INTO @DiscountTypes VALUES ('PERCENT'), ('FLAT');

-- Generate discount events
WHILE @SkuCounter <= 200
BEGIN
    -- Cycle through existing SKUs (1-55) using modulo to ensure all SKUs exist in dim_product
    DECLARE @SkuNum INT = ((@SkuCounter - 1) % 55) + 1;
    DECLARE @Sku VARCHAR(50) = 'SKU-' + FORMAT(10000 + @SkuNum, 'D5');
    DECLARE @RegionCodesList TABLE (idx INT IDENTITY(1,1), code VARCHAR(10));
    INSERT INTO @RegionCodesList SELECT code FROM @RegionCodes;
    DECLARE @RegionNum INT = (ABS(CHECKSUM(NEWID())) % 6) + 1;
    DECLARE @RegionCode VARCHAR(10) = (SELECT code FROM @RegionCodesList WHERE idx = @RegionNum);
    
    DECLARE @ChannelCodesList TABLE (idx INT IDENTITY(1,1), code VARCHAR(10));
    INSERT INTO @ChannelCodesList SELECT code FROM @ChannelCodes;
    DECLARE @ChannelNum INT = (ABS(CHECKSUM(NEWID())) % 4) + 1;
    DECLARE @ChannelCode VARCHAR(10) = (SELECT code FROM @ChannelCodesList WHERE idx = @ChannelNum);
    
    DECLARE @DiscountTypesList TABLE (idx INT IDENTITY(1,1), type NVARCHAR(100));
    INSERT INTO @DiscountTypesList SELECT type FROM @DiscountTypes;
    DECLARE @DiscountTypeNum INT = (ABS(CHECKSUM(NEWID())) % 2) + 1;
    DECLARE @DiscountType NVARCHAR(100) = (SELECT type FROM @DiscountTypesList WHERE idx = @DiscountTypeNum);
    DECLARE @StartDate DATE = DATEADD(DAY, (ABS(CHECKSUM(NEWID())) % 30), @BaseDate);
    DECLARE @EndDate DATE = DATEADD(DAY, (ABS(CHECKSUM(NEWID())) % 15) + 7, @StartDate);
    
    DECLARE @DiscountValue DECIMAL(18,4);
    IF @DiscountType = 'PERCENT'
        SET @DiscountValue = CAST((ABS(CHECKSUM(NEWID())) % 50 + 5) AS DECIMAL(18,4));
    ELSE
        SET @DiscountValue = CAST((ABS(CHECKSUM(NEWID())) % 1000 + 10) AS DECIMAL(18,4));
    
    INSERT INTO pricing.stg_discount_events 
        (sku, region_code, channel_code, discount_type, discount_value, start_date, end_date)
    VALUES (@Sku, @RegionCode, @ChannelCode, @DiscountType, @DiscountValue, @StartDate, @EndDate);
    
    SET @SkuCounter = @SkuCounter + 1;
END;

-- Intentionally inject bad data: overlapping discount events for same sku/region/channel
DECLARE @OverlapSku VARCHAR(50) = 'SKU-10010';
DECLARE @OverlapRegion VARCHAR(10) = 'NE';
DECLARE @OverlapChannel VARCHAR(10) = 'ONLINE';
DECLARE @OverlapBase DATE = DATEADD(DAY, -20, CAST(GETDATE() AS DATE));

INSERT INTO pricing.stg_discount_events 
    (sku, region_code, channel_code, discount_type, discount_value, start_date, end_date)
VALUES
    (@OverlapSku, @OverlapRegion, @OverlapChannel, 'PERCENT', 15.0, @OverlapBase, DATEADD(DAY, 10, @OverlapBase)),
    (@OverlapSku, @OverlapRegion, @OverlapChannel, 'PERCENT', 20.0, DATEADD(DAY, 5, @OverlapBase), DATEADD(DAY, 15, @OverlapBase)),
    (@OverlapSku, @OverlapRegion, @OverlapChannel, 'FLAT', 50.0, DATEADD(DAY, 8, @OverlapBase), DATEADD(DAY, 18, @OverlapBase)),
    (@OverlapSku, @OverlapRegion, @OverlapChannel, 'PERCENT', 25.0, DATEADD(DAY, 2, @OverlapBase), DATEADD(DAY, 12, @OverlapBase)),
    (@OverlapSku, @OverlapRegion, @OverlapChannel, 'FLAT', 75.0, DATEADD(DAY, 1, @OverlapBase), DATEADD(DAY, 14, @OverlapBase));
GO

