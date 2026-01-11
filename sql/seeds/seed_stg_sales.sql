USE PricingDWH;
GO

TRUNCATE TABLE pricing.stg_sales;
GO

DECLARE @StartDate DATE = DATEADD(DAY, -60, CAST(GETDATE() AS DATE));
DECLARE @EndDate DATE = CAST(GETDATE() AS DATE);
DECLARE @RowCount INT = 10000;
DECLARE @Counter INT = 1;

-- Pre-declare table variables outside the loop
DECLARE @RegionCodes TABLE (idx INT, code VARCHAR(10));
INSERT INTO @RegionCodes VALUES (1, 'NE'), (2, 'SE'), (3, 'MW'), (4, 'SW'), (5, 'W'), (6, 'Central');

DECLARE @ChannelCodes TABLE (idx INT, code VARCHAR(10));
INSERT INTO @ChannelCodes VALUES (1, 'ONLINE'), (2, 'RETAIL'), (3, 'DIST'), (4, 'DIRECT');

WHILE @Counter <= @RowCount
BEGIN
    DECLARE @SaleDate DATE = DATEADD(DAY, ABS(CHECKSUM(NEWID())) % 61, @StartDate);
    DECLARE @SkuNum INT = (ABS(CHECKSUM(NEWID())) % 55) + 1;
    DECLARE @Sku VARCHAR(50) = 'SKU-' + FORMAT(10000 + @SkuNum, 'D5');
    
    -- Use TOP (1) ORDER BY NEWID() for random selection
    DECLARE @RegionCode VARCHAR(20) = (SELECT TOP (1) code FROM @RegionCodes ORDER BY NEWID());
    DECLARE @ChannelCode VARCHAR(20) = (SELECT TOP (1) code FROM @ChannelCodes ORDER BY NEWID());
    
    DECLARE @Qty INT = (ABS(CHECKSUM(NEWID())) % 100) + 1;
    DECLARE @UnitPrice DECIMAL(18,2) = CAST((ABS(CHECKSUM(NEWID())) % 50000 + 1000) AS DECIMAL(18,2)) / 100.0;
    DECLARE @NetSales DECIMAL(18,2) = @Qty * @UnitPrice;
    DECLARE @SourceFile NVARCHAR(260) = 'sales_' + FORMAT(@SaleDate, 'yyyy_MM_dd') + '.csv';
    
    INSERT INTO pricing.stg_sales (sale_date, sku, region_code, channel_code, qty, net_sales, source_file)
    VALUES (@SaleDate, @Sku, @RegionCode, @ChannelCode, @Qty, @NetSales, @SourceFile);
    
    SET @Counter = @Counter + 1;
END;
GO


