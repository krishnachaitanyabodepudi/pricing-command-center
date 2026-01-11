USE PricingDWH;
GO

PRINT 'Starting seed data load...';
PRINT '';
GO

-- ============================================================================
-- Step 1: Seeding regions
-- ============================================================================
PRINT 'Step 1: Seeding regions...';
GO

MERGE pricing.dim_region AS target
USING (
    SELECT 'NE' AS region_code, 'Northeast' AS region_name
    UNION ALL SELECT 'SE', 'Southeast'
    UNION ALL SELECT 'MW', 'Midwest'
    UNION ALL SELECT 'SW', 'Southwest'
    UNION ALL SELECT 'W', 'West'
    UNION ALL SELECT 'Central', 'Central'
) AS source
ON target.region_code = source.region_code
WHEN MATCHED THEN
    UPDATE SET region_name = source.region_name
WHEN NOT MATCHED THEN
    INSERT (region_code, region_name)
    VALUES (source.region_code, source.region_name);
GO

-- ============================================================================
-- Step 2: Seeding channels
-- ============================================================================
PRINT 'Step 2: Seeding channels...';
GO

MERGE pricing.dim_channel AS target
USING (
    SELECT 'ONLINE' AS channel_code, 'Online Store' AS channel_name
    UNION ALL SELECT 'RETAIL', 'Retail Stores'
    UNION ALL SELECT 'DISTRIBUTOR', 'Distributor Network'
    UNION ALL SELECT 'DIRECT', 'Direct Sales'
) AS source
ON target.channel_code = source.channel_code
WHEN MATCHED THEN
    UPDATE SET channel_name = source.channel_name
WHEN NOT MATCHED THEN
    INSERT (channel_code, channel_name)
    VALUES (source.channel_code, source.channel_name);
GO

-- ============================================================================
-- Step 3: Seeding products
-- ============================================================================
PRINT 'Step 3: Seeding products...';
GO

MERGE pricing.dim_product AS target
USING (
    VALUES
    ('SKU-10001', 'Sterile Gloves Premium', 'Medical', 'MedTech', 1),
    ('SKU-10002', 'Surgical Scalpel Set', 'Surgical', 'SurgiPro', 1),
    ('SKU-10003', 'Antibiotic Ointment', 'Pharma', 'PharmCore', 1),
    ('SKU-10004', 'Digital Thermometer', 'Consumer', 'HealthHome', 1),
    ('SKU-10005', 'X-Ray Machine Mobile', 'Equipment', 'MedEquip', 1),
    ('SKU-10006', 'Disposable Syringes 10ml', 'Medical', 'MedTech', 1),
    ('SKU-10007', 'Surgical Mask N95', 'Medical', 'MedTech', 1),
    ('SKU-10008', 'Suture Kit Professional', 'Surgical', 'SurgiPro', 1),
    ('SKU-10009', 'Pain Relief Capsules', 'Pharma', 'PharmCore', 1),
    ('SKU-10010', 'Blood Pressure Monitor', 'Consumer', 'HealthHome', 1),
    ('SKU-10011', 'Ultrasound Scanner', 'Equipment', 'MedEquip', 1),
    ('SKU-10012', 'Bandage Roll 4in', 'Medical', 'MedTech', 1),
    ('SKU-10013', 'Surgical Forceps', 'Surgical', 'SurgiPro', 1),
    ('SKU-10014', 'Antihistamine Tablets', 'Pharma', 'PharmCore', 1),
    ('SKU-10015', 'First Aid Kit Deluxe', 'Consumer', 'HealthHome', 1),
    ('SKU-10016', 'ECG Machine Portable', 'Equipment', 'MedEquip', 1),
    ('SKU-10017', 'IV Catheter 18G', 'Medical', 'MedTech', 1),
    ('SKU-10018', 'Surgical Scissors', 'Surgical', 'SurgiPro', 1),
    ('SKU-10019', 'Cough Syrup', 'Pharma', 'PharmCore', 1),
    ('SKU-10020', 'Pulse Oximeter', 'Consumer', 'HealthHome', 1),
    ('SKU-10021', 'Defibrillator AED', 'Equipment', 'MedEquip', 1),
    ('SKU-10022', 'Gauze Pads 4x4', 'Medical', 'MedTech', 1),
    ('SKU-10023', 'Surgical Retractor', 'Surgical', 'SurgiPro', 1),
    ('SKU-10024', 'Antacid Tablets', 'Pharma', 'PharmCore', 1),
    ('SKU-10025', 'Neck Brace Adjustable', 'Consumer', 'HealthHome', 1),
    ('SKU-10026', 'Ventilator ICU', 'Equipment', 'MedEquip', 1),
    ('SKU-10027', 'Alcohol Swabs 100ct', 'Medical', 'MedTech', 1),
    ('SKU-10028', 'Surgical Clamp', 'Surgical', 'SurgiPro', 1),
    ('SKU-10029', 'Cold & Flu Medicine', 'Pharma', 'PharmCore', 1),
    ('SKU-10030', 'Knee Brace Support', 'Consumer', 'HealthHome', 1),
    ('SKU-10031', 'MRI Machine 3T', 'Equipment', 'MedEquip', 1),
    ('SKU-10032', 'Wound Dressing Pack', 'Medical', 'MedTech', 1),
    ('SKU-10033', 'Surgical Needle Holder', 'Surgical', 'SurgiPro', 1),
    ('SKU-10034', 'Vitamin D Supplements', 'Pharma', 'PharmCore', 1),
    ('SKU-10035', 'Ankle Support Wrap', 'Consumer', 'HealthHome', 1),
    ('SKU-10036', 'CT Scanner 64-Slice', 'Equipment', 'MedEquip', 1),
    ('SKU-10037', 'Adhesive Tape Medical', 'Medical', 'MedTech', 1),
    ('SKU-10038', 'Surgical Blade #11', 'Surgical', 'SurgiPro', 1),
    ('SKU-10039', 'Multivitamin Capsules', 'Pharma', 'PharmCore', 1),
    ('SKU-10040', 'Wrist Support Brace', 'Consumer', 'HealthHome', 1),
    ('SKU-10041', 'Infusion Pump', 'Equipment', 'MedEquip', 1),
    ('SKU-10042', 'Cotton Swabs Sterile', 'Medical', 'MedTech', 1),
    ('SKU-10043', 'Surgical Hemostat', 'Surgical', 'SurgiPro', 1),
    ('SKU-10044', 'Calcium Supplements', 'Pharma', 'PharmCore', 1),
    ('SKU-10045', 'Back Support Belt', 'Consumer', 'HealthHome', 1),
    ('SKU-10046', 'Patient Monitor', 'Equipment', 'MedEquip', 1),
    ('SKU-10047', 'Disposable Gloves Latex', 'Medical', 'MedTech', 1),
    ('SKU-10048', 'Surgical Probe', 'Surgical', 'SurgiPro', 1),
    ('SKU-10049', 'Iron Supplements', 'Pharma', 'PharmCore', 1),
    ('SKU-10050', 'Compression Stockings', 'Consumer', 'HealthHome', 1),
    ('SKU-10051', 'Autoclave Sterilizer', 'Equipment', 'MedEquip', 1),
    ('SKU-10052', 'Antiseptic Solution', 'Medical', 'MedTech', 1),
    ('SKU-10053', 'Surgical Sponge', 'Surgical', 'SurgiPro', 1),
    ('SKU-10054', 'Probiotic Capsules', 'Pharma', 'PharmCore', 1),
    ('SKU-10055', 'Elbow Support Brace', 'Consumer', 'HealthHome', 1)
) AS source (sku, product_name, category, brand, is_active)
ON target.sku = source.sku
WHEN MATCHED THEN
    UPDATE SET 
        product_name = source.product_name,
        category = source.category,
        brand = source.brand,
        is_active = source.is_active
WHEN NOT MATCHED THEN
    INSERT (sku, product_name, category, brand, is_active)
    VALUES (source.sku, source.product_name, source.category, source.brand, source.is_active);
GO

-- ============================================================================
-- Step 4: Seeding pricing rules
-- ============================================================================
PRINT 'Step 4: Seeding pricing rules...';
GO

MERGE pricing.dim_pricing_rule AS target
USING (
    VALUES
    ('Volume Discount Tier 1', 'VOLUME', 1, '2024-01-01', '2025-12-31', 1),
    ('Competitive Match Policy', 'COMPETITIVE', 2, '2024-06-01', '2025-06-30', 1),
    ('Seasonal Promotion Q1', 'SEASONAL', 3, '2025-01-01', '2025-03-31', 1),
    ('Channel Pricing Online', 'CHANNEL', 2, '2024-01-01', NULL, 1),
    ('Emergency Pricing Override', 'OVERRIDE', 1, '2024-01-01', '2025-12-31', 1),
    ('End of Life Discount', 'EOL', 4, '2024-09-01', '2024-12-31', 0),
    ('New Product Launch', 'LAUNCH', 2, '2025-01-15', '2025-04-15', 1),
    ('Regional Adjustment NE', 'REGIONAL', 5, '2024-03-01', '2025-03-01', 1),
    ('Bulk Order Discount', 'VOLUME', 3, '2024-01-01', NULL, 1),
    ('Contract Pricing Tier', 'CONTRACT', 1, '2024-01-01', '2025-12-31', 1)
) AS source (rule_name, rule_type, priority, start_date, end_date, is_active)
ON target.rule_name = source.rule_name
WHEN MATCHED THEN
    UPDATE SET
        rule_type = source.rule_type,
        priority = source.priority,
        start_date = source.start_date,
        end_date = source.end_date,
        is_active = source.is_active
WHEN NOT MATCHED THEN
    INSERT (rule_name, rule_type, priority, start_date, end_date, is_active)
    VALUES (source.rule_name, source.rule_type, source.priority, source.start_date, source.end_date, source.is_active);
GO

-- ============================================================================
-- Step 5: Seeding staging sales
-- ============================================================================
PRINT 'Step 5: Seeding staging sales...';
GO

TRUNCATE TABLE pricing.stg_sales;
GO

DECLARE @StartDate DATE = DATEADD(DAY, -60, CAST(GETDATE() AS DATE));
DECLARE @EndDate DATE = CAST(GETDATE() AS DATE);
DECLARE @RowCount INT = 10000;
DECLARE @Counter INT = 1;

WHILE @Counter <= @RowCount
BEGIN
    DECLARE @SaleDate DATE = DATEADD(DAY, ABS(CHECKSUM(NEWID())) % 61, @StartDate);
    DECLARE @SkuNum INT = (ABS(CHECKSUM(NEWID())) % 55) + 1;
    DECLARE @Sku VARCHAR(255) = 'SKU-' + FORMAT(10000 + @SkuNum, 'D5');
    DECLARE @RegionNum INT = (ABS(CHECKSUM(NEWID())) % 6) + 1;
    DECLARE @RegionCodes TABLE (idx INT, code VARCHAR(10));
    INSERT INTO @RegionCodes VALUES (1, 'NE'), (2, 'SE'), (3, 'MW'), (4, 'SW'), (5, 'W'), (6, 'Central');
    DECLARE @RegionCode VARCHAR(10) = (SELECT code FROM @RegionCodes WHERE idx = @RegionNum);
    DECLARE @ChannelNum INT = (ABS(CHECKSUM(NEWID())) % 4) + 1;
    DECLARE @ChannelCodes TABLE (idx INT, code VARCHAR(10));
    INSERT INTO @ChannelCodes VALUES (1, 'ONLINE'), (2, 'RETAIL'), (3, 'DISTRIBUTOR'), (4, 'DIRECT');
    DECLARE @ChannelCode VARCHAR(10) = (SELECT code FROM @ChannelCodes WHERE idx = @ChannelNum);
    DECLARE @Qty INT = (ABS(CHECKSUM(NEWID())) % 100) + 1;
    DECLARE @UnitPrice DECIMAL(18,2) = CAST((ABS(CHECKSUM(NEWID())) % 50000 + 1000) AS DECIMAL(18,2)) / 100.0;
    DECLARE @NetSales DECIMAL(18,2) = @Qty * @UnitPrice;
    DECLARE @SourceFile NVARCHAR(500) = 'sales_' + FORMAT(@SaleDate, 'yyyy_MM_dd') + '.csv';
    
    INSERT INTO pricing.stg_sales (sale_date, sku, region_code, channel_code, qty, net_sales, source_file)
    VALUES (@SaleDate, @Sku, @RegionCode, @ChannelCode, @Qty, @NetSales, @SourceFile);
    
    SET @Counter = @Counter + 1;
END;
GO

-- ============================================================================
-- Step 6: Seeding staging price history
-- ============================================================================
PRINT 'Step 6: Seeding staging price history...';
GO

TRUNCATE TABLE pricing.stg_price_history;
GO

-- Generate price history with effective_start/end logic
DECLARE @BaseDate DATE = DATEADD(DAY, -90, CAST(GETDATE() AS DATE));
DECLARE @SkuCounter INT = 1;
DECLARE @RegionCodes TABLE (code VARCHAR(10));
INSERT INTO @RegionCodes VALUES ('NE'), ('SE'), ('MW'), ('SW'), ('W'), ('Central');
DECLARE @ChannelCodes TABLE (code VARCHAR(10));
INSERT INTO @ChannelCodes VALUES ('ONLINE'), ('RETAIL'), ('DISTRIBUTOR'), ('DIRECT');

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

-- Intentionally inject bad data: missing/invalid SKUs
DECLARE @BadDataBaseDate DATE = DATEADD(DAY, -90, CAST(GETDATE() AS DATE));
INSERT INTO pricing.stg_price_history 
    (sku, region_code, channel_code, price, currency, effective_start, effective_end, source_system)
VALUES
    (NULL, 'NE', 'ONLINE', 99.99, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS'),
    ('INVALID-SKU-99999', 'SE', 'RETAIL', 149.50, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS'),
    ('', 'MW', 'DISTRIBUTOR', 199.00, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS'),
    ('SKU-XXXXX', 'SW', 'DIRECT', 249.75, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS'),
    ('SKU-99999', 'W', 'ONLINE', 299.99, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS'),
    (NULL, 'Central', 'RETAIL', 89.50, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS'),
    ('BAD-SKU-001', 'NE', 'DISTRIBUTOR', 179.25, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS'),
    ('', 'SE', 'DIRECT', 219.00, 'USD', @BadDataBaseDate, NULL, 'PRICING_SYS'),
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

-- ============================================================================
-- Step 7: Seeding staging discount events
-- ============================================================================
PRINT 'Step 7: Seeding staging discount events...';
GO

TRUNCATE TABLE pricing.stg_discount_events;
GO

DECLARE @BaseDate DATE = DATEADD(DAY, -30, CAST(GETDATE() AS DATE));
DECLARE @SkuCounter INT = 1;
DECLARE @RegionCodes TABLE (code VARCHAR(10));
INSERT INTO @RegionCodes VALUES ('NE'), ('SE'), ('MW'), ('SW'), ('W'), ('Central');
DECLARE @ChannelCodes TABLE (code VARCHAR(10));
INSERT INTO @ChannelCodes VALUES ('ONLINE'), ('RETAIL'), ('DISTRIBUTOR'), ('DIRECT');
DECLARE @DiscountTypes TABLE (type NVARCHAR(100));
INSERT INTO @DiscountTypes VALUES ('PERCENT'), ('FLAT');

-- Generate discount events
WHILE @SkuCounter <= 40
BEGIN
    DECLARE @Sku VARCHAR(255) = 'SKU-' + FORMAT(10000 + @SkuCounter, 'D5');
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
DECLARE @OverlapSku VARCHAR(255) = 'SKU-10010';
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

PRINT '';
PRINT 'Seed data load complete.';
GO

