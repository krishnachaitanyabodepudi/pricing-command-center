USE PricingDWH;
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


