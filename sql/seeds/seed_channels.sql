USE PricingDWH;
GO

MERGE pricing.dim_channel AS target
USING (
    SELECT 'ONLINE' AS channel_code, 'Online Store' AS channel_name
    UNION ALL SELECT 'RETAIL', 'Retail Stores'
    UNION ALL SELECT 'DIST', 'Distributor Network'
    UNION ALL SELECT 'DIRECT', 'Direct Sales'
) AS source
ON target.channel_code = source.channel_code
WHEN MATCHED THEN
    UPDATE SET channel_name = source.channel_name
WHEN NOT MATCHED THEN
    INSERT (channel_code, channel_name)
    VALUES (source.channel_code, source.channel_name);
GO


