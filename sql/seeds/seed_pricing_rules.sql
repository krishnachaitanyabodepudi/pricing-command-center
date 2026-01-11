USE PricingDWH;
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


