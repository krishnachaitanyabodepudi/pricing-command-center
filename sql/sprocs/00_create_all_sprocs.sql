USE PricingDWH;
GO

PRINT 'Creating stored procedures...';
GO

-- Create/update sp_refresh_pricing_mart
:r sql/sprocs/sp_refresh_pricing_mart.sql
GO

PRINT 'Stored procedures created.';
GO


