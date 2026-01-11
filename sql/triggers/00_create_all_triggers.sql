USE PricingDWH;
GO

PRINT 'Creating triggers...';
GO

-- Create/update trg_log_price_override
:r sql/triggers/trg_log_price_override.sql
GO

PRINT 'Triggers created.';
GO


