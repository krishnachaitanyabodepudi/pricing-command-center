USE PricingDWH;
GO

PRINT 'Creating views...';
GO

-- Create/update vw_sales_daily
:r sql/views/vw_sales_daily.sql
GO

-- Create/update vw_discount_active
:r sql/views/vw_discount_active.sql
GO

-- Create/update vw_etl_latest_run
:r sql/views/vw_etl_latest_run.sql
GO

-- Create/update vw_pricing_bi_dataset
:r sql/views/vw_pricing_bi_dataset.sql
GO

PRINT 'Views created.';
GO


