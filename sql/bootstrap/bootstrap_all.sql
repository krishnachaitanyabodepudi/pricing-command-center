-- Bootstrap script: Sets up entire PricingDWH database from scratch
-- Run via: docker exec -i pricing-sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "password" -d PricingDWH -C -i /workspace/sql/bootstrap/bootstrap_all.sql
-- NOTE: This script uses :r includes and requires SQLCMD mode
-- All paths are absolute and rooted at /workspace/sql/

USE PricingDWH;
GO

PRINT '============================================================';
PRINT 'PRICING COMMAND CENTER - DATABASE BOOTSTRAP';
PRINT '============================================================';
PRINT '';
GO

-- Step 1: Create Schema
PRINT 'Step 1: Creating database schema...';
PRINT '';
:r /workspace/sql/ddl/01_schema.sql
GO

PRINT 'Schema creation complete.';
PRINT '';
GO

-- Step 2: Seed Data - Dimensions
PRINT 'Step 2: Loading seed data...';
PRINT '';
PRINT '  Step 2.1: Seeding regions...';
:r /workspace/sql/seeds/seed_regions.sql
GO

PRINT '  Step 2.2: Seeding channels...';
:r /workspace/sql/seeds/seed_channels.sql
GO

PRINT '  Step 2.3: Seeding products...';
:r /workspace/sql/seeds/seed_products.sql
GO

PRINT '  Step 2.4: Seeding pricing rules...';
:r /workspace/sql/seeds/seed_pricing_rules.sql
GO

PRINT '  Step 2.5: Seeding staging sales...';
:r /workspace/sql/seeds/seed_stg_sales.sql
GO

PRINT '  Step 2.6: Seeding staging price history...';
:r /workspace/sql/seeds/seed_stg_price_history.sql
GO

PRINT '  Step 2.7: Seeding staging discount events...';
:r /workspace/sql/seeds/seed_stg_discount_events.sql
GO

PRINT 'Seed data load complete.';
PRINT '';
GO

-- Step 3: Create Stored Procedures
PRINT 'Step 3: Creating stored procedures...';
PRINT '';
PRINT '  Step 3.1: Creating sp_refresh_pricing_mart...';
:r /workspace/sql/sprocs/sp_refresh_pricing_mart.sql
GO

PRINT 'Stored procedures creation complete.';
PRINT '';
GO

-- Step 4: Create Views
PRINT 'Step 4: Creating views...';
PRINT '';
PRINT '  Step 4.1: Creating vw_sales_daily...';
:r /workspace/sql/views/vw_sales_daily.sql
GO

PRINT '  Step 4.2: Creating vw_discount_active...';
:r /workspace/sql/views/vw_discount_active.sql
GO

PRINT '  Step 4.3: Creating vw_etl_latest_run...';
:r /workspace/sql/views/vw_etl_latest_run.sql
GO

PRINT '  Step 4.4: Creating vw_pricing_bi_dataset...';
:r /workspace/sql/views/vw_pricing_bi_dataset.sql
GO

PRINT 'Views creation complete.';
PRINT '';
GO

-- Step 5: Create Triggers
PRINT 'Step 5: Creating triggers...';
PRINT '';
PRINT '  Step 5.1: Creating trg_log_price_override...';
:r /workspace/sql/triggers/trg_log_price_override.sql
GO

PRINT 'Triggers creation complete.';
PRINT '';
GO

-- Step 6: Create Indexes (if exists)
PRINT 'Step 6: Creating indexes...';
PRINT '';
:r /workspace/sql/indexes/index_changes.sql
GO

PRINT 'Indexes step complete.';
PRINT '';
GO

-- Sanity Checks
PRINT '============================================================';
PRINT 'BOOTSTRAP COMPLETE - RUNNING SANITY CHECKS';
PRINT '============================================================';
PRINT '';
GO

PRINT 'Staging table row counts:';
SELECT 
    'stg_sales' AS table_name, 
    COUNT(*) AS row_count 
FROM pricing.stg_sales
UNION ALL
SELECT 'stg_price_history', COUNT(*) FROM pricing.stg_price_history
UNION ALL
SELECT 'stg_discount_events', COUNT(*) FROM pricing.stg_discount_events;
GO

PRINT '';
PRINT 'Fact table row counts:';
SELECT 
    'fact_sales' AS table_name, 
    COUNT(*) AS row_count 
FROM pricing.fact_sales
UNION ALL
SELECT 'fact_price_history', COUNT(*) FROM pricing.fact_price_history
UNION ALL
SELECT 'fact_discount_events', COUNT(*) FROM pricing.fact_discount_events;
GO

PRINT '';
PRINT 'Latest ETL run:';
SELECT TOP 1 
    run_id,
    pipeline_name,
    started_at,
    finished_at,
    status,
    rows_loaded,
    rows_rejected,
    failure_reason
FROM pricing.etl_run_history
ORDER BY run_id DESC;
GO

PRINT '';
PRINT '============================================================';
PRINT 'BOOTSTRAP COMPLETE - ALL STEPS FINISHED';
PRINT '============================================================';
GO
