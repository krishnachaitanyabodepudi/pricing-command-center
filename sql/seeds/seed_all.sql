-- NOTE: This script uses :r includes.
-- Run in SSMS with SQLCMD Mode enabled, or execute via sqlcmd.
-- Alternative: Use seed_all_no_r.sql for standard T-SQL execution.

USE PricingDWH;
GO

PRINT 'Starting seed data load...';
PRINT '';
GO

PRINT 'Step 1: Seeding regions...';
:r sql/seeds/seed_regions.sql
GO

PRINT 'Step 2: Seeding channels...';
:r sql/seeds/seed_channels.sql
GO

PRINT 'Step 3: Seeding products...';
:r sql/seeds/seed_products.sql
GO

PRINT 'Step 4: Seeding pricing rules...';
:r sql/seeds/seed_pricing_rules.sql
GO

PRINT 'Step 5: Seeding staging sales...';
:r sql/seeds/seed_stg_sales.sql
GO

PRINT 'Step 6: Seeding staging price history...';
:r sql/seeds/seed_stg_price_history.sql
GO

PRINT 'Step 7: Seeding staging discount events...';
:r sql/seeds/seed_stg_discount_events.sql
GO

PRINT '';
PRINT 'Seed data load complete.';
GO
