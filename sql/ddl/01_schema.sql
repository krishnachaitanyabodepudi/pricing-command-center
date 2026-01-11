USE PricingDWH;
GO

SET NOCOUNT ON;
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET ARITHABORT ON;
GO

-- Create schema if not exists
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'pricing')
BEGIN
    EXEC('CREATE SCHEMA pricing');
END
GO

-- Dimensions

-- dim_product
IF OBJECT_ID('pricing.dim_product', 'U') IS NULL
BEGIN
    CREATE TABLE pricing.dim_product (
        sku VARCHAR(255) NOT NULL,
        product_name NVARCHAR(500) NULL,
        category NVARCHAR(255) NULL,
        brand NVARCHAR(255) NULL,
        is_active BIT NOT NULL DEFAULT 1,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_dim_product PRIMARY KEY (sku)
    );
END
GO

-- dim_region
IF OBJECT_ID('pricing.dim_region', 'U') IS NULL
BEGIN
    CREATE TABLE pricing.dim_region (
        region_code VARCHAR(10) NOT NULL,
        region_name NVARCHAR(255) NULL,
        CONSTRAINT PK_dim_region PRIMARY KEY (region_code)
    );
END
GO

-- dim_channel
IF OBJECT_ID('pricing.dim_channel', 'U') IS NULL
BEGIN
    CREATE TABLE pricing.dim_channel (
        channel_code VARCHAR(10) NOT NULL,
        channel_name NVARCHAR(255) NULL,
        CONSTRAINT PK_dim_channel PRIMARY KEY (channel_code)
    );
END
GO

-- dim_pricing_rule
IF OBJECT_ID('pricing.dim_pricing_rule', 'U') IS NULL
BEGIN
    CREATE TABLE pricing.dim_pricing_rule (
        rule_id INT IDENTITY(1,1) NOT NULL,
        rule_name NVARCHAR(255) NULL,
        rule_type NVARCHAR(100) NULL,
        priority INT NULL,
        start_date DATE NULL,
        end_date DATE NULL,
        is_active BIT NOT NULL DEFAULT 1,
        CONSTRAINT PK_dim_pricing_rule PRIMARY KEY (rule_id)
    );
END
GO

-- Facts

-- fact_sales
IF OBJECT_ID('pricing.fact_sales', 'U') IS NULL
BEGIN
    CREATE TABLE pricing.fact_sales (
        sale_id BIGINT IDENTITY(1,1) NOT NULL,
        sale_date DATE NOT NULL,
        sku VARCHAR(255) NOT NULL,
        region_code VARCHAR(10) NOT NULL,
        channel_code VARCHAR(10) NOT NULL,
        qty INT NOT NULL,
        net_sales DECIMAL(18,2) NOT NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_fact_sales PRIMARY KEY (sale_id),
        CONSTRAINT FK_fact_sales_dim_product FOREIGN KEY (sku) REFERENCES pricing.dim_product(sku),
        CONSTRAINT FK_fact_sales_dim_region FOREIGN KEY (region_code) REFERENCES pricing.dim_region(region_code),
        CONSTRAINT FK_fact_sales_dim_channel FOREIGN KEY (channel_code) REFERENCES pricing.dim_channel(channel_code)
    );
END
GO

-- fact_price_history
IF OBJECT_ID('pricing.fact_price_history', 'U') IS NULL
BEGIN
    CREATE TABLE pricing.fact_price_history (
        price_hist_id BIGINT IDENTITY(1,1) NOT NULL,
        sku VARCHAR(255) NOT NULL,
        region_code VARCHAR(10) NOT NULL,
        channel_code VARCHAR(10) NOT NULL,
        price DECIMAL(18,4) NOT NULL,
        currency CHAR(3) NOT NULL,
        effective_start DATE NOT NULL,
        effective_end DATE NULL,
        source_system NVARCHAR(100) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_fact_price_history PRIMARY KEY (price_hist_id),
        CONSTRAINT FK_fact_price_history_dim_product FOREIGN KEY (sku) REFERENCES pricing.dim_product(sku),
        CONSTRAINT FK_fact_price_history_dim_region FOREIGN KEY (region_code) REFERENCES pricing.dim_region(region_code),
        CONSTRAINT FK_fact_price_history_dim_channel FOREIGN KEY (channel_code) REFERENCES pricing.dim_channel(channel_code)
    );
END
GO

-- fact_discount_events
IF OBJECT_ID('pricing.fact_discount_events', 'U') IS NULL
BEGIN
    CREATE TABLE pricing.fact_discount_events (
        discount_event_id BIGINT IDENTITY(1,1) NOT NULL,
        sku VARCHAR(255) NOT NULL,
        region_code VARCHAR(10) NOT NULL,
        channel_code VARCHAR(10) NOT NULL,
        discount_type NVARCHAR(100) NULL,
        discount_value DECIMAL(18,4) NOT NULL,
        start_date DATE NOT NULL,
        end_date DATE NOT NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_fact_discount_events PRIMARY KEY (discount_event_id),
        CONSTRAINT FK_fact_discount_events_dim_product FOREIGN KEY (sku) REFERENCES pricing.dim_product(sku),
        CONSTRAINT FK_fact_discount_events_dim_region FOREIGN KEY (region_code) REFERENCES pricing.dim_region(region_code),
        CONSTRAINT FK_fact_discount_events_dim_channel FOREIGN KEY (channel_code) REFERENCES pricing.dim_channel(channel_code)
    );
END
GO

-- fact_margin_impact
IF OBJECT_ID('pricing.fact_margin_impact', 'U') IS NULL
BEGIN
    CREATE TABLE pricing.fact_margin_impact (
        impact_id BIGINT IDENTITY(1,1) NOT NULL,
        sku VARCHAR(255) NOT NULL,
        region_code VARCHAR(10) NOT NULL,
        channel_code VARCHAR(10) NOT NULL,
        as_of_date DATE NOT NULL,
        price DECIMAL(18,4) NOT NULL,
        unit_cost DECIMAL(18,4) NOT NULL,
        margin_pct DECIMAL(9,4) NULL,
        created_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_fact_margin_impact PRIMARY KEY (impact_id),
        CONSTRAINT FK_fact_margin_impact_dim_product FOREIGN KEY (sku) REFERENCES pricing.dim_product(sku),
        CONSTRAINT FK_fact_margin_impact_dim_region FOREIGN KEY (region_code) REFERENCES pricing.dim_region(region_code),
        CONSTRAINT FK_fact_margin_impact_dim_channel FOREIGN KEY (channel_code) REFERENCES pricing.dim_channel(channel_code)
    );
END
GO

-- Operational tables

-- etl_run_history
IF OBJECT_ID('pricing.etl_run_history', 'U') IS NULL
BEGIN
    CREATE TABLE pricing.etl_run_history (
        run_id BIGINT IDENTITY(1,1) NOT NULL,
        pipeline_name NVARCHAR(255) NULL,
        started_at DATETIME2 NOT NULL,
        finished_at DATETIME2 NULL,
        status NVARCHAR(50) NULL,
        rows_loaded INT NOT NULL DEFAULT 0,
        rows_rejected INT NOT NULL DEFAULT 0,
        failure_reason NVARCHAR(MAX) NULL,
        CONSTRAINT PK_etl_run_history PRIMARY KEY (run_id)
    );
END
GO

-- price_override_audit
IF OBJECT_ID('pricing.price_override_audit', 'U') IS NULL
BEGIN
    CREATE TABLE pricing.price_override_audit (
        audit_id BIGINT IDENTITY(1,1) NOT NULL,
        sku VARCHAR(255) NOT NULL,
        region_code VARCHAR(10) NOT NULL,
        channel_code VARCHAR(10) NOT NULL,
        old_price DECIMAL(18,4) NULL,
        new_price DECIMAL(18,4) NOT NULL,
        changed_by NVARCHAR(256) NULL,
        changed_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME(),
        reason NVARCHAR(4000) NULL,
        CONSTRAINT PK_price_override_audit PRIMARY KEY (audit_id)
    );
END
GO

-- Staging tables (no FKs)

-- stg_sales
IF OBJECT_ID('pricing.stg_sales', 'U') IS NULL
BEGIN
    CREATE TABLE pricing.stg_sales (
        sale_date DATE NULL,
        sku VARCHAR(50) NULL,
        region_code VARCHAR(20) NULL,
        channel_code VARCHAR(20) NULL,
        qty INT NULL,
        net_sales DECIMAL(18,2) NULL,
        source_file NVARCHAR(260) NULL,
        loaded_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
GO

-- stg_price_history
IF OBJECT_ID('pricing.stg_price_history', 'U') IS NULL
BEGIN
    CREATE TABLE pricing.stg_price_history (
        sku VARCHAR(50) NULL,
        region_code VARCHAR(20) NULL,
        channel_code VARCHAR(20) NULL,
        price DECIMAL(18,4) NULL,
        currency CHAR(3) NULL,
        effective_start DATE NULL,
        effective_end DATE NULL,
        source_system NVARCHAR(100) NULL,
        loaded_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
GO

-- stg_discount_events
IF OBJECT_ID('pricing.stg_discount_events', 'U') IS NULL
BEGIN
    CREATE TABLE pricing.stg_discount_events (
        sku VARCHAR(50) NULL,
        region_code VARCHAR(20) NULL,
        channel_code VARCHAR(20) NULL,
        discount_type NVARCHAR(20) NULL,
        discount_value DECIMAL(18,4) NULL,
        start_date DATE NULL,
        end_date DATE NULL,
        loaded_at DATETIME2 NOT NULL DEFAULT SYSUTCDATETIME()
    );
END
GO

-- Nonclustered indexes

-- Index for current price lookups (effective_end IS NULL for current prices)
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_fact_price_history_current' AND object_id = OBJECT_ID('pricing.fact_price_history'))
BEGIN
    -- Ensure required SET options for CREATE INDEX under sqlcmd
    SET ANSI_NULLS ON;
    SET QUOTED_IDENTIFIER ON;
    CREATE NONCLUSTERED INDEX IX_fact_price_history_current
    ON pricing.fact_price_history (sku, region_code, channel_code, effective_end)
    WHERE effective_end IS NULL;
END
GO

-- Index for sales by date
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_fact_sales_sale_date' AND object_id = OBJECT_ID('pricing.fact_sales'))
BEGIN
    -- Ensure required SET options for CREATE INDEX under sqlcmd
    SET ANSI_NULLS ON;
    SET QUOTED_IDENTIFIER ON;
    CREATE NONCLUSTERED INDEX IX_fact_sales_sale_date
    ON pricing.fact_sales (sale_date, sku);
END
GO

-- Index for discount active filtering (active discounts based on date range)
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_fact_discount_events_active' AND object_id = OBJECT_ID('pricing.fact_discount_events'))
BEGIN
    -- Ensure required SET options for CREATE INDEX under sqlcmd
    SET ANSI_NULLS ON;
    SET QUOTED_IDENTIFIER ON;
    CREATE NONCLUSTERED INDEX IX_fact_discount_events_active
    ON pricing.fact_discount_events (sku, region_code, channel_code, start_date, end_date);
END
GO


