USE PricingDWH;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_WARNINGS ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET ARITHABORT ON;
GO

CREATE OR ALTER PROCEDURE pricing.sp_refresh_pricing_mart
AS
BEGIN
    SET NOCOUNT ON;
    SET ANSI_NULLS ON;
    SET QUOTED_IDENTIFIER ON;
    SET ANSI_WARNINGS ON;
    SET CONCAT_NULL_YIELDS_NULL ON;
    SET ARITHABORT ON;
    
    DECLARE @RunId BIGINT;
    DECLARE @RowsLoaded INT = 0;
    DECLARE @RowsRejected INT = 0;
    DECLARE @SalesRowsInserted INT = 0;
    DECLARE @DiscountRowsInserted INT = 0;
    DECLARE @PriceHistoryRowsInserted INT = 0;
    DECLARE @PriceHistoryRowsRejected INT = 0;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Create ETL run record
        INSERT INTO pricing.etl_run_history 
            (pipeline_name, started_at, status, rows_loaded, rows_rejected)
        VALUES 
            ('pricing_refresh', SYSUTCDATETIME(), 'RUNNING', 0, 0);
        
        SET @RunId = SCOPE_IDENTITY();
        
        -- Load fact_sales from staging (deduplicated)
        INSERT INTO pricing.fact_sales 
            (sale_date, sku, region_code, channel_code, qty, net_sales)
        SELECT DISTINCT
            stg.sale_date,
            stg.sku,
            stg.region_code,
            stg.channel_code,
            stg.qty,
            stg.net_sales
        FROM pricing.stg_sales stg
        WHERE NOT EXISTS (
            SELECT 1 
            FROM pricing.fact_sales fact
            WHERE fact.sale_date = stg.sale_date
                AND fact.sku = stg.sku
                AND fact.region_code = stg.region_code
                AND fact.channel_code = stg.channel_code
                AND fact.qty = stg.qty
                AND fact.net_sales = stg.net_sales
        )
            AND stg.sale_date IS NOT NULL
            AND stg.sku IS NOT NULL
            AND stg.region_code IS NOT NULL
            AND stg.channel_code IS NOT NULL;
        
        SET @SalesRowsInserted = @@ROWCOUNT;
        SET @RowsLoaded = @RowsLoaded + @SalesRowsInserted;
        
        -- Load fact_discount_events from staging (deduplicated)
        INSERT INTO pricing.fact_discount_events
            (sku, region_code, channel_code, discount_type, discount_value, start_date, end_date)
        SELECT DISTINCT
            stg.sku,
            stg.region_code,
            stg.channel_code,
            stg.discount_type,
            stg.discount_value,
            stg.start_date,
            stg.end_date
        FROM pricing.stg_discount_events stg
        WHERE NOT EXISTS (
            SELECT 1
            FROM pricing.fact_discount_events fact
            WHERE fact.sku = stg.sku
                AND fact.region_code = stg.region_code
                AND fact.channel_code = stg.channel_code
                AND fact.discount_type = stg.discount_type
                AND fact.discount_value = stg.discount_value
                AND fact.start_date = stg.start_date
                AND fact.end_date = stg.end_date
        )
            AND stg.sku IS NOT NULL
            AND stg.region_code IS NOT NULL
            AND stg.channel_code IS NOT NULL
            AND stg.start_date IS NOT NULL
            AND stg.end_date IS NOT NULL;
        
        SET @DiscountRowsInserted = @@ROWCOUNT;
        SET @RowsLoaded = @RowsLoaded + @DiscountRowsInserted;
        
        -- Load fact_price_history from staging with validation and overlap handling
        -- First, identify and count rejected rows
        SELECT @PriceHistoryRowsRejected = COUNT(*)
        FROM pricing.stg_price_history stg
        WHERE stg.sku IS NULL 
            OR LTRIM(RTRIM(ISNULL(stg.sku, ''))) = ''
            OR NOT EXISTS (SELECT 1 FROM pricing.dim_product dp WHERE dp.sku = stg.sku)
            OR stg.price < 0
            OR stg.effective_start IS NULL;
        
        SET @RowsRejected = @RowsRejected + @PriceHistoryRowsRejected;
        
        -- Insert valid price history rows
        INSERT INTO pricing.fact_price_history
            (sku, region_code, channel_code, price, currency, effective_start, effective_end, source_system)
        SELECT DISTINCT
            stg.sku,
            stg.region_code,
            stg.channel_code,
            stg.price,
            stg.currency,
            stg.effective_start,
            stg.effective_end,
            stg.source_system
        FROM pricing.stg_price_history stg
        WHERE stg.sku IS NOT NULL
            AND LTRIM(RTRIM(stg.sku)) <> ''
            AND EXISTS (SELECT 1 FROM pricing.dim_product dp WHERE dp.sku = stg.sku)
            AND stg.price >= 0
            AND stg.effective_start IS NOT NULL
            AND NOT EXISTS (
                SELECT 1
                FROM pricing.fact_price_history fact
                WHERE fact.sku = stg.sku
                    AND fact.region_code = stg.region_code
                    AND fact.channel_code = stg.channel_code
                    AND fact.price = stg.price
                    AND fact.currency = ISNULL(stg.currency, fact.currency)
                    AND fact.effective_start = stg.effective_start
                    AND ISNULL(fact.effective_end, '9999-12-31') = ISNULL(stg.effective_end, '9999-12-31')
            );
        
        SET @PriceHistoryRowsInserted = @@ROWCOUNT;
        SET @RowsLoaded = @RowsLoaded + @PriceHistoryRowsInserted;
        
        -- Fix overlapping effective_end dates for price history
        -- For each (sku, region_code, channel_code), ensure no overlaps
        -- Handle late-arriving data by adjusting effective_end dates
        WITH PriceHistoryOrdered AS (
            SELECT 
                price_hist_id,
                sku,
                region_code,
                channel_code,
                effective_start,
                effective_end,
                created_at,
                LEAD(effective_start) OVER (
                    PARTITION BY sku, region_code, channel_code 
                    ORDER BY effective_start, created_at, price_hist_id
                ) AS next_effective_start
            FROM pricing.fact_price_history
        )
        UPDATE fph
        SET effective_end = CASE 
                WHEN pho.next_effective_start IS NOT NULL 
                THEN CASE 
                    WHEN DATEADD(DAY, -1, pho.next_effective_start) < fph.effective_start 
                    THEN fph.effective_start 
                    ELSE DATEADD(DAY, -1, pho.next_effective_start) 
                END
                ELSE NULL
            END
        FROM pricing.fact_price_history fph
        INNER JOIN PriceHistoryOrdered pho ON fph.price_hist_id = pho.price_hist_id
        WHERE (pho.next_effective_start IS NOT NULL AND (fph.effective_end IS NULL OR fph.effective_end <> DATEADD(DAY, -1, pho.next_effective_start)))
            OR (pho.next_effective_start IS NULL AND fph.effective_end IS NOT NULL);
        
        -- Update ETL run history with success
        UPDATE pricing.etl_run_history
        SET finished_at = SYSUTCDATETIME(),
            status = 'SUCCESS',
            rows_loaded = @RowsLoaded,
            rows_rejected = @RowsRejected,
            failure_reason = NULL
        WHERE run_id = @RunId;
        
        COMMIT TRANSACTION;
        
        -- Return results
        SELECT 
            @RunId AS run_id,
            'SUCCESS' AS status,
            @RowsLoaded AS rows_loaded,
            @RowsRejected AS rows_rejected;
            
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        DECLARE @ErrorMessage NVARCHAR(MAX) = ERROR_MESSAGE();
        
        -- Update ETL run history with failure
        IF @RunId IS NOT NULL
        BEGIN
            BEGIN TRY
                UPDATE pricing.etl_run_history
                SET finished_at = SYSUTCDATETIME(),
                    status = 'FAILED',
                    failure_reason = @ErrorMessage
                WHERE run_id = @RunId;
            END TRY
            BEGIN CATCH
                -- If update fails, ignore to avoid masking original error
            END CATCH
        END
        
        -- Re-raise error
        DECLARE @ErrorMsg NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        RAISERROR(@ErrorMsg, @ErrorSeverity, @ErrorState);
        RETURN;
    END CATCH
END;
GO

