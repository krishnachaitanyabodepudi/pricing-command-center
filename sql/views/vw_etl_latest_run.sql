USE PricingDWH;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET ANSI_WARNINGS ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET ARITHABORT ON;
GO

CREATE OR ALTER VIEW pricing.vw_etl_latest_run
AS
SELECT TOP (1)
    run_id,
    started_at,
    finished_at,
    status,
    rows_loaded,
    rows_rejected,
    failure_reason
FROM pricing.etl_run_history
WHERE pipeline_name = 'pricing_refresh'
ORDER BY run_id DESC;
GO
