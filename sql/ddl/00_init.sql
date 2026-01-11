-- Initialize PricingDWH database on container startup
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'PricingDWH')
BEGIN
    CREATE DATABASE PricingDWH;
END
GO

USE PricingDWH;
GO


