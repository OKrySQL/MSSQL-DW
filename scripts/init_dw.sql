-- Crete Database 'DataWarehouse'

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'DataWarehouse')
	BEGIN
		CREATE DATABASE DataWarehouse;
	END
GO

USE DataWarehouse;
GO

IF NOT EXISTS(SELECT * FROM sys.schemas WHERE name = 'bronze')
	BEGIN
		EXEC('CREATE SCHEMA bronze;');
	END
GO

IF NOT EXISTS(SELECT * FROM sys.schemas WHERE name = 'silver')
	BEGIN
		EXEC('CREATE SCHEMA silver;');
	END
GO

IF NOT EXISTS(SELECT * FROM sys.schemas WHERE name = 'gold')
	BEGIN
		EXEC('CREATE SCHEMA gold;');
	END
GO
